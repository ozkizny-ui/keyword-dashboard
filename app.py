"""
오즈키즈 키워드 검색수 대시보드
Streamlit 기반 - 주간 검색수 트래킹 & 트렌드 분석

실행: streamlit run app.py
"""
import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone

import config

KST = timezone(timedelta(hours=9))
from google_sheets import (
    read_weekly_data, read_trend_data, read_rank_data,
    read_rank_history, append_rank_history,
    save_setting, read_setting,
)
from ad_rank_parser import parse_ad_report, summarize_by_keyword

# ══════════════════════════════════════════════
# 페이지 설정
# ══════════════════════════════════════════════

st.set_page_config(
    page_title="오즈키즈 키워드 대시보드",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 커스텀 CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.2rem; border-radius: 12px; color: white; text-align: center;
    }
    .metric-card h3 { font-size: 0.85rem; font-weight: 300; margin: 0; opacity: 0.9; }
    .metric-card p { font-size: 1.8rem; font-weight: 700; margin: 0.3rem 0 0 0; }
    .change-up { color: #ff4b4b; font-weight: 700; }
    .change-down { color: #0068c9; font-weight: 700; }
    .alert-badge {
        display: inline-block; padding: 2px 8px; border-radius: 20px;
        font-size: 0.75rem; font-weight: 700;
    }
    .alert-hot { background: #ffe0e0; color: #d32f2f; }
    .alert-cold { background: #e0e8ff; color: #1565c0; }
    div[data-testid="stSidebar"] { background: #f8f9fc; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════
# 데이터 로드 (캐시)
# ══════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_weekly():
    return read_weekly_data()

@st.cache_data(ttl=300)
def load_trend():
    return read_trend_data()

@st.cache_data(ttl=300)
def load_rank():
    return read_rank_data()

@st.cache_data(ttl=300)
def load_rank_shopping():
    return read_rank_history(config.SHEET_NAME_RANK_SHOPPING)

@st.cache_data(ttl=300)
def load_rank_powerlink():
    return read_rank_history(config.SHEET_NAME_RANK_POWERLINK)

@st.cache_data(ttl=300)
def load_rank_blog():
    return read_rank_history(config.SHEET_NAME_RANK_BLOG)

@st.cache_data(ttl=300)
def load_setting(key: str, fallback: str = "") -> str:
    """'설정' 시트에서 값 읽기 (5분 캐시, API 호출 최소화)"""
    try:
        return read_setting(key, fallback)
    except Exception:
        return fallback

@st.cache_data(ttl=3600)
def load_meta():
    try:
        return pd.read_csv(config.KEYWORDS_META_FILE, encoding="utf-8-sig")
    except FileNotFoundError:
        return pd.DataFrame(columns=["keyword", "계절", "카테고리", "성별/나이"])


def calc_changes(df: pd.DataFrame) -> pd.DataFrame:
    """주간 변화율 계산"""
    week_cols = [c for c in df.columns if c != "keyword"]
    if len(week_cols) < 2:
        return df.assign(변화량=pd.NA, 변화율=pd.NA)

    latest, prev = week_cols[-1], week_cols[-2]
    result = df[["keyword", latest, prev]].copy()
    result.columns = ["keyword", "이번주", "지난주"]
    result["변화량"] = result["이번주"] - result["지난주"]
    # 지난주가 0이거나 NaN이면 변화율 계산 불가 → NaN
    valid = result["지난주"].fillna(0) > 0
    result["변화율"] = pd.NA
    result.loc[valid, "변화율"] = (
        result.loc[valid, "변화량"] / result.loc[valid, "지난주"] * 100
    ).round(1)
    return result


def format_change(val, is_pct=False):
    """변화량/율을 색상 HTML로 포맷"""
    suffix = "%" if is_pct else ""
    if val > 0:
        return f'<span class="change-up">▲ +{val:,.1f}{suffix}</span>'
    elif val < 0:
        return f'<span class="change-down">▼ {val:,.1f}{suffix}</span>'
    return f"- {suffix}"


def alert_badge(change_pct: float) -> str:
    """변화율이 클 때 배지 표시"""
    threshold = config.CHANGE_ALERT_THRESHOLD
    if change_pct >= threshold:
        return f'<span class="alert-badge alert-hot">🔥 급상승</span>'
    elif change_pct <= -threshold:
        return f'<span class="alert-badge alert-cold">❄️ 급하락</span>'
    return ""


# ══════════════════════════════════════════════
# 사이드바 - 필터
# ══════════════════════════════════════════════

if os.path.exists("logo.png"):
    st.sidebar.image("logo.png", width=80)
else:
    st.sidebar.markdown("### 🏪 오즈키즈")
st.sidebar.title("🔍 키워드 필터")

meta_df = load_meta()

# ── 필터 상태 JSON 저장/로드 ──────────────────────
_FILTER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "filter_state.json")

def _load_filter_state() -> dict:
    try:
        with open(_FILTER_FILE, "r", encoding="utf-8") as _f:
            return json.load(_f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_filter_state(state: dict):
    with open(_FILTER_FILE, "w", encoding="utf-8") as _f:
        json.dump(state, _f, ensure_ascii=False, indent=2)

_saved_filters = _load_filter_state()

# session_state 초기화 — 앱 재시작 시 JSON에서 복원
for _key, _json_key in [
    ("filter_seasons", "seasons"),
    ("filter_categories", "categories"),
    ("filter_genders", "genders"),
]:
    if _key not in st.session_state:
        st.session_state[_key] = _saved_filters.get(_json_key, [])

# 필터 1: 계절
season_options = sorted(meta_df["계절"].dropna().unique().tolist()) if "계절" in meta_df.columns else []
selected_seasons = st.sidebar.multiselect(
    "계절 (미선택 = 전체)",
    season_options,
    default=[v for v in st.session_state["filter_seasons"] if v in season_options],
    key="filter_seasons",
)

# 필터 2: 카테고리
category_options = sorted(meta_df["카테고리"].dropna().unique().tolist()) if "카테고리" in meta_df.columns else []
selected_categories = st.sidebar.multiselect(
    "카테고리 (미선택 = 전체)",
    category_options,
    default=[v for v in st.session_state["filter_categories"] if v in category_options],
    key="filter_categories",
)

# 필터 3: 성별/나이
gender_options = sorted(meta_df["성별/나이"].dropna().unique().tolist()) if "성별/나이" in meta_df.columns else []
selected_genders = st.sidebar.multiselect(
    "성별/나이 (미선택 = 전체)",
    gender_options,
    default=[v for v in st.session_state["filter_genders"] if v in gender_options],
    key="filter_genders",
)

# 필터 변경 시 JSON에 저장
_cur_filters = {
    "seasons": selected_seasons,
    "categories": selected_categories,
    "genders": selected_genders,
}
if _cur_filters != _saved_filters:
    _save_filter_state(_cur_filters)

# 키워드 직접 검색
keyword_search = st.sidebar.text_input("🔎 키워드 검색", placeholder="키워드명 입력...")


def _get_season_top3(avail_kws: list, vol_df: pd.DataFrame) -> list:
    """현재 시즌 키워드 중 최근 주차 검색수 TOP 3 반환.
    시즌 키워드가 없으면 전체 검색수 TOP 3 반환."""
    _month = datetime.now(KST).month
    _season_map = {12: "겨울", 1: "겨울", 2: "겨울",
                   3: "봄",   4: "봄",   5: "봄",
                   6: "여름", 7: "여름", 8: "여름",
                   9: "가을", 10: "가을", 11: "가을"}
    season = _season_map[_month]

    week_cols = [c for c in vol_df.columns if c != "keyword"]
    vol_map = vol_df.set_index("keyword")[week_cols[-1]].to_dict() if week_cols else {}

    season_kws = []
    if not meta_df.empty and "계절" in meta_df.columns:
        season_kws = [
            kw for kw in meta_df[meta_df["계절"].str.contains(season, na=False)]["keyword"].tolist()
            if kw in avail_kws
        ]

    pool = season_kws if season_kws else avail_kws
    ranked = sorted(pool, key=lambda kw: vol_map.get(kw, 0), reverse=True)
    return ranked[:3]


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """사이드바 필터 적용 (multiselect — 미선택 시 전체)"""
    if meta_df.empty or "keyword" not in meta_df.columns:
        filtered_keywords = df["keyword"].tolist()
    else:
        mask = pd.Series(True, index=meta_df.index)
        if selected_seasons:
            # 계절은 "봄/여름" 형태로 복수값이 들어있을 수 있으므로 contains 방식 유지
            mask &= meta_df["계절"].apply(
                lambda x: any(s in str(x) for s in selected_seasons) if pd.notna(x) else False
            )
        if selected_categories:
            mask &= meta_df["카테고리"].isin(selected_categories)
        if selected_genders:
            mask &= meta_df["성별/나이"].isin(selected_genders)
        filtered_keywords = meta_df[mask]["keyword"].tolist()

    result = df[df["keyword"].isin(filtered_keywords)] if filtered_keywords else df

    if keyword_search:
        result = result[result["keyword"].str.contains(keyword_search, case=False, na=False)]

    return result


# ══════════════════════════════════════════════
# 메인 대시보드
# ══════════════════════════════════════════════

st.title("📊 오즈키즈 키워드 검색수 대시보드")
st.caption(f"마지막 업데이트: {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} | 브랜드: {config.BRAND_STORE_NAME}")

# 탭 구성
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 주간 검색수",
    "📊 연간 트렌드",
    "🛒 쇼핑검색 순위",
    "🔗 파워링크 순위",
    "📝 블로그 순위",
    "⚙️ 데이터 관리",
])


# ══════════════════════════════════════════════
# 순위 탭 공통 헬퍼 (탭 코드보다 먼저 정의)
# ══════════════════════════════════════════════

_RANK_SHOW_COLS = ["keyword", "avg_rank", "impressions", "clicks", "cost"]
_RANK_COL_KR = {
    "keyword": "키워드", "avg_rank": "평균노출순위",
    "impressions": "노출수", "clicks": "클릭수", "cost": "총비용",
}


def _merge_meta(df: pd.DataFrame) -> pd.DataFrame:
    """df에 keywords_meta.csv의 계절/품목 정보를 병합. 매칭 안 되면 빈 문자열."""
    _src = next((c for c in ["품목", "카테고리"] if c in meta_df.columns), None)
    if meta_df.empty or "keyword" not in meta_df.columns:
        return df
    _cols = ["keyword"]
    if "계절" in meta_df.columns:
        _cols.append("계절")
    if _src:
        _cols.append(_src)
    _sub = meta_df[_cols].copy()
    if _src and _src != "품목":
        _sub = _sub.rename(columns={_src: "품목"})
    _drop = [c for c in ["계절", "품목"] if c in df.columns]
    _base = df.drop(columns=_drop) if _drop else df
    merged = _base.merge(_sub, on="keyword", how="left")
    for mc in ["계절", "품목"]:
        if mc in merged.columns:
            merged[mc] = merged[mc].fillna("").replace("None", "").astype(str)
            merged[mc] = merged[mc].apply(lambda v: "" if str(v).strip() == "" else v)
    return merged


def _multiselect_filter(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    """계절/품목 st.multiselect 필터 UI를 테이블 위에 렌더링하고 필터된 df를 반환."""
    _s_opts = sorted(df["계절"].replace("", pd.NA).dropna().unique().tolist()) if "계절" in df.columns else []
    _i_opts = sorted(df["품목"].replace("", pd.NA).dropna().unique().tolist()) if "품목" in df.columns else []
    sel_s = _s_opts[:]
    sel_i = _i_opts[:]
    if _s_opts or _i_opts:
        c1, c2 = st.columns(2)
        with c1:
            if _s_opts:
                sel_s = st.multiselect("계절 필터", _s_opts, default=_s_opts, key=f"{key_prefix}_season")
        with c2:
            if _i_opts:
                sel_i = st.multiselect("품목 필터", _i_opts, default=_i_opts, key=f"{key_prefix}_item")
    out = df.copy()
    if _s_opts and "계절" in out.columns:
        out = out[out["계절"].isin(sel_s)]
    if _i_opts and "품목" in out.columns:
        out = out[out["품목"].isin(sel_i)]
    return out


def _period_filter(week_cols: list, key_prefix: str) -> list:
    """기간 선택 필터 UI를 렌더링하고 선택된 주차 컬럼 목록을 반환."""
    if not week_cols:
        return week_cols

    quick_sel = st.radio(
        "📅 기간 선택",
        ["최근 2주 비교", "작년 동일기간 비교", "직접 선택"],
        index=0,
        horizontal=True,
        key=f"{key_prefix}_quick",
    )

    if quick_sel == "최근 2주 비교":
        return week_cols[-2:]

    elif quick_sel == "작년 동일기간 비교":
        # 52주 전 컬럼을 기본값으로 계산
        last_year_default = week_cols[0]
        try:
            latest_start = datetime.strptime(week_cols[-1].split("-")[0], "%Y.%m.%d")
            target = latest_start - timedelta(weeks=52)
            best_col, best_diff = None, None
            for col in week_cols[:-1]:
                try:
                    col_start = datetime.strptime(col.split("-")[0], "%Y.%m.%d")
                    diff = abs((col_start - target).days)
                    if best_diff is None or diff < best_diff:
                        best_diff, best_col = diff, col
                except ValueError:
                    continue
            if best_col:
                last_year_default = best_col
        except (ValueError, IndexError):
            pass
        col1, col2 = st.columns(2)
        with col1:
            ly_start = st.selectbox(
                "작년 기간",
                week_cols,
                index=week_cols.index(last_year_default),
                key=f"{key_prefix}_ly_start",
            )
        with col2:
            ly_end = st.selectbox(
                "이번 기간",
                week_cols,
                index=len(week_cols) - 1,
                key=f"{key_prefix}_ly_end",
            )
        s_idx = week_cols.index(ly_start)
        e_idx = week_cols.index(ly_end)
        if s_idx == e_idx:
            return [week_cols[s_idx]]
        if s_idx > e_idx:
            s_idx, e_idx = e_idx, s_idx
        return [week_cols[s_idx], week_cols[e_idx]]

    else:  # 직접 선택
        col1, col2 = st.columns(2)
        with col1:
            start = st.selectbox("시작", week_cols, index=0, key=f"{key_prefix}_start")
        with col2:
            end = st.selectbox("끝", week_cols, index=len(week_cols) - 1, key=f"{key_prefix}_end")
        s_idx = week_cols.index(start)
        e_idx = week_cols.index(end)
        if s_idx > e_idx:
            s_idx, e_idx = e_idx, s_idx
        return week_cols[s_idx:e_idx + 1]


def _render_rank_tab(
    upload_label: str,
    uploader_key: str,
    ad_type: str,
    expected_type: str,
    sheet_name: str,
    load_fn,
    tab_label: str,
):
    """순위 탭 공통 렌더링"""

    # ══ 섹션 1: CSV 업로드 ══
    st.markdown(f"#### 📂 {upload_label}")
    uploaded = st.file_uploader(
        f"{upload_label} (CSV/Excel)",
        type=["csv", "xlsx", "xls"],
        key=uploader_key,
    )

    st.markdown("---")

    _parsed_summary = None
    _date_label = None

    if uploaded:
        try:
            _report, _date_label = parse_ad_report(uploaded, ad_type=ad_type)
            _df = _report[_report["ad_type"] == expected_type].copy() if not _report.empty else _report
            if _df.empty:
                st.warning(f"{expected_type} 데이터가 없습니다. 파일을 확인해주세요.")
            else:
                st.caption(f"📅 파일 날짜 범위: {_date_label}")
                _parsed_summary = _merge_meta(summarize_by_keyword(_df))
        except Exception as _parse_err:
            st.error(f"파싱 실패: {_parse_err}")
            st.caption("파일 형식: 1행=제목(날짜포함), 2행=컬럼명, 3행~=데이터")

    # ══ 섹션 2: 테이블 ══
    if _parsed_summary is not None:
        _disp = _parsed_summary.sort_values("avg_rank").copy()
        _disp = _multiselect_filter(_disp, f"{uploader_key}_up")
        _disp["keyword"] = _disp.apply(
            lambda r: f"⚠️ {r['keyword']}"
            if pd.to_numeric(r["avg_rank"], errors="coerce") >= 10
            else r["keyword"],
            axis=1,
        )
        _col_order = ["keyword"] + [c for c in ("계절", "품목") if c in _disp.columns]
        _col_order += [c for c in _RANK_SHOW_COLS if c != "keyword" and c in _disp.columns]
        _disp = _disp[[c for c in _col_order if c in _disp.columns]]
        _disp = _disp.rename(columns={**_RANK_COL_KR, "avg_rank": f"평균노출순위 ({_date_label})"})

        st.metric("키워드 수", len(_disp))
        st.dataframe(_disp, use_container_width=True, hide_index=True, height=350)

        # ══ 섹션 3: 저장 버튼 ══
        if st.button(f"📤 Google Sheets에 저장 ({_date_label})", key=f"save_{uploader_key}"):
            try:
                append_rank_history(_parsed_summary, _date_label, sheet_name)
                st.cache_data.clear()
                st.success(f"저장 완료! ({_date_label})")
                st.rerun()
            except Exception as _save_err:
                st.error(f"저장 실패: {_save_err}")

    else:
        _hist = load_fn()
        if not _hist.empty:
            _hist = _merge_meta(_hist)
            _meta_cols = {"keyword", "계절", "품목"}
            _date_cols = [c for c in _hist.columns if c not in _meta_cols]
            _col_order = ["keyword"] + [c for c in ("계절", "품목") if c in _hist.columns] + _date_cols
            _hist = _hist[[c for c in _col_order if c in _hist.columns]]

            if _date_cols:
                _sel_date_cols = _period_filter(_date_cols, uploader_key)
                _non_date = [c for c in _hist.columns if c not in _date_cols]
                _hist = _hist[_non_date + _sel_date_cols]

            _hist = _multiselect_filter(_hist, f"{uploader_key}_hist")
            st.dataframe(_hist, use_container_width=True, hide_index=True, height=350)
        else:
            st.info("데이터가 없습니다. CSV 파일을 업로드해주세요.")


# ── TAB 1: 주간 검색수 ──
with tab1:
    weekly_df = load_weekly()

    if weekly_df.empty:
        st.info("아직 수집된 데이터가 없습니다. `fetch_weekly_data.py`를 먼저 실행해주세요.")
    else:
        filtered = apply_filters(weekly_df)
        changes = calc_changes(filtered)
        week_cols = [c for c in filtered.columns if c != "keyword"]

        # 변화율 numeric 변환 및 ranked 준비
        if "변화율" in changes.columns:
            changes["변화율"] = pd.to_numeric(changes["변화율"], errors="coerce")
        ranked = changes.dropna(subset=["변화율"]) if "변화율" in changes.columns else pd.DataFrame()

        # ── 현재 계절 판단
        _month = datetime.now(KST).month
        _season_map = {12: "겨울", 1: "겨울", 2: "겨울",
                       3: "봄",   4: "봄",   5: "봄",
                       6: "여름", 7: "여름", 8: "여름",
                       9: "가을", 10: "가을", 11: "가을"}
        current_season = _season_map[_month]

        # ── 상단 차트 3개 ────────────────────────────────
        ch1, ch2, ch3 = st.columns(3)

        # ── 차트 1: 급상승 TOP 10 올해 vs 작년 비교
        with ch1:
            st.markdown("**📈 급상승 TOP 10 — 올해 vs 작년**")
            if week_cols and len(week_cols) >= 2 and not ranked.empty:
                _this_week_col = week_cols[-1]
                _vol_map = filtered.set_index("keyword")[_this_week_col].to_dict()

                # 이번주 검색수 1,000 이상인 것만 대상
                _ranked_filtered = ranked[
                    ranked["keyword"].map(lambda k: _vol_map.get(k, 0)) >= 1000
                ]
                _top10_up = _ranked_filtered.nlargest(10, "변화율")[["keyword", "이번주", "변화율"]].copy()

                if not _top10_up.empty:
                    # ── 작년 같은 주차 컬럼을 weekly_df에서 탐색
                    _ly_col = None
                    _cur_start = None
                    try:
                        _cur_start = datetime.strptime(
                            _this_week_col.split("-")[0], "%Y.%m.%d"
                        )
                        _ly_target = _cur_start - timedelta(weeks=52)
                        _best_col, _best_diff = None, None
                        for _wc in weekly_df.columns[1:]:
                            try:
                                _wc_start = datetime.strptime(_wc.split("-")[0], "%Y.%m.%d")
                                _diff = abs((_wc_start - _ly_target).days)
                                if _best_diff is None or _diff < _best_diff:
                                    _best_diff, _best_col = _diff, _wc
                            except ValueError:
                                continue
                        if _best_col and _best_diff <= 14:
                            _ly_col = _best_col
                    except (ValueError, IndexError):
                        pass

                    if _ly_col:
                        # weekly_df에서 작년 데이터 가져오기
                        _ly_map = weekly_df.set_index("keyword")[_ly_col].to_dict()
                        _top10_up["작년"] = _top10_up["keyword"].map(_ly_map).fillna(0)
                    elif _cur_start is not None:
                        # 연간 트렌드 시트에서 같은 ISO 주차 데이터 탐색
                        _trend_fb = load_trend()
                        _top10_up["작년"] = 0
                        if (
                            not _trend_fb.empty
                            and "keyword" in _trend_fb.columns
                            and "estimated_weekly_volume" in _trend_fb.columns
                            and "date" in _trend_fb.columns
                        ):
                            _tdf2 = _trend_fb.copy()
                            _tdf2["_date"] = pd.to_datetime(_tdf2["date"], errors="coerce")
                            _tdf2["_year"] = _tdf2["_date"].dt.year
                            _tdf2["_week"] = _tdf2["_date"].dt.isocalendar().week
                            _iso_week = _cur_start.isocalendar()[1]
                            _ly_year = _cur_start.year - 1
                            _ly_trend = _tdf2[
                                (_tdf2["_year"] == _ly_year) & (_tdf2["_week"] == _iso_week)
                            ][["keyword", "estimated_weekly_volume"]]
                            _ly_map2 = _ly_trend.set_index("keyword")["estimated_weekly_volume"].to_dict()
                            _top10_up["작년"] = _top10_up["keyword"].map(_ly_map2).fillna(0)
                    else:
                        _top10_up["작년"] = 0

                    _top10_up = _top10_up.rename(columns={"이번주": "올해"})
                    _top10_up = _top10_up.sort_values("올해", ascending=True)

                    _chart_df = pd.melt(
                        _top10_up[["keyword", "올해", "작년"]],
                        id_vars="keyword", var_name="기간", value_name="검색수",
                    )
                    fig_cmp = px.bar(
                        _chart_df, x="검색수", y="keyword",
                        color="기간", orientation="h", barmode="group",
                        color_discrete_map={"올해": "#667eea", "작년": "#adb5bd"},
                        text="검색수",
                    )
                    fig_cmp.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
                    fig_cmp.update_layout(
                        margin=dict(t=10, b=10, l=10, r=80),
                        yaxis_title=None, xaxis_title=None,
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                        height=280,
                    )
                    st.plotly_chart(fig_cmp, use_container_width=True)
                else:
                    st.info("조건에 맞는 키워드 없음\n(이번주 검색수 1,000 이상 & 변화율 상위 필요)")
            else:
                st.info("데이터 부족 (최소 2주 필요)")

        # ── 차트 2: 검색수 TOP 10 가로 바차트
        with ch2:
            st.markdown("**🏆 검색수 TOP 10 (이번주)**")
            if week_cols:
                _top10 = (
                    filtered[["keyword", week_cols[-1]]]
                    .rename(columns={week_cols[-1]: "검색수"})
                    .nlargest(10, "검색수")
                    .sort_values("검색수", ascending=True)  # plotly h-bar: 위→아래 큰 순서
                )
                if not _top10.empty:
                    fig_bar = px.bar(
                        _top10, x="검색수", y="keyword",
                        orientation="h",
                        color="검색수",
                        color_continuous_scale="Blues",
                        text="검색수",
                    )
                    fig_bar.update_traces(
                        texttemplate="%{text:,.0f}", textposition="outside"
                    )
                    fig_bar.update_layout(
                        margin=dict(t=10, b=10, l=10, r=70),
                        coloraxis_showscale=False,
                        yaxis_title=None,
                        xaxis_title=None,
                        height=280,
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.info("데이터 없음")
            else:
                st.info("주차 데이터 없음")

        # ── 차트 3: 계절 전환 지표 (다음 계절 키워드 검색수 추이)
        with ch3:
            _next_season_map = {"봄": "여름", "여름": "가을", "가을": "겨울", "겨울": "봄"}
            next_season = _next_season_map[current_season]
            st.markdown(f"**📅 계절 전환 지표 ({next_season} 키워드 동향)**")
            if not meta_df.empty and "계절" in meta_df.columns and len(week_cols) >= 2:
                _next_kws = meta_df[
                    meta_df["계절"].str.contains(next_season, na=False)
                ]["keyword"].tolist()
                _next_vol = weekly_df[weekly_df["keyword"].isin(_next_kws)]
                _last4 = week_cols[-4:] if len(week_cols) >= 4 else week_cols
                if not _next_vol.empty:
                    _trend = _next_vol[_last4].sum().reset_index()
                    _trend.columns = ["주차", "검색수 합계"]
                    fig_line = px.line(
                        _trend, x="주차", y="검색수 합계",
                        markers=True,
                        color_discrete_sequence=["#667eea"],
                    )
                    fig_line.update_traces(line_width=2.5, marker_size=7)
                    fig_line.update_layout(
                        margin=dict(t=10, b=30, l=10, r=10),
                        xaxis_title=None,
                        yaxis_title=None,
                        height=280,
                    )
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.info(f"{next_season} 키워드 없음")
            else:
                st.info("데이터 부족 (최소 2주 필요)")

        st.markdown("---")

        # ── 기간 필터 (급상승/급하락 · 그래프 · 테이블 공통)
        period_week_cols = _period_filter(week_cols, "weekly")
        period_filtered  = filtered[["keyword"] + period_week_cols]
        period_changes   = calc_changes(period_filtered)
        if "변화율" in period_changes.columns:
            period_changes["변화율"] = pd.to_numeric(period_changes["변화율"], errors="coerce")
        period_ranked = (
            period_changes.dropna(subset=["변화율"])
            if "변화율" in period_changes.columns else pd.DataFrame()
        )

        # ── 급상승/급하락 TOP 10 (선택 기간 마지막 2주 기준)
        if not period_ranked.empty:
            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("🔥 급상승 TOP 10")
                top_up = period_ranked.nlargest(10, "변화량")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_up, use_container_width=True, hide_index=True)
            with col_right:
                st.subheader("❄️ 급하락 TOP 10")
                top_down = period_ranked.nsmallest(10, "변화량")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_down, use_container_width=True, hide_index=True)

        # ── 키워드별 검색수 순위
        st.markdown("---")
        st.subheader("🔢 키워드별 검색수 순위")
        if period_week_cols:
            _rank_tbl = period_filtered[["keyword", period_week_cols[-1]]].copy()
            _rank_tbl.columns = ["keyword", "이번주"]
            _rank_tbl = _rank_tbl.sort_values("이번주", ascending=False).reset_index(drop=True)
            _rank_tbl.insert(0, "순위", range(1, len(_rank_tbl) + 1))
            if len(period_week_cols) >= 2:
                _prev_map = period_filtered.set_index("keyword")[period_week_cols[-2]].to_dict()
                _rate_map = period_changes.set_index("keyword")["변화율"].to_dict() if "변화율" in period_changes.columns else {}
                _rank_tbl["지난주"] = _rank_tbl["keyword"].map(_prev_map).fillna(0).astype(int)
                _rank_tbl["변화율"] = _rank_tbl["keyword"].map(_rate_map).apply(
                    lambda x: f"{x:+.1f}%" if pd.notna(x) else "-"
                )
            else:
                _rank_tbl["지난주"] = "-"
                _rank_tbl["변화율"] = "-"
            st.dataframe(_rank_tbl, use_container_width=True, hide_index=True, height=400)

        st.markdown("---")

        # ── 키워드 주간 추이 그래프 (항상 최근 8주 고정)
        st.subheader("📈 키워드 주간 추이")
        _last8_cols = week_cols[-8:] if len(week_cols) >= 8 else week_cols
        _last8_filtered = filtered[["keyword"] + _last8_cols]
        kw_options = _last8_filtered["keyword"].tolist()
        # 이번주 검색수 1,000 이상 & 변화율 상위 5개를 기본 선택
        if not ranked.empty and "이번주" in ranked.columns and "변화율" in ranked.columns:
            _default_kws = (
                ranked[ranked["이번주"] >= 1000]
                .nlargest(5, "변화율")["keyword"]
                .tolist()
            )
            _default_kws = [kw for kw in _default_kws if kw in kw_options]
        else:
            _default_kws = []
        selected_kws = st.multiselect("키워드 선택 (최대 10개)", kw_options, default=_default_kws, max_selections=10)

        if selected_kws:
            chart_data = _last8_filtered[_last8_filtered["keyword"].isin(selected_kws)].melt(
                id_vars="keyword", value_vars=_last8_cols, var_name="주차", value_name="검색수"
            )
            fig = px.line(
                chart_data, x="주차", y="검색수", color="keyword",
                markers=True, title="키워드별 주간 검색수 추이",
                template="plotly_white",
            )
            fig.update_layout(
                height=450, legend=dict(orientation="h", y=-0.2),
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

        # ── 전체 데이터 테이블 (선택 기간)
        st.subheader("📋 전체 데이터")
        st.dataframe(period_filtered, use_container_width=True, hide_index=True, height=400)


# ── TAB 2: 연간 트렌드 ──
with tab2:
    trend_df = load_trend()

    _has_trend_data = (
        not trend_df.empty
        and "keyword" in trend_df.columns
        and "estimated_weekly_volume" in trend_df.columns
        and "date" in trend_df.columns
        and trend_df["keyword"].notna().any()
    )

    if not _has_trend_data:
        st.info("데이터 관리 탭에서 데이터 수집을 먼저 실행해주세요.")
    else:
        st.subheader("📊 올해 vs 작년 검색 트렌드 비교")
        st.caption("네이버 데이터랩 비율 × 실제 검색수 기반 추정치")

        this_year = datetime.now(KST).year
        last_year = this_year - 1

        # 날짜·연도·주차·검색수 파생 컬럼 생성
        _tdf = trend_df.copy()
        _tdf["_date"] = pd.to_datetime(_tdf["date"], errors="coerce")
        _tdf["_year"] = _tdf["_date"].dt.year
        _tdf["_week"] = _tdf["_date"].dt.isocalendar().week
        _tdf["_vol"] = pd.to_numeric(_tdf["estimated_weekly_volume"], errors="coerce")
        _tdf["_week"] = pd.to_numeric(_tdf["_week"], errors="coerce")

        # 올해·작년 데이터 있는 행만
        _tdf = _tdf[
            _tdf["keyword"].notna()
            & _tdf["_year"].isin([this_year, last_year])
            & _tdf["_week"].notna()
            & _tdf["_vol"].notna()
        ]

        avail_kws = sorted(_tdf["keyword"].dropna().unique().tolist())

        if not avail_kws:
            st.info("데이터 관리 탭에서 데이터 수집을 먼저 실행해주세요.")
        else:
            _weekly_for_default = load_weekly()
            _w_cols = [c for c in _weekly_for_default.columns if c != "keyword"]
            if _w_cols:
                _trend_defaults = (
                    _weekly_for_default[_weekly_for_default["keyword"].isin(avail_kws)]
                    [["keyword", _w_cols[-1]]]
                    .rename(columns={_w_cols[-1]: "_vol"})
                    .sort_values("_vol", ascending=False)["keyword"]
                    .head(3)
                    .tolist()
                )
            else:
                _trend_defaults = avail_kws[:3]
            trend_selected = st.multiselect(
                "키워드 선택 (최대 5개)",
                avail_kws,
                default=_trend_defaults,
                max_selections=5,
                key="trend_kw",
            )

            if trend_selected:
                plot_df = _tdf[_tdf["keyword"].isin(trend_selected)].copy()

                if plot_df.empty:
                    st.info("선택한 키워드에 대한 데이터가 없습니다.")
                else:
                    # 키워드별 색상 고정
                    _palette = px.colors.qualitative.Plotly
                    _color_map = {kw: _palette[i % len(_palette)] for i, kw in enumerate(trend_selected)}

                    # ISO 주차 → "월주" 라벨 변환
                    from datetime import date as _dc
                    def _wk_label(w: int) -> str:
                        try:
                            d = _dc.fromisocalendar(this_year, int(w), 1)
                            return f"{d.month}월{(d.day - 1) // 7 + 1}주"
                        except (ValueError, OverflowError):
                            return str(w)

                    _tickvals = list(range(1, 53, 2))
                    _ticktext = [_wk_label(w) for w in _tickvals]

                    fig = go.Figure()
                    for kw in trend_selected:
                        for year, dash, label_suffix in [
                            (this_year, "solid", f" (올해 {this_year})"),
                            (last_year, "dash",  f" (작년 {last_year})"),
                        ]:
                            _sub = (
                                plot_df[(plot_df["keyword"] == kw) & (plot_df["_year"] == year)]
                                .sort_values("_week")
                            )
                            if _sub.empty:
                                continue
                            fig.add_trace(go.Scatter(
                                x=_sub["_week"].astype(int),
                                y=_sub["_vol"],
                                mode="lines",
                                name=f"{kw}{label_suffix}",
                                line=dict(color=_color_map[kw], dash=dash, width=2),
                                customdata=_sub["_week"].apply(_wk_label),
                                hovertemplate=(
                                    f"<b>{kw}{label_suffix}</b><br>"
                                    "%{customdata}<br>검색수: %{y:,.0f}<extra></extra>"
                                ),
                            ))

                    fig.update_layout(
                        title="올해 vs 작년 주간 검색수 추이",
                        xaxis=dict(
                            title=None,
                            tickvals=_tickvals,
                            ticktext=_ticktext,
                            tickangle=-45,
                            range=[1, 53],
                        ),
                        yaxis=dict(title="추정 검색수"),
                        height=500,
                        hovermode="x unified",
                        legend=dict(orientation="h", y=-0.3),
                        template="plotly_white",
                        annotations=[dict(
                            text="실선 = 올해 | 점선 = 작년",
                            xref="paper", yref="paper",
                            x=1, y=1.02, showarrow=False,
                            font=dict(size=11, color="gray"),
                        )],
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # 데이터랩 원본 비율 (ratio 컬럼 있을 때만)
                if "ratio" in trend_df.columns:
                    with st.expander("📉 데이터랩 원본 비율 보기"):
                        _ratio_df = _tdf[_tdf["keyword"].isin(trend_selected)].copy()
                        _ratio_df["ratio"] = pd.to_numeric(_ratio_df.get("ratio", pd.NA), errors="coerce")
                        _ratio_df = _ratio_df.dropna(subset=["ratio"])
                        if not _ratio_df.empty:
                            fig2 = px.line(
                                _ratio_df, x="_date", y="ratio", color="keyword",
                                title="데이터랩 검색 비율 (0~100)",
                                labels={"_date": "날짜", "ratio": "비율"},
                                template="plotly_white",
                            )
                            fig2.update_layout(height=350)
                            st.plotly_chart(fig2, use_container_width=True)


# ── TAB 3: 쇼핑검색 순위 ──
with tab3:
    _render_rank_tab(
        upload_label="쇼핑검색 리포트 CSV 업로드",
        uploader_key="shopping_upload",
        ad_type="shopping",
        expected_type="쇼핑검색",
        sheet_name=config.SHEET_NAME_RANK_SHOPPING,
        load_fn=load_rank_shopping,
        tab_label="쇼핑검색",
    )


# ── TAB 4: 파워링크 순위 ──
with tab4:
    _render_rank_tab(
        upload_label="파워링크 리포트 CSV 업로드",
        uploader_key="powerlink_upload",
        ad_type="powerlink",
        expected_type="파워링크",
        sheet_name=config.SHEET_NAME_RANK_POWERLINK,
        load_fn=load_rank_powerlink,
        tab_label="파워링크",
    )


# ── TAB 5: 블로그 순위 ──
with tab5:
    _render_rank_tab(
        upload_label="블로그 순위 CSV/엑셀 업로드",
        uploader_key="blog_upload",
        ad_type="blog",
        expected_type="블로그",
        sheet_name=config.SHEET_NAME_RANK_BLOG,
        load_fn=load_rank_blog,
        tab_label="블로그",
    )


# ── TAB 6: 데이터 관리 ──
with tab6:
    st.subheader("⚙️ 데이터 관리 & 설정")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 🔄 수동 데이터 수집")
        st.caption("버튼을 누르면 네이버 API에서 최신 데이터를 가져와 Google Sheets에 저장합니다.")

        if st.button("🚀 지금 데이터 수집 실행", type="primary"):
            progress = st.empty()
            try:
                import traceback
                from fetch_weekly_data import load_keywords, get_week_label
                from naver_api import fetch_search_volume, fetch_datalab_trend, estimate_weekly_search_volume
                from google_sheets import append_weekly_data, save_trend_data

                week_label = get_week_label()

                # ── 1단계: 키워드 로드 ──
                progress.info("⏳ 1/3 키워드 파일 로드 중...")
                keywords = load_keywords()
                progress.info(f"⏳ 1/3 키워드 {len(keywords)}개 로드 완료")

                # ── 2단계: 검색수 조회 ──
                progress.info(f"⏳ 2/3 네이버 검색광고 API 조회 중... ({len(keywords)}개 키워드)")
                volume_df = fetch_search_volume(keywords)

                if volume_df.empty:
                    st.error("❌ 검색수 데이터를 가져오지 못했습니다. API 키를 확인하세요.")
                else:
                    # ── Google Sheets 저장 ──
                    progress.info(f"⏳ 2/3 Google Sheets에 저장 중... ({week_label})")
                    append_weekly_data(volume_df, week_label)
                    progress.info(f"⏳ 2/3 주간 검색수 {len(volume_df)}개 키워드 저장 완료!")

                    # ── 3단계: 트렌드 조회 ──
                    progress.info(f"⏳ 3/3 데이터랩 트렌드 조회 중... (시간이 좀 걸립니다)")
                    from datetime import datetime, timedelta, timezone
                    _KST = timezone(timedelta(hours=9))
                    end_date = datetime.now(_KST).strftime("%Y-%m-%d")
                    start_date = (datetime.now(_KST) - timedelta(days=365)).strftime("%Y-%m-%d")
                    trend_df = fetch_datalab_trend(keywords, start_date, end_date)

                    if not trend_df.empty:
                        estimated = estimate_weekly_search_volume(volume_df, trend_df)
                        if not estimated.empty:
                            save_trend_data(estimated)

                    st.cache_data.clear()
                    progress.empty()
                    st.success(f"✅ 수집 완료! 주차: {week_label} | 키워드: {len(volume_df)}개 | 페이지를 새로고침(F5)하면 반영됩니다.")

            except Exception as e:
                progress.empty()
                st.error(f"❌ 수집 실패: {e}")
                st.code(traceback.format_exc())

    with col2:
        st.markdown("#### 📋 키워드 메타 정보")
        st.caption("키워드별 계절/카테고리/성별/나이 태그를 관리합니다.")

        meta = load_meta()
        if meta.empty:
            st.warning(f"`{config.KEYWORDS_META_FILE}` 파일이 없습니다. 아래에서 다운로드 받아 작성해주세요.")
        else:
            st.success(f"{len(meta)}개 키워드 메타 정보 로드됨")
            st.dataframe(meta.head(10), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 📁 파일 경로 & API 설정")
    st.json({
        "키워드 파일": config.KEYWORDS_FILE,
        "메타 파일": config.KEYWORDS_META_FILE,
        "Google Sheet ID": config.SPREADSHEET_ID,
        "변화율 알림 기준": f"±{config.CHANGE_ALERT_THRESHOLD}%",
        "네이버 검색광고 API": "✅ 설정됨" if config.NAVER_AD_API_LICENSE else "❌ 미설정",
        "네이버 데이터랩 API": "✅ 설정됨" if config.NAVER_CLIENT_ID else "❌ 미설정",
        "Google Credentials": "✅ 설정됨" if config.GOOGLE_CREDENTIALS_FILE else "❌ 미설정",
    })
