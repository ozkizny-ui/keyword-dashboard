"""
오즈키즈 키워드 검색수 대시보드
Streamlit 기반 - 주간 검색수 트래킹 & 트렌드 분석

실행: streamlit run app.py
"""
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

import config
from google_sheets import (
    read_weekly_data, read_trend_data, read_rank_data,
    read_rank_history, append_rank_history,
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

@st.cache_data(ttl=3600)
def load_meta():
    try:
        return pd.read_csv(config.KEYWORDS_META_FILE, encoding="utf-8-sig")
    except FileNotFoundError:
        return pd.DataFrame(columns=["keyword", "계절", "복종", "성별"])


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

# 필터 1: 계절
seasons = ["전체"] + sorted(meta_df["계절"].dropna().unique().tolist()) if "계절" in meta_df.columns else ["전체"]
selected_season = st.sidebar.selectbox("계절", seasons)

# 필터 2: 복종
categories = ["전체"] + sorted(meta_df["복종"].dropna().unique().tolist()) if "복종" in meta_df.columns else ["전체"]
selected_category = st.sidebar.selectbox("복종 (의류/신발/잡화)", categories)

# 필터 3: 성별
genders = ["전체"] + sorted(meta_df["성별"].dropna().unique().tolist()) if "성별" in meta_df.columns else ["전체"]
selected_gender = st.sidebar.selectbox("성별", genders)

# 키워드 직접 검색
keyword_search = st.sidebar.text_input("🔎 키워드 검색", placeholder="키워드명 입력...")


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """사이드바 필터 적용"""
    if meta_df.empty or "keyword" not in meta_df.columns:
        filtered_keywords = df["keyword"].tolist()
    else:
        mask = pd.Series(True, index=meta_df.index)
        if selected_season != "전체":
            mask &= meta_df["계절"].str.contains(selected_season, na=False)
        if selected_category != "전체":
            mask &= meta_df["복종"] == selected_category
        if selected_gender != "전체":
            mask &= meta_df["성별"] == selected_gender
        filtered_keywords = meta_df[mask]["keyword"].tolist()

    result = df[df["keyword"].isin(filtered_keywords)] if filtered_keywords else df

    if keyword_search:
        result = result[result["keyword"].str.contains(keyword_search, case=False, na=False)]

    return result


# ══════════════════════════════════════════════
# 메인 대시보드
# ══════════════════════════════════════════════

st.title("📊 오즈키즈 키워드 검색수 대시보드")
st.caption(f"마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 브랜드: {config.BRAND_STORE_NAME}")

# 탭 구성
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 주간 검색수",
    "📊 연간 트렌드",
    "🛒 쇼핑검색 순위",
    "🔗 파워링크 순위",
    "📝 블로그 순위",
    "⚙️ 데이터 관리",
])


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
        _month = datetime.now().month
        _season_map = {12: "겨울", 1: "겨울", 2: "겨울",
                       3: "봄",   4: "봄",   5: "봄",
                       6: "여름", 7: "여름", 8: "여름",
                       9: "가을", 10: "가을", 11: "가을"}
        current_season = _season_map[_month]

        # ── 카드 1: 급상승 1위
        if not ranked.empty:
            _r = ranked.nlargest(1, "변화율").iloc[0]
            card1_name = _r["keyword"]
            card1_sub = f"+{_r['변화율']:.1f}%" if _r["변화율"] >= 0 else f"{_r['변화율']:.1f}%"
        else:
            card1_name, card1_sub = "-", "데이터 부족"

        # ── 카드 2: 시즌 키워드
        if not ranked.empty and not meta_df.empty and "계절" in meta_df.columns:
            _season_kws = meta_df[meta_df["계절"].str.contains(current_season, na=False)]["keyword"].tolist()
            _season_ranked = ranked[ranked["keyword"].isin(_season_kws)]
            if not _season_ranked.empty:
                _s = _season_ranked.nlargest(1, "변화율").iloc[0]
                card2_name = _s["keyword"]
                card2_sub = f"+{_s['변화율']:.1f}%" if _s["변화율"] >= 0 else f"{_s['변화율']:.1f}%"
            else:
                card2_name, card2_sub = "-", f"{current_season} 데이터 없음"
        else:
            card2_name, card2_sub = "-", "메타 데이터 없음"

        # ── 카드 3: 검색수 TOP 1
        if week_cols:
            _top_idx = filtered[week_cols[-1]].idxmax()
            card3_name = filtered.loc[_top_idx, "keyword"]
            card3_sub = f"{filtered.loc[_top_idx, week_cols[-1]]:,}"
        else:
            card3_name, card3_sub = "-", ""

        # ── 카드 4: 시장 트렌드
        if len(week_cols) >= 2:
            _total_this = filtered[week_cols[-1]].sum()
            _total_prev = filtered[week_cols[-2]].sum()
            if _total_prev > 0:
                _pct = (_total_this - _total_prev) / _total_prev * 100
                card4_val = f"+{_pct:.1f}%" if _pct >= 0 else f"{_pct:.1f}%"
            else:
                card4_val = "-"
        else:
            card4_val = "데이터 부족"

        # ── 요약 카드 표시
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""<div class="metric-card">
                <h3>📈 급상승 1위</h3><p style="font-size:1.3rem">{card1_name}</p>
                <small style="opacity:0.85">{card1_sub}</small>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""<div class="metric-card">
                <h3>🔥 시즌 키워드 ({current_season})</h3><p style="font-size:1.3rem">{card2_name}</p>
                <small style="opacity:0.85">{card2_sub}</small>
            </div>""", unsafe_allow_html=True)
        with col3:
            st.markdown(f"""<div class="metric-card">
                <h3>🏆 검색수 TOP 1</h3><p style="font-size:1.3rem">{card3_name}</p>
                <small style="opacity:0.85">{card3_sub}</small>
            </div>""", unsafe_allow_html=True)
        with col4:
            st.markdown(f"""<div class="metric-card">
                <h3>📊 시장 트렌드</h3><p>{card4_val}</p>
                <small style="opacity:0.85">전주 대비</small>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # ── 급상승/급하락 TOP 10
        if not ranked.empty:
            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("🔥 급상승 TOP 10")
                top_up = ranked.nlargest(10, "변화율")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_up, use_container_width=True, hide_index=True)
            with col_right:
                st.subheader("❄️ 급하락 TOP 10")
                top_down = ranked.nsmallest(10, "변화율")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_down, use_container_width=True, hide_index=True)

        # ── 키워드별 검색수 순위
        st.markdown("---")
        st.subheader("🔢 키워드별 검색수 순위")
        if week_cols:
            _rank_tbl = filtered[["keyword", week_cols[-1]]].copy()
            _rank_tbl.columns = ["keyword", "이번주"]
            _rank_tbl = _rank_tbl.sort_values("이번주", ascending=False).reset_index(drop=True)
            _rank_tbl.insert(0, "순위", range(1, len(_rank_tbl) + 1))
            if len(week_cols) >= 2:
                _prev_map = filtered.set_index("keyword")[week_cols[-2]].to_dict()
                _rate_map = changes.set_index("keyword")["변화율"].to_dict() if "변화율" in changes.columns else {}
                _rank_tbl["지난주"] = _rank_tbl["keyword"].map(_prev_map).fillna(0).astype(int)
                _rank_tbl["변화율"] = _rank_tbl["keyword"].map(_rate_map).apply(
                    lambda x: f"{x:+.1f}%" if pd.notna(x) else "-"
                )
            else:
                _rank_tbl["지난주"] = "-"
                _rank_tbl["변화율"] = "-"
            st.dataframe(_rank_tbl, use_container_width=True, hide_index=True, height=400)

        st.markdown("---")

        # 키워드 선택 → 주간 추이 그래프
        st.subheader("📈 키워드 주간 추이")
        kw_options = filtered["keyword"].tolist()
        selected_kws = st.multiselect("키워드 선택 (최대 10개)", kw_options, default=kw_options[:3], max_selections=10)

        if selected_kws:
            chart_data = filtered[filtered["keyword"].isin(selected_kws)].melt(
                id_vars="keyword", value_vars=week_cols, var_name="주차", value_name="검색수"
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

        # 전체 데이터 테이블
        st.subheader("📋 전체 데이터")
        st.dataframe(filtered, use_container_width=True, hide_index=True, height=400)


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

        this_year = datetime.now().year
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
            trend_selected = st.multiselect(
                "키워드 선택 (최대 5개)",
                avail_kws,
                default=avail_kws[:min(3, len(avail_kws))],
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
                                hovertemplate=(
                                    f"<b>{kw}{label_suffix}</b><br>"
                                    "주차: %{x}<br>검색수: %{y:,.0f}<extra></extra>"
                                ),
                            ))

                    fig.update_layout(
                        title="올해 vs 작년 주간 검색수 추이",
                        xaxis=dict(title="주차", dtick=4, range=[1, 53]),
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


_RANK_SHOW_COLS = ["keyword", "avg_rank", "impressions", "clicks", "cost"]
_RANK_COL_KR = {
    "keyword": "키워드", "avg_rank": "평균노출순위",
    "impressions": "노출수", "clicks": "클릭수", "cost": "총비용",
}


def _render_rank_tab(
    upload_label: str,
    uploader_key: str,
    ad_type: str,
    expected_type: str,
    sheet_name: str,
    load_fn,
    tab_label: str,
):
    """순위 탭 공통 렌더링: 업로드 → 파싱 결과 → 저장 → 이력 테이블 & 그래프"""

    # ── 현재 주 날짜 범위 ──
    _n = datetime.now()
    _mon = _n - timedelta(days=_n.weekday())
    _sun = _mon + timedelta(days=6)
    _cur_week = f"{_mon.strftime('%Y.%m.%d')}-{_sun.strftime('%Y.%m.%d')}"
    st.caption(f"현재 주차: {_cur_week}")

    # ══ 상단: 파일 업로드 ══
    st.markdown(f"#### 📂 {upload_label}")
    uploaded = st.file_uploader(
        f"{upload_label} (CSV/Excel)",
        type=["csv", "xlsx", "xls"],
        key=uploader_key,
    )

    if uploaded:
        try:
            _report, _date_label = parse_ad_report(uploaded, ad_type=ad_type)
            # 해당 타입만 필터
            _df = _report[_report["ad_type"] == expected_type].copy() if not _report.empty else _report

            if _df.empty:
                st.warning(f"{expected_type} 데이터가 없습니다. 파일을 확인해주세요.")
            else:
                st.info(f"📅 파일 날짜 범위: **{_date_label}**")
                _summary = summarize_by_keyword(_df)
                _col_kr = {**_RANK_COL_KR, "avg_rank": f"평균노출순위 ({_date_label})"}
                _disp = (
                    _summary[[c for c in _RANK_SHOW_COLS if c in _summary.columns]]
                    .sort_values("avg_rank")
                    .rename(columns=_col_kr)
                )
                st.metric("키워드 수", len(_disp))
                st.dataframe(_disp, use_container_width=True, hide_index=True, height=350)

                if st.button(f"📤 Google Sheets에 저장 ({_date_label})", key=f"save_{uploader_key}"):
                    try:
                        append_rank_history(_df, _date_label, sheet_name)
                        st.cache_data.clear()
                        st.success(f"저장 완료! ({_date_label})")
                    except Exception as _save_err:
                        st.error(f"저장 실패: {_save_err}")

        except Exception as _parse_err:
            st.error(f"파싱 실패: {_parse_err}")
            st.caption("파일 형식: 1행=제목(날짜포함), 2행=컬럼명, 3행~=데이터")

    # ══ 하단: 저장된 이력 ══
    st.markdown("---")
    st.subheader(f"📈 {tab_label} 순위 이력")

    _hist = load_fn()
    if _hist.empty:
        st.info("저장된 데이터가 없습니다. 위에서 리포트를 업로드하고 저장해주세요.")
        return

    _date_cols = [c for c in _hist.columns if c != "keyword"]
    _kw_opts   = _hist["keyword"].dropna().tolist()

    if not _kw_opts:
        st.info("키워드 데이터가 없습니다.")
        return

    # 이력 전체 테이블
    with st.expander("📋 전체 이력 데이터 보기"):
        st.dataframe(_hist, use_container_width=True, hide_index=True, height=300)

    # 키워드 선택 → 그래프
    _sel = st.multiselect(
        "키워드 선택 (최대 10개)",
        _kw_opts,
        default=_kw_opts[:min(5, len(_kw_opts))],
        max_selections=10,
        key=f"rank_kw_{uploader_key}",
    )
    if not _sel:
        return

    _plot = (
        _hist[_hist["keyword"].isin(_sel)]
        .melt(id_vars="keyword", value_vars=_date_cols, var_name="날짜범위", value_name="순위")
    )
    _plot["순위"] = pd.to_numeric(_plot["순위"], errors="coerce")
    _plot = _plot.dropna(subset=["순위"])

    if _plot.empty:
        st.info("선택한 키워드에 저장된 순위 데이터가 없습니다.")
        return

    _fig = px.line(
        _plot, x="날짜범위", y="순위", color="keyword",
        markers=True,
        title=f"{tab_label} 키워드별 순위 추이",
        labels={"날짜범위": "날짜 범위", "순위": "순위"},
        template="plotly_white",
    )
    _fig.update_layout(
        yaxis=dict(title="순위 (1위가 위)", autorange="reversed", dtick=1),
        height=450,
        hovermode="x unified",
        legend=dict(orientation="h", y=-0.25),
    )
    st.plotly_chart(_fig, use_container_width=True)


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
            with st.spinner("데이터 수집 중... (약 2~5분 소요)"):
                try:
                    from fetch_weekly_data import main as fetch_main
                    fetch_main()
                    st.cache_data.clear()
                    st.success("수집 완료! 페이지를 새로고침합니다...")
                    st.rerun()
                except Exception as e:
                    st.error(f"수집 실패: {e}")

    with col2:
        st.markdown("#### 📋 키워드 메타 정보")
        st.caption("키워드별 계절/복종/성별 태그를 관리합니다.")

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
