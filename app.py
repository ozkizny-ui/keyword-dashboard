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
    save_new_keywords, read_new_keywords,
    read_keyword_dict,
)
from ad_rank_parser import parse_ad_report, parse_ad_report_multiweek, summarize_by_keyword

# ══════════════════════════════════════════════
# 페이지 설정
# ══════════════════════════════════════════════

st.set_page_config(
    page_title="오즈키즈 키워드 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Pretendard:wght@300;400;500;600;700;800&display=swap');

/* ════════════════════════════════════════════
   기본 (데스크톱)
════════════════════════════════════════════ */

/* ── 기본 폰트 ── */
html, body, [class*="css"], .stMarkdown, .stDataFrame {
    font-family: 'Pretendard', 'Noto Sans KR', -apple-system, sans-serif !important;
}

/* ── 배경 ── */
[data-testid="stAppViewContainer"] { background: #f5f7fa; }
[data-testid="stAppViewContainer"] > .main { background: #f5f7fa; }
[data-testid="stMain"] { background: #f5f7fa; }
.block-container {
    padding-top: 3rem !important;
    padding-bottom: 2rem !important;
    max-width: 100% !important;
}

/* ── 사이드바 ── */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e8ecf0 !important;
}
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    font-size: 0.95rem !important; font-weight: 700 !important;
    color: #1e2a3a !important; letter-spacing: -0.01em;
}
[data-testid="stSidebar"] label {
    font-size: 0.75rem !important; font-weight: 600 !important;
    color: #64748b !important; text-transform: uppercase; letter-spacing: 0.06em;
}

/* ── 메인 타이틀 ── */
h1 { font-size: 1.55rem !important; font-weight: 800 !important;
     color: #0f172a !important; letter-spacing: -0.03em !important; margin-bottom: 0 !important; }
h2 { font-size: 1.15rem !important; font-weight: 700 !important; color: #1e293b !important; }
h3 { font-size: 0.95rem !important; font-weight: 600 !important; color: #334155 !important; }

/* ── 탭 ── */
[data-testid="stTabs"] [role="tablist"] {
    background: #ffffff;
    border-radius: 14px;
    padding: 5px;
    gap: 3px;
    border: 1px solid #e8ecf0;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    flex-wrap: wrap;
}
[data-testid="stTabs"] [role="tab"] {
    border-radius: 10px !important;
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    color: #64748b !important;
    padding: 0.45rem 1.1rem !important;
    transition: all 0.18s ease;
    border: none !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    background: #4361ee !important;
    color: #ffffff !important;
    font-weight: 700 !important;
    box-shadow: 0 2px 10px rgba(67,97,238,0.28) !important;
}
[data-testid="stTabs"] [role="tab"]:hover:not([aria-selected="true"]) {
    background: #eef0fd !important; color: #4361ee !important;
}
[data-testid="stTabs"] [role="tabpanel"] {
    padding-top: 1.4rem !important;
}

/* ── 메트릭 카드 ── */
[data-testid="stMetric"] {
    background: #ffffff !important;
    border-radius: 12px !important;
    padding: 1rem 1.25rem !important;
    border: 1px solid #e8ecf0 !important;
    border-left: 3px solid #4361ee !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05) !important;
}
[data-testid="stMetric"] label {
    font-size: 0.72rem !important; font-weight: 700 !important;
    color: #64748b !important; text-transform: uppercase; letter-spacing: 0.07em;
}
[data-testid="stMetricValue"] {
    font-size: 1.8rem !important; font-weight: 800 !important; color: #0f172a !important;
}
[data-testid="stMetricDelta"] { font-size: 0.8rem !important; font-weight: 600 !important; }

/* ── 버튼 ── */
.stButton > button {
    border-radius: 10px !important; font-weight: 600 !important; font-size: 0.85rem !important;
    border: 1.5px solid #e2e8f0 !important; transition: all 0.16s ease !important;
    color: #475569 !important; background: #ffffff !important;
    min-height: 44px !important;
}
.stButton > button:hover {
    border-color: #4361ee !important; color: #4361ee !important;
    box-shadow: 0 2px 8px rgba(67,97,238,0.15) !important; background: #eef0fd !important;
}
.stButton > button[kind="primary"] {
    background: #4361ee !important; color: #ffffff !important;
    border: none !important; box-shadow: 0 2px 10px rgba(67,97,238,0.3) !important;
}
.stButton > button[kind="primary"]:hover {
    background: #3451d1 !important; color: #ffffff !important;
}

/* ── 파일 업로더 ── */
[data-testid="stFileUploader"] {
    background: #ffffff;
    border-radius: 12px;
    border: 2px dashed #c7d2fe !important;
    transition: border-color 0.2s;
}
[data-testid="stFileUploader"]:hover { border-color: #4361ee !important; }
[data-testid="stFileUploadDropzone"] {
    background: #f8f9ff !important; border-radius: 10px;
}

/* ── 데이터프레임 ── */
[data-testid="stDataFrame"] {
    border-radius: 12px !important;
    overflow: hidden;
    border: 1px solid #e8ecf0 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}

/* ── 구분선 ── */
hr { border: none; border-top: 1px solid #e8ecf0; margin: 1.25rem 0; }

/* ── 캡션 ── */
[data-testid="stCaptionContainer"] p {
    color: #94a3b8 !important; font-size: 0.78rem !important;
}

/* ── Expander ── */
[data-testid="stExpander"] {
    border-radius: 12px !important;
    border: 1px solid #e8ecf0 !important;
    background: #ffffff !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
}

/* ── 알림 ── */
[data-testid="stAlert"] { border-radius: 12px !important; border: none !important; }

/* ── 멀티셀렉트 태그 ── */
span[data-baseweb="tag"] {
    background: #eef0fd !important; color: #4361ee !important;
    border-radius: 6px !important; font-weight: 600 !important;
}

/* ── 텍스트 인풋 ── */
[data-testid="stTextInput"] input {
    border-radius: 10px !important; border-color: #e2e8f0 !important;
    font-size: 0.875rem !important;
    min-height: 44px !important;
}
[data-testid="stTextInput"] input:focus {
    border-color: #4361ee !important;
    box-shadow: 0 0 0 2px rgba(67,97,238,0.15) !important;
}

/* ── 셀렉트박스 ── */
[data-testid="stSelectbox"] > div > div {
    border-radius: 10px !important; border-color: #e2e8f0 !important;
    min-height: 44px !important;
}

/* ── 멀티셀렉트 ── */
[data-testid="stMultiSelect"] > div > div {
    border-radius: 10px !important; border-color: #e2e8f0 !important;
    min-height: 44px !important;
}

/* ── 커스텀 클래스 ── */
.metric-card {
    background: linear-gradient(135deg, #4361ee 0%, #7c3aed 100%);
    padding: 1.25rem 1.5rem; border-radius: 14px; color: white; text-align: center;
    box-shadow: 0 4px 16px rgba(67,97,238,0.22);
}
.metric-card h3 { font-size: 0.78rem; font-weight: 600; margin: 0; opacity: 0.85; letter-spacing: 0.06em; text-transform: uppercase; }
.metric-card p  { font-size: 2rem; font-weight: 800; margin: 0.3rem 0 0; letter-spacing: -0.02em; }

.change-up   { color: #ef4444; font-weight: 700; }
.change-down { color: #3b82f6; font-weight: 700; }

.alert-badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 0.72rem; font-weight: 700; }
.alert-hot  { background: #fee2e2; color: #dc2626; }
.alert-cold { background: #dbeafe; color: #1d4ed8; }

/* ── 섹션 헤더 강조 ── */
.section-header {
    display: flex; align-items: center; gap: 0.5rem;
    font-size: 1rem; font-weight: 700; color: #1e293b;
    padding-bottom: 0.5rem; border-bottom: 2px solid #eef0fd; margin-bottom: 1rem;
}

/* ── 사이드바 라디오 네비게이션 ── */
[data-testid="stSidebar"] [data-testid="stRadio"] > div {
    display: flex; flex-direction: column; gap: 2px;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    display: flex; align-items: center;
    padding: 0.6rem 0.75rem;
    border-radius: 8px;
    font-size: 0.925rem;
    font-weight: 500;
    color: #475569;
    cursor: pointer;
    min-height: 44px;
    transition: background 0.15s, color 0.15s;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: #eef0fd;
    color: #4361ee;
}
[data-testid="stSidebar"] [data-testid="stRadio"] [aria-checked="true"] ~ label,
[data-testid="stSidebar"] [data-testid="stRadio"] input:checked + div label {
    background: #eef0fd;
    color: #4361ee;
    font-weight: 700;
}
/* 라디오 원형 도트만 숨김 */
[data-testid="stSidebar"] [data-testid="stRadio"] [data-baseweb="radio"] > div:first-child {
    display: none !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] input[type="radio"] {
    display: none !important;
}

/* ════════════════════════════════════════════
   모바일 반응형 (≤ 768px)
════════════════════════════════════════════ */
@media (max-width: 768px) {

    /* 컨테이너 패딩 축소 */
    .block-container {
        padding-top: 0.75rem !important;
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
        padding-bottom: 4rem !important;
    }

    /* 헤딩 크기 */
    h1 { font-size: 1.2rem !important; }
    h2 { font-size: 1rem !important; }
    h3 { font-size: 0.875rem !important; }

    /* 메트릭 */
    [data-testid="stMetric"] {
        padding: 0.75rem 1rem !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.4rem !important;
    }

    /* 컬럼 → 세로 스택 */
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
        gap: 0.5rem !important;
    }
    [data-testid="column"] {
        flex: 1 1 100% !important;
        width: 100% !important;
        min-width: 100% !important;
    }

    /* 버튼 전체 너비 */
    .stButton > button {
        width: 100% !important;
        font-size: 0.9rem !important;
        min-height: 48px !important;
    }

    /* 탭 작게 */
    [data-testid="stTabs"] [role="tab"] {
        font-size: 0.75rem !important;
        padding: 0.4rem 0.55rem !important;
    }

    /* 데이터프레임 가로 스크롤 */
    [data-testid="stDataFrame"] {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }

    /* 인풋/셀렉트 터치 타깃 */
    [data-testid="stTextInput"] input,
    [data-testid="stSelectbox"] > div > div,
    [data-testid="stMultiSelect"] > div > div {
        font-size: 1rem !important;
        min-height: 48px !important;
    }

    /* 사이드바 전체 너비 (열렸을 때) */
    [data-testid="stSidebar"] {
        width: 85vw !important;
        min-width: 260px !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label {
        font-size: 1rem !important;
        padding: 0.75rem 1rem !important;
        min-height: 48px !important;
    }

    /* 구분선 여백 축소 */
    hr { margin: 0.75rem 0; }

    /* 캡션 */
    [data-testid="stCaptionContainer"] p {
        font-size: 0.72rem !important;
    }
}

/* ≤ 480px: 소형 폰 */
@media (max-width: 480px) {
    .block-container {
        padding-left: 0.5rem !important;
        padding-right: 0.5rem !important;
    }
    h1 { font-size: 1.05rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.25rem !important; }
    [data-testid="stTabs"] [role="tab"] {
        font-size: 0.7rem !important;
        padding: 0.35rem 0.45rem !important;
    }
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════
# 데이터 로드 (캐시)
# ══════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_weekly():
    """키워드사전 탭에서 주간검색수 데이터 로드 (fallback: 주간검색수 시트)"""
    kd = load_keyword_dict()
    if not kd.empty and "keyword" in kd.columns:
        meta_cols = {"계절", "복종", "연령", "성별", "카테고리", "대표키워드", "키워드"}
        week_cols = [c for c in kd.columns if c not in meta_cols and c != "keyword"]
        if week_cols:
            result = kd[["keyword"] + week_cols].copy()
            result["keyword"] = result["keyword"].str.strip()
            result = result.drop_duplicates(subset="keyword", keep="first").reset_index(drop=True)
            return result
    # fallback: 기존 주간검색수 시트
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
def load_rank_cafe():
    return read_rank_history(config.SHEET_NAME_RANK_CAFE)

@st.cache_data(ttl=300)
def load_setting(key: str, fallback: str = "") -> str:
    """'설정' 시트에서 값 읽기 (5분 캐시, API 호출 최소화)"""
    try:
        return read_setting(key, fallback)
    except Exception:
        return fallback

@st.cache_data(ttl=3600)
def load_meta():
    """키워드사전 탭에서 메타 정보 로드 (fallback: keywords_meta.csv)"""
    kd = load_keyword_dict()
    if not kd.empty and "keyword" in kd.columns:
        meta = kd[["keyword"]].copy()
        # 계절 매핑
        if "계절" in kd.columns:
            meta["계절"] = kd["계절"]
        # 카테고리 매핑
        if "카테고리" in kd.columns:
            meta["카테고리"] = kd["카테고리"]
        # 성별/나이 매핑 (키워드사전: 성별 + 연령 → 결합)
        if "성별" in kd.columns and "연령" in kd.columns:
            meta["성별/나이"] = kd["성별"].astype(str).str.strip() + "/" + kd["연령"].astype(str).str.strip()
            meta["성별/나이"] = meta["성별/나이"].str.strip("/").replace({"": pd.NA, "/": pd.NA})
        elif "성별" in kd.columns:
            meta["성별/나이"] = kd["성별"]
        meta["keyword"] = meta["keyword"].str.strip()
        meta = meta.drop_duplicates(subset="keyword", keep="first").reset_index(drop=True)
        return meta
    # fallback: CSV 파일
    try:
        return pd.read_csv(config.KEYWORDS_META_FILE, encoding="utf-8-sig")
    except FileNotFoundError:
        return pd.DataFrame(columns=["keyword", "계절", "카테고리", "성별/나이"])

@st.cache_data(ttl=300)
def load_keyword_dict():
    """키워드사전 탭 로드 (Apps Script 수집 데이터)"""
    try:
        return read_keyword_dict()
    except Exception:
        return pd.DataFrame()


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
# 사이드바 - 로고 + 메뉴 + 조건부 필터
# ══════════════════════════════════════════════

if os.path.exists("logo.png"):
    st.sidebar.image("logo.png", width=72)
else:
    st.sidebar.markdown(
        "<div style='font-size:1.1rem;font-weight:800;color:#4361ee;letter-spacing:-0.02em;"
        "padding:0.5rem 0 0.5rem;'>오즈키즈</div>",
        unsafe_allow_html=True,
    )

# ── 메뉴 ──────────────────────────────────────
_MENU_ITEMS = ["📈 주간 검색수", "📊 연간 트렌드", "🛒 쇼핑검색 순위", "🔗 파워링크 순위", "📝 블로그/카페 순위", "🆕 신규키워드 개발", "⚙️ 데이터 관리"]
selected_menu = st.sidebar.radio(
    "메뉴",
    _MENU_ITEMS,
    label_visibility="collapsed",
)

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
    try:
        with open(_FILTER_FILE, "w", encoding="utf-8") as _f:
            json.dump(state, _f, ensure_ascii=False, indent=2)
    except Exception:
        pass

_saved_filters = _load_filter_state()

# ── 필터 기본값 (주간 검색수 / 연간 트렌드 탭에서 인라인으로 재설정)
selected_seasons: list = []
selected_categories: list = []
selected_genders: list = []
keyword_search: str = ""


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

st.markdown(
    f"""<div style="padding:0.25rem 0 1.25rem 0;">
        <div style="font-size:clamp(1.1rem,4vw,1.55rem);font-weight:800;color:#0f172a;
                    letter-spacing:-0.03em;line-height:1.2;">📊 오즈키즈 키워드 대시보드</div>
        <div style="font-size:clamp(0.7rem,2.5vw,0.78rem);color:#94a3b8;
                    margin-top:0.2rem;font-weight:400;">
            브랜드: <b style="color:#64748b;">{config.BRAND_STORE_NAME}</b>
            &nbsp;·&nbsp; 업데이트: {datetime.now(KST).strftime('%Y.%m.%d %H:%M')}
        </div>
    </div>""",
    unsafe_allow_html=True,
)



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


def _rank_style(df: pd.DataFrame, this_col: str, prev_col: str = None) -> pd.DataFrame:
    """순위 테이블 이번주 셀만 배경색. 🔻 4+ 하락 → 연한 파란색, ⚠️ 10위 밖 → 연한 노란색."""
    result = pd.DataFrame("", index=df.index, columns=df.columns)
    if this_col not in df.columns:
        return result
    col_idx = df.columns.get_loc(this_col)
    for i in range(len(df)):
        this_r = pd.to_numeric(df[this_col].iloc[i], errors="coerce")
        prev_r = (
            pd.to_numeric(df[prev_col].iloc[i], errors="coerce")
            if (prev_col and prev_col in df.columns)
            else float("nan")
        )
        is_drop = pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4
        is_warn = pd.notna(this_r) and this_r > 10
        if is_drop:
            result.iloc[i, col_idx] = "background-color: #e3f2fd"
        elif is_warn:
            result.iloc[i, col_idx] = "background-color: #fff9c4"
    return result


def _render_rank_tab(
    upload_label: str,
    uploader_key: str,
    ad_type: str,
    expected_type: str,
    sheet_name: str,
    load_fn,
    tab_label: str,
    use_styling: bool = False,
    multi_week: bool = False,
):
    """순위 탭 공통 렌더링"""

    # ══ 섹션 1: CSV 업로드 ══
    st.markdown(
        f"<div class='section-header'>📂 {upload_label}</div>",
        unsafe_allow_html=True,
    )
    uploaded = st.file_uploader(
        f"{upload_label} (CSV/Excel)",
        type=["csv", "xlsx", "xls"],
        key=uploader_key,
    )

    st.markdown("---")

    _parsed_summary = None
    _date_label = None
    _multi_week_data = None  # (merged_df, this_label, prev_label, save_summary)

    if uploaded:
        if multi_week:
            try:
                week_dfs, week_labels = parse_ad_report_multiweek(uploaded, ad_type=ad_type)
                filtered_dfs: dict = {}
                for lbl, wdf in week_dfs.items():
                    fdf = wdf[wdf["ad_type"] == expected_type].copy() if not wdf.empty else wdf
                    if not fdf.empty:
                        filtered_dfs[lbl] = _merge_meta(fdf)

                valid_labels = [l for l in week_labels if l in filtered_dfs]

                if not valid_labels:
                    st.warning(f"{expected_type} 데이터가 없습니다. 파일을 확인해주세요.")
                elif len(valid_labels) == 1:
                    # 단일 주차 → 기존 방식으로 처리
                    _date_label = valid_labels[0]
                    _parsed_summary = filtered_dfs[_date_label]
                    st.caption(f"📅 파일 날짜 범위: {_date_label}")
                else:
                    this_label = valid_labels[-1]
                    prev_label = valid_labels[-2]
                    st.caption(f"📅 이번주: {this_label} | 지난주: {prev_label}")

                    this_df = filtered_dfs[this_label]
                    prev_df = filtered_dfs[prev_label]

                    # 이번주/지난주 avg_rank 병합
                    this_rank = this_df[["keyword", "avg_rank"]].rename(columns={"avg_rank": "이번주"})
                    prev_rank = prev_df[["keyword", "avg_rank"]].rename(columns={"avg_rank": "지난주"})
                    merged = this_rank.merge(prev_rank, on="keyword", how="outer")

                    # 메타 컬럼 추가 (이번주 기준)
                    meta_cols = [c for c in ("계절", "품목") if c in this_df.columns]
                    if meta_cols:
                        meta_df = this_df[["keyword"] + meta_cols]
                        merged = meta_df.merge(merged, on="keyword", how="right")

                    merged = merged.sort_values("이번주", ascending=False, na_position="last").reset_index(drop=True)

                    # 저장용: 이번주 데이터 (표시용 — 비어있는지 체크에 사용)
                    save_df = week_dfs[this_label][week_dfs[this_label]["ad_type"] == expected_type].copy()
                    save_summary = _merge_meta(save_df) if not save_df.empty else pd.DataFrame()

                    # 저장용: 모든 주차 데이터
                    all_save_data = {}
                    for _lbl in valid_labels:
                        _sdf = week_dfs[_lbl][week_dfs[_lbl]["ad_type"] == expected_type].copy()
                        if not _sdf.empty:
                            all_save_data[_lbl] = _merge_meta(_sdf)

                    _multi_week_data = (merged, this_label, prev_label, save_summary, all_save_data, valid_labels)
                    _date_label = this_label
            except Exception as _parse_err:
                st.error(f"파싱 실패: {_parse_err}")
                st.caption("파일 형식: 1행=제목(날짜포함), 2행=컬럼명, 3행~=데이터")
        else:
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

    # ══ 섹션 2: 테이블 (다중 주차) ══
    if _multi_week_data is not None:
        merged, this_label, prev_label, save_summary, all_save_data, valid_labels = _multi_week_data
        _disp = _multiselect_filter(merged, f"{uploader_key}_up")

        # 컬럼 순서: 계절, 품목, 키워드, 지난주, 이번주
        _meta_cols = [c for c in ("계절", "품목") if c in _disp.columns]
        _col_order = _meta_cols + ["keyword", "지난주", "이번주"]
        _disp = _disp[[c for c in _col_order if c in _disp.columns]].reset_index(drop=True)

        # 숫자값 캡처 (키워드 아이콘 추가 전)
        _this_nums = pd.to_numeric(_disp["이번주"], errors="coerce") if "이번주" in _disp.columns else pd.Series(dtype=float)
        _prev_nums = pd.to_numeric(_disp["지난주"], errors="coerce") if "지난주" in _disp.columns else pd.Series(dtype=float)

        # 키워드 컬럼에 아이콘 추가 (🔻: 4 이상 하락, ⚠️: 10위 초과)
        def _kw_icon(i):
            kw = str(_disp["keyword"].iloc[i])
            this_r = _this_nums.iloc[i] if i < len(_this_nums) else float("nan")
            prev_r = _prev_nums.iloc[i] if i < len(_prev_nums) else float("nan")
            if pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4:
                return f"🔻 {kw}"
            elif pd.notna(this_r) and this_r > 10:
                return f"⚠️ {kw}"
            return kw

        _disp["keyword"] = [_kw_icon(i) for i in range(len(_disp))]

        # 컬럼명 변환
        _this_col = f"이번주 ({this_label})"
        _prev_col = f"지난주 ({prev_label})"
        _disp = _disp.rename(columns={
            "keyword": "키워드",
            "이번주": _this_col,
            "지난주": _prev_col,
        })

        st.metric("키워드 수", len(_disp))

        # 이번주 셀만 배경색 적용 (숫자 그대로 유지 → 오른쪽 정렬 자동)
        def _mw_style(df):
            result = pd.DataFrame("", index=df.index, columns=df.columns)
            if _this_col not in df.columns:
                return result
            col_idx = df.columns.get_loc(_this_col)
            for i in range(len(df)):
                this_r = _this_nums.iloc[i] if i < len(_this_nums) else float("nan")
                prev_r = _prev_nums.iloc[i] if i < len(_prev_nums) else float("nan")
                if pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4:
                    result.iloc[i, col_idx] = "background-color: #e3f2fd"
                elif pd.notna(this_r) and this_r > 10:
                    result.iloc[i, col_idx] = "background-color: #fff9c4"
            return result

        st.dataframe(
            _disp.style
                .apply(_mw_style, axis=None)
                .format({_this_col: "{:,.0f}", _prev_col: "{:,.0f}"}, na_rep="-"),
            use_container_width=True, hide_index=True, height=350,
        )

        # ══ 섹션 3: 저장 버튼 ══
        if not save_summary.empty:
            _last_saved = load_setting(f"last_saved_{uploader_key}", "")
            if st.button(f"📤 Google Sheets에 저장 (전체 {len(all_save_data)}주차)", key=f"save_{uploader_key}"):
                try:
                    for _lbl in valid_labels:
                        if _lbl in all_save_data and not all_save_data[_lbl].empty:
                            append_rank_history(all_save_data[_lbl], _lbl, sheet_name)
                    _now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
                    save_setting(f"last_saved_{uploader_key}", _now_str)
                    st.cache_data.clear()
                    st.success(f"저장 완료! (총 {len(all_save_data)}주차)")
                    st.rerun()
                except Exception as _save_err:
                    st.error(f"저장 실패: {_save_err}")
            if _last_saved:
                st.markdown(f"<span style='color:#475569;font-size:0.82rem;font-weight:600;'>🕐 마지막 저장: {_last_saved}</span>", unsafe_allow_html=True)

    # ══ 섹션 2: 테이블 (단일 주차) ══
    elif _parsed_summary is not None:
        _disp = _parsed_summary.sort_values("avg_rank").copy()
        _disp = _multiselect_filter(_disp, f"{uploader_key}_up")
        _disp = _disp.reset_index(drop=True)

        # 스타일링용 원본 avg_rank 캡처 (keyword ⚠️ 변환 전)
        _style_ranks = pd.to_numeric(_disp["avg_rank"], errors="coerce") if "avg_rank" in _disp.columns else pd.Series(dtype=float)
        _style_kws   = _disp["keyword"].tolist() if "keyword" in _disp.columns else []

        # 지난주 순위 로드 (이력 시트 마지막 저장 주차)
        _prev_rank_map: dict = {}
        if use_styling:
            _ph = load_fn()
            if not _ph.empty:
                _phdc = [c for c in _ph.columns if c not in {"keyword", "계절", "품목"}]
                if _phdc:
                    _prev_rank_map = _ph.set_index("keyword")[_phdc[-1]].to_dict()

        # 키워드 아이콘 추가 (🔻: 4이상 하락, ⚠️: 10위 초과)
        def _add_kw_icon(r):
            kw = r["keyword"]
            this_r = pd.to_numeric(r["avg_rank"], errors="coerce")
            prev_r = pd.to_numeric(_prev_rank_map.get(str(kw).strip()), errors="coerce") if _prev_rank_map else float("nan")
            if pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4:
                return f"🔻 {kw}"
            elif pd.notna(this_r) and this_r > 10:
                return f"⚠️ {kw}"
            return kw

        _disp["keyword"] = _disp.apply(_add_kw_icon, axis=1)
        _col_order = ["keyword"] + [c for c in ("계절", "품목") if c in _disp.columns]
        _col_order += [c for c in _RANK_SHOW_COLS if c != "keyword" and c in _disp.columns]
        _disp = _disp[[c for c in _col_order if c in _disp.columns]]
        _rank_col_label = f"평균노출순위 ({_date_label})"
        _disp = _disp.rename(columns={**_RANK_COL_KR, "avg_rank": _rank_col_label})
        _disp = _disp.reset_index(drop=True)

        st.metric("키워드 수", len(_disp))

        _num_cols_disp = {c: "{:,.0f}" for c in _disp.columns if pd.api.types.is_numeric_dtype(_disp[c])}
        if use_styling:
            def _upload_style(df):
                result = pd.DataFrame("", index=df.index, columns=df.columns)
                _rank_label = [c for c in df.columns if "평균노출순위" in c]
                _rank_ci = df.columns.get_loc(_rank_label[0]) if _rank_label else -1
                for i in range(len(df)):
                    this_r = _style_ranks.iloc[i] if i < len(_style_ranks) else float("nan")
                    orig_kw = _style_kws[i] if i < len(_style_kws) else ""
                    prev_r = pd.to_numeric(_prev_rank_map.get(str(orig_kw).strip()), errors="coerce")
                    is_drop = pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4
                    is_warn = pd.notna(this_r) and this_r > 10
                    if _rank_ci >= 0:
                        if is_drop:
                            result.iloc[i, _rank_ci] = "background-color: #e3f2fd"
                        elif is_warn:
                            result.iloc[i, _rank_ci] = "background-color: #fff9c4"
                return result
            st.dataframe(
                _disp.style.apply(_upload_style, axis=None).format(_num_cols_disp, na_rep="-"),
                use_container_width=True, hide_index=True, height=350,
            )
        else:
            st.dataframe(
                _disp.style.format(_num_cols_disp, na_rep="-"),
                use_container_width=True, hide_index=True, height=350,
            )

        # ══ 섹션 3: 저장 버튼 ══
        _last_saved = load_setting(f"last_saved_{uploader_key}", "")
        if st.button("📤 Google Sheets에 저장", key=f"save_{uploader_key}"):
            try:
                append_rank_history(_parsed_summary, _date_label, sheet_name)
                _now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
                save_setting(f"last_saved_{uploader_key}", _now_str)
                st.cache_data.clear()
                st.success(f"저장 완료! ({_date_label})")
                st.rerun()
            except Exception as _save_err:
                st.error(f"저장 실패: {_save_err}")
        if _last_saved:
            st.markdown(f"<span style='color:#475569;font-size:0.82rem;font-weight:600;'>🕐 마지막 저장: {_last_saved}</span>", unsafe_allow_html=True)

    else:
        _hist = load_fn()
        if not _hist.empty:
            _hist = _merge_meta(_hist)
            _meta_cols = {"keyword", "계절", "품목"}
            _date_cols = [c for c in _hist.columns if c not in _meta_cols]
            _col_order = ["keyword"] + [c for c in ("계절", "품목") if c in _hist.columns] + _date_cols
            _hist = _hist[[c for c in _col_order if c in _hist.columns]]

            _sel_date_cols: list = []
            if _date_cols:
                _sel_date_cols = _period_filter(_date_cols, uploader_key)
                _non_date = [c for c in _hist.columns if c not in _date_cols]
                _hist = _hist[_non_date + _sel_date_cols]

            _hist = _multiselect_filter(_hist, f"{uploader_key}_hist")
            _hist = _hist.reset_index(drop=True)

            # 히스토리 뷰에도 아이콘 추가
            if use_styling and len(_sel_date_cols) >= 2:
                _h_this = _sel_date_cols[-1]
                _h_prev = _sel_date_cols[-2]
                _h_this_nums = pd.to_numeric(_hist[_h_this], errors="coerce") if _h_this in _hist.columns else pd.Series(dtype=float)
                _h_prev_nums = pd.to_numeric(_hist[_h_prev], errors="coerce") if _h_prev in _hist.columns else pd.Series(dtype=float)

                def _hist_kw_icon(i):
                    kw = str(_hist["keyword"].iloc[i])
                    this_r = _h_this_nums.iloc[i] if i < len(_h_this_nums) else float("nan")
                    prev_r = _h_prev_nums.iloc[i] if i < len(_h_prev_nums) else float("nan")
                    if pd.notna(this_r) and pd.notna(prev_r) and (this_r - prev_r) >= 4:
                        return f"🔻 {kw}"
                    elif pd.notna(this_r) and this_r > 10:
                        return f"⚠️ {kw}"
                    return kw

                _hist["keyword"] = [_hist_kw_icon(i) for i in range(len(_hist))]

            _hist_num_fmt = {c: "{:,.0f}" for c in _hist.columns if pd.api.types.is_numeric_dtype(_hist[c])}
            if use_styling and len(_sel_date_cols) >= 2:
                _this_col = _sel_date_cols[-1]
                _prev_col = _sel_date_cols[-2]
                st.dataframe(
                    _hist.style.apply(_rank_style, this_col=_this_col, prev_col=_prev_col, axis=None)
                         .format(_hist_num_fmt, na_rep="-"),
                    use_container_width=True, hide_index=True, height=350,
                )
            else:
                st.dataframe(
                    _hist.style.format(_hist_num_fmt, na_rep="-"),
                    use_container_width=True, hide_index=True, height=350,
                )
        else:
            st.info("데이터가 없습니다. CSV 파일을 업로드해주세요.")


# ── 블로그/카페 순위 공통 테이블 렌더링 ──────────────
def _render_blog_cafe_table(raw_df: pd.DataFrame, key_prefix: str):
    """블로그/카페 순위 이력 테이블 렌더링 (공통)"""
    if raw_df.empty:
        st.info("데이터가 없습니다. 자동 조회 버튼을 눌러 수집해주세요.")
        return

    _bc_meta_set = {"keyword", "계절", "품목"}
    _bc_date_cols = [c for c in raw_df.columns if c not in _bc_meta_set]
    if not _bc_date_cols:
        st.warning("날짜 컬럼이 없습니다.")
        return

    _bcdf = _merge_meta(raw_df.copy()).reset_index(drop=True)
    _bc_this = _bc_date_cols[-1]
    _bc_prev = _bc_date_cols[-2] if len(_bc_date_cols) >= 2 else None
    st.caption(f"📅 이번주: {_bc_this}" + (f"  |  지난주: {_bc_prev}" if _bc_prev else ""))

    def _bc_parse(v):
        s = str(v).strip()
        if s in ("서치피드", ""):
            return None
        try:
            return float(s)
        except ValueError:
            return None

    def _bc_fmt(v):
        s = str(v).strip()
        if s == "서치피드":
            return "서치피드"
        n = _bc_parse(s)
        if n is None:
            return "-"
        return "순위권 밖" if int(n) == 0 else str(int(n))

    def _bc_icon(this_v, prev_v):
        if this_v is None or prev_v is None or this_v == 0 or prev_v == 0:
            return ""
        return "🔺" if this_v < prev_v else ("🔻" if this_v > prev_v else "")

    _bc_this_nums = _bcdf[_bc_this].apply(_bc_parse).reset_index(drop=True)
    _bc_prev_nums = (
        _bcdf[_bc_prev].apply(_bc_parse).reset_index(drop=True)
        if _bc_prev else pd.Series([None] * len(_bcdf), dtype=object)
    )
    _bc_icons = [_bc_icon(_bc_this_nums.iloc[i], _bc_prev_nums.iloc[i]) for i in range(len(_bcdf))]

    _bc_disp = pd.DataFrame()
    _bc_disp["키워드"] = [
        f"{ic} {kw}".strip() if ic else kw
        for ic, kw in zip(_bc_icons, _bcdf["keyword"])
    ]
    if "계절" in _bcdf.columns:
        _bc_disp["계절"] = _bcdf["계절"].values
    if "품목" in _bcdf.columns:
        _bc_disp["품목"] = _bcdf["품목"].values
    _bc_disp[f"이번주 ({_bc_this})"] = _bcdf[_bc_this].apply(_bc_fmt).values
    if _bc_prev:
        _bc_disp[f"지난주 ({_bc_prev})"] = _bcdf[_bc_prev].apply(_bc_fmt).values

    _bc_sort = _bc_this_nums.apply(lambda v: 9999 if (v is None or v == 0) else v)
    _bc_disp = _bc_disp.iloc[_bc_sort.argsort().values].reset_index(drop=True)

    st.metric("키워드 수", len(_bc_disp))
    st.dataframe(_bc_disp, use_container_width=True, hide_index=True, height=420)

    if len(_bc_date_cols) >= 2:
        with st.expander("📅 전체 이력 보기"):
            _bc_sel = _period_filter(_bc_date_cols, f"{key_prefix}_hist")
            _bc_hist = _bcdf[
                ["keyword"] + [c for c in ("계절", "품목") if c in _bcdf.columns] + _bc_sel
            ].copy()
            for _c in _bc_sel:
                _bc_hist[_c] = _bc_hist[_c].apply(_bc_fmt)
            st.dataframe(_bc_hist, use_container_width=True, hide_index=True, height=350)


# ── 주간 검색수 ──
if selected_menu == "📈 주간 검색수":
    # ── 인라인 필터 (가로 4열) — 저장된 값을 기본값으로 ─
    _s_opts = sorted(meta_df["계절"].dropna().unique().tolist()) if "계절" in meta_df.columns else []
    _c_opts = sorted(meta_df["카테고리"].dropna().unique().tolist()) if "카테고리" in meta_df.columns else []
    _g_opts = sorted(meta_df["성별/나이"].dropna().unique().tolist()) if "성별/나이" in meta_df.columns else []
    _wf1, _wf2, _wf3, _wf4 = st.columns(4)
    with _wf1:
        selected_seasons = st.multiselect(
            "계절", _s_opts,
            default=[v for v in _saved_filters.get("seasons", []) if v in _s_opts],
            placeholder="전체", key="w_seasons",
        )
    with _wf2:
        selected_categories = st.multiselect(
            "카테고리", _c_opts,
            default=[v for v in _saved_filters.get("categories", []) if v in _c_opts],
            placeholder="전체", key="w_categories",
        )
    with _wf3:
        selected_genders = st.multiselect(
            "성별/나이", _g_opts,
            default=[v for v in _saved_filters.get("genders", []) if v in _g_opts],
            placeholder="전체", key="w_genders",
        )
    with _wf4:
        keyword_search = st.text_input("🔎 키워드 검색", placeholder="키워드명 입력...", key="w_kw_search")
    # 변경 시 저장
    _cur_filters = {"seasons": selected_seasons, "categories": selected_categories, "genders": selected_genders}
    if _cur_filters != {k: _saved_filters.get(k, []) for k in ("seasons", "categories", "genders")}:
        _save_filter_state(_cur_filters)
        _saved_filters = _cur_filters

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

        # ── 차트 1: 키워드 성장 버블맵 (전체 너비)
        st.markdown("**🫧 키워드 성장 버블맵**")
        if week_cols and len(week_cols) >= 2 and not ranked.empty:
            # 이번주 검색수 5,000 이상인 것만 대상 + 검색수 상위 50개로 제한
            _bubble_df = ranked[ranked["이번주"] >= 5000][["keyword", "이번주", "지난주", "변화율"]].copy()
            _bubble_df = _bubble_df.dropna(subset=["변화율"])
            _bubble_df = _bubble_df.nlargest(50, "이번주")

            if not _bubble_df.empty:
                # 버블 색상: 양수=빨간계열, 음수=파란계열
                _bubble_df["color"] = _bubble_df["변화율"].apply(
                    lambda v: f"rgba(220,53,69,{min(0.4 + abs(v)/200, 0.95):.2f})"
                    if v >= 0
                    else f"rgba(13,110,253,{min(0.4 + abs(v)/200, 0.95):.2f})"
                )
                # 버블 크기: 이번주 검색수에 비례
                _max_vol = _bubble_df["이번주"].max()
                _bubble_df["size"] = (_bubble_df["이번주"] / _max_vol * 80).clip(lower=8)

                fig_bubble = go.Figure()
                fig_bubble.add_trace(go.Scatter(
                    x=_bubble_df["이번주"],
                    y=_bubble_df["변화율"],
                    mode="markers+text",
                    marker=dict(
                        size=_bubble_df["size"],
                        color=_bubble_df["color"],
                        line=dict(width=0.5, color="rgba(255,255,255,0.6)"),
                        sizemode="diameter",
                    ),
                    text=_bubble_df["keyword"],
                    textposition="middle center",
                    textfont=dict(size=10, color="#1a1a1a"),
                    customdata=_bubble_df[["keyword", "이번주", "지난주", "변화율"]].values,
                    hovertemplate=(
                        "<b>%{customdata[0]}</b><br>"
                        "이번주: %{customdata[1]:,.0f}<br>"
                        "지난주: %{customdata[2]:,.0f}<br>"
                        "변화율: %{customdata[3]:+.1f}%"
                        "<extra></extra>"
                    ),
                ))
                # y=0 기준선 점선
                fig_bubble.add_hline(
                    y=0, line_dash="dot", line_color="gray", line_width=1.5
                )
                fig_bubble.update_layout(
                    title=dict(text="키워드 성장 버블맵", font=dict(size=14), x=0.5, xanchor="center"),
                    xaxis=dict(title="이번주 검색수", tickformat=","),
                    yaxis=dict(title="전주 대비 변화율 (%)"),
                    margin=dict(t=50, b=50, l=60, r=30),
                    height=500,
                    showlegend=False,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig_bubble, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
            else:
                st.info("조건에 맞는 키워드 없음\n(이번주 검색수 1,000 이상 필요)")
        else:
            st.info("데이터 부족 (최소 2주 필요)")

        # ── 차트 2·3: 검색수 TOP 10 + 계절 전환 지표 (나란히 작게)
        ch2, ch3 = st.columns(2)

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
                    st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
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
                    st.plotly_chart(fig_line, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})
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

        # ── 급상승/급하락 TOP 100 (선택 기간 마지막 2주 기준)
        if not period_ranked.empty:
            _top_fmt = {
                "이번주": "{:,.0f}", "지난주": "{:,.0f}", "변화량": "{:,.0f}",
                "변화율": lambda x: f"{x:+.1f}%" if pd.notna(x) else "-",
            }
            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("🔥 급상승 TOP 100")
                top_up = period_ranked.nlargest(100, "변화량")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_up.style.format(_top_fmt, na_rep="-"), use_container_width=True, hide_index=True)
            with col_right:
                st.subheader("❄️ 급하락 TOP 100")
                top_down = period_ranked.nsmallest(100, "변화량")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(top_down.style.format(_top_fmt, na_rep="-"), use_container_width=True, hide_index=True)

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
            _num_safe = lambda x: f"{x:,.0f}" if isinstance(x, (int, float)) and pd.notna(x) else x
            st.dataframe(
                _rank_tbl.style.format({"이번주": "{:,.0f}", "지난주": _num_safe}, na_rep="-"),
                use_container_width=True, hide_index=True, height=400,
            )

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
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

        # ── 전체 데이터 테이블 (선택 기간)
        st.subheader("📋 전체 데이터")
        _wk_fmt = {c: "{:,.0f}" for c in period_filtered.columns if c != "keyword"}
        st.dataframe(
            period_filtered.style.format(_wk_fmt, na_rep="-"),
            use_container_width=True, hide_index=True, height=400,
        )


# ── 연간 트렌드 ──
elif selected_menu == "📊 연간 트렌드":
    # ── 인라인 필터 (가로 4열) ────────────────────
    _s_opts = sorted(meta_df["계절"].dropna().unique().tolist()) if "계절" in meta_df.columns else []
    _c_opts = sorted(meta_df["카테고리"].dropna().unique().tolist()) if "카테고리" in meta_df.columns else []
    _g_opts = sorted(meta_df["성별/나이"].dropna().unique().tolist()) if "성별/나이" in meta_df.columns else []
    _tf1, _tf2, _tf3, _tf4 = st.columns(4)
    with _tf1:
        selected_seasons = st.multiselect("계절", _s_opts, placeholder="전체", key="t_seasons")
    with _tf2:
        selected_categories = st.multiselect("카테고리", _c_opts, placeholder="전체", key="t_categories")
    with _tf3:
        selected_genders = st.multiselect("성별/나이", _g_opts, placeholder="전체", key="t_genders")
    with _tf4:
        keyword_search = st.text_input("🔎 키워드 검색", placeholder="키워드명 입력...", key="t_kw_search")

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

        # 인라인 필터 적용 (선택한 계절/카테고리/성별나이/키워드 검색)
        _kw_filter_df = apply_filters(pd.DataFrame({"keyword": avail_kws}))
        avail_kws = _kw_filter_df["keyword"].tolist()

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
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})

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
                            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False, "scrollZoom": False})


# ── 쇼핑검색 순위 ──
elif selected_menu == "🛒 쇼핑검색 순위":
    _render_rank_tab(
        upload_label="쇼핑검색 리포트 CSV 업로드",
        uploader_key="shopping_upload",
        ad_type="shopping",
        expected_type="쇼핑검색",
        sheet_name=config.SHEET_NAME_RANK_SHOPPING,
        load_fn=load_rank_shopping,
        tab_label="쇼핑검색",
        use_styling=True,
        multi_week=True,
    )


# ── 파워링크 순위 ──
elif selected_menu == "🔗 파워링크 순위":
    _render_rank_tab(
        upload_label="파워링크 리포트 CSV 업로드",
        uploader_key="powerlink_upload",
        ad_type="powerlink",
        expected_type="파워링크",
        sheet_name=config.SHEET_NAME_RANK_POWERLINK,
        load_fn=load_rank_powerlink,
        tab_label="파워링크",
        use_styling=True,
        multi_week=True,
    )


# ── 블로그/카페 순위 ──
elif selected_menu == "📝 블로그/카페 순위":
    st.subheader("📝 블로그/카페 순위")

    # ── 인라인 필터 (3열) — meta_df 기반 키워드 필터링
    _bc_f1, _bc_f2, _bc_f3 = st.columns(3)
    _bc_s_opts = sorted(meta_df["계절"].dropna().unique().tolist()) if "계절" in meta_df.columns else []
    _bc_c_opts = sorted(meta_df["카테고리"].dropna().unique().tolist()) if "카테고리" in meta_df.columns else []
    _bc_g_opts = sorted(meta_df["성별/나이"].dropna().unique().tolist()) if "성별/나이" in meta_df.columns else []
    with _bc_f1:
        _bc_sel_seasons = st.multiselect("계절", _bc_s_opts, placeholder="전체", key="bc_seasons")
    with _bc_f2:
        _bc_sel_cats = st.multiselect("카테고리", _bc_c_opts, placeholder="전체", key="bc_cats")
    with _bc_f3:
        _bc_sel_genders = st.multiselect("성별/나이", _bc_g_opts, placeholder="전체", key="bc_genders")

    # 필터 적용: 선택 시 meta_df 기준 교집합, 미선택 시 keywords.xlsx 전체
    def _bc_filter_keywords() -> list:
        from fetch_weekly_data import load_keywords as _lkw
        if not _bc_sel_seasons and not _bc_sel_cats and not _bc_sel_genders:
            return _lkw()
        if meta_df.empty or "keyword" not in meta_df.columns:
            return _lkw()
        _mask = pd.Series(True, index=meta_df.index)
        if _bc_sel_seasons:
            _mask &= meta_df["계절"].apply(
                lambda x: any(s in str(x) for s in _bc_sel_seasons) if pd.notna(x) else False
            )
        if _bc_sel_cats and "카테고리" in meta_df.columns:
            _mask &= meta_df["카테고리"].isin(_bc_sel_cats)
        if _bc_sel_genders and "성별/나이" in meta_df.columns:
            _mask &= meta_df["성별/나이"].isin(_bc_sel_genders)
        return meta_df[_mask]["keyword"].dropna().astype(str).tolist()

    _bc_kws_preview = _bc_filter_keywords()
    st.caption(f"조회 대상 키워드: **{len(_bc_kws_preview)}개**" +
               (" (전체)" if not _bc_sel_seasons and not _bc_sel_cats and not _bc_sel_genders else " (필터 적용)"))

    # ── API 응답 진단 (bloggerlink 확인용)
    with st.expander("🔧 API 응답 진단 (bloggerlink 확인)"):
        _diag_kw = st.text_input("테스트할 키워드", placeholder="예: 아동복", key="diag_kw")
        if st.button("진단 실행", key="diag_run"):
            if _diag_kw:
                import requests as _req
                _diag_headers = {
                    "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
                    "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
                }
                try:
                    _r = _req.get(
                        "https://openapi.naver.com/v1/search/blog.json",
                        headers=_diag_headers,
                        params={"query": _diag_kw, "display": 10, "start": 1, "sort": "sim"},
                        timeout=10,
                    )
                    if _r.status_code == 200:
                        _items = _r.json().get("items", [])
                        st.caption(f"총 {len(_items)}개 결과 반환")
                        for _ii, _it in enumerate(_items[:10], 1):
                            st.markdown(
                                f"**{_ii}.** bloggername=`{_it.get('bloggername','')}` "
                                f"| bloggerlink=`{_it.get('bloggerlink','')}` "
                                f"| link=`{_it.get('link','')[:60]}...`"
                            )
                    else:
                        st.error(f"API 오류 {_r.status_code}: {_r.text[:200]}")
                except Exception as _de:
                    st.error(f"요청 실패: {_de}")
            else:
                st.warning("키워드를 입력하세요.")

    # ── 자동 조회 버튼 (2열 나란히)
    _btn_col1, _btn_col2 = st.columns(2)

    with _btn_col1:
        if st.button("🔍 블로그 순위 자동 조회", key="fetch_blog", type="primary"):
            _blog_prog = st.empty()
            try:
                from fetch_weekly_data import get_week_label as _get_wl
                from naver_api import fetch_blog_rank
                _blog_prog.info("⏳ 키워드 로드 중...")
                _kws = _bc_filter_keywords()
                _wl = _get_wl()
                _blog_pbar = st.progress(0)
                _blog_ptxt = st.empty()
                def _blog_cb(idx, total, kw):
                    _blog_pbar.progress((idx + 1) / total)
                    _blog_ptxt.caption(f"조회 중 ({idx+1}/{total}): {kw}")
                _blog_result = fetch_blog_rank(_kws, progress_cb=_blog_cb)
                _blog_pbar.empty(); _blog_ptxt.empty()
                if not _blog_result.empty:
                    append_rank_history(
                        _blog_result.rename(columns={"rank": "avg_rank"}),
                        _wl, config.SHEET_NAME_RANK_BLOG,
                    )
                    st.cache_data.clear()
                    _blog_prog.success(f"✅ 블로그 저장 완료! ({_wl}, {len(_blog_result)}개 키워드)")
                else:
                    _blog_prog.warning("결과 없음")
            except Exception as _e:
                import traceback
                _blog_prog.error(f"❌ 오류: {_e}")
                st.code(traceback.format_exc())

    with _btn_col2:
        if st.button("🔍 카페 순위 자동 조회", key="fetch_cafe", type="primary"):
            _cafe_prog = st.empty()
            try:
                from fetch_weekly_data import get_week_label as _get_wl2
                from naver_api import fetch_cafe_rank
                _cafe_prog.info("⏳ 키워드 로드 중...")
                _kws2 = _bc_filter_keywords()
                _wl2 = _get_wl2()
                _cafe_pbar = st.progress(0)
                _cafe_ptxt = st.empty()
                def _cafe_cb(idx, total, kw):
                    _cafe_pbar.progress((idx + 1) / total)
                    _cafe_ptxt.caption(f"조회 중 ({idx+1}/{total}): {kw}")
                _cafe_result = fetch_cafe_rank(_kws2, progress_cb=_cafe_cb)
                _cafe_pbar.empty(); _cafe_ptxt.empty()
                if not _cafe_result.empty:
                    append_rank_history(
                        _cafe_result.rename(columns={"rank": "avg_rank"}),
                        _wl2, config.SHEET_NAME_RANK_CAFE,
                    )
                    st.cache_data.clear()
                    _cafe_prog.success(f"✅ 카페 저장 완료! ({_wl2}, {len(_cafe_result)}개 키워드)")
                else:
                    _cafe_prog.warning("결과 없음")
            except Exception as _e:
                import traceback
                _cafe_prog.error(f"❌ 오류: {_e}")
                st.code(traceback.format_exc())

    st.markdown("---")

    # ── 블로그 순위 테이블 (필터 적용)
    st.subheader("📝 블로그 순위")
    _blog_raw = load_rank_blog()
    _is_filtered = bool(_bc_sel_seasons or _bc_sel_cats or _bc_sel_genders)
    if _is_filtered and not _blog_raw.empty and _bc_kws_preview:
        _blog_raw = _blog_raw[_blog_raw["keyword"].isin(_bc_kws_preview)].reset_index(drop=True)
    _render_blog_cafe_table(_blog_raw, "blog")

    st.markdown("---")

    # ── 카페 순위 테이블 (필터 적용)
    st.subheader("☕ 카페 순위")
    _cafe_raw = load_rank_cafe()
    if _is_filtered and not _cafe_raw.empty and _bc_kws_preview:
        _cafe_raw = _cafe_raw[_cafe_raw["keyword"].isin(_bc_kws_preview)].reset_index(drop=True)
    _render_blog_cafe_table(_cafe_raw, "cafe")


# ── 데이터 관리 ──
elif selected_menu == "🆕 신규키워드 개발":
    st.subheader("🆕 신규키워드 개발")

    # ── 메타 데이터에서 카테고리/타겟 목록 추출
    _nk_categories = ["(선택 안함)"]
    _nk_targets = ["(선택 안함)"]
    if not meta_df.empty:
        if "카테고리" in meta_df.columns:
            _nk_categories += sorted(meta_df["카테고리"].dropna().unique().tolist())
        if "성별/나이" in meta_df.columns:
            _nk_targets += sorted(meta_df["성별/나이"].dropna().unique().tolist())

    # ── 입력 폼
    with st.form("new_keyword_form"):
        _nk_product = st.text_input(
            "제품명",
            placeholder="예: 아기옷, 바람막이, 체험장갑",
        )
        _nk_col2, _nk_col3 = st.columns(2)
        with _nk_col2:
            _nk_category = st.selectbox("카테고리", _nk_categories)
        with _nk_col3:
            _nk_target = st.selectbox("타겟", _nk_targets)
        _nk_submitted = st.form_submit_button("🔍 연관 키워드 조회", type="primary", use_container_width=True)

    if _nk_submitted:
        if not _nk_product.strip():
            st.warning("제품명을 입력해주세요.")
        else:
            _nk_cat_val = "" if _nk_category == "(선택 안함)" else _nk_category
            _nk_tgt_val = "" if _nk_target == "(선택 안함)" else _nk_target

            # 쉼표, 줄바꿈으로 구분하여 시드 키워드 추출
            # "유아 목장갑" → ["유아목장갑"] (붙여쓰기만, 개별 단어 분리 안함)
            # "유아목장갑, 아기장갑" → ["유아목장갑", "아기장갑"]
            import re
            _nk_raw_tokens = [k.strip() for k in re.split(r'[,\n]+', _nk_product) if k.strip()]
            _nk_seed_keywords = []
            for _token in _nk_raw_tokens:
                _nk_seed_keywords.append(_token.replace(" ", ""))  # 공백 제거한 버전만
            _nk_seed_keywords = list(dict.fromkeys(_nk_seed_keywords))

            _nk_naver_df = pd.DataFrame()

            with st.spinner("네이버 블로그/카페/쇼핑 검색 결과에서 연관 키워드 추출 중..."):
                try:
                    import traceback as _nk_tb
                    from naver_api import suggest_related_keywords

                    st.caption(f"🔎 시드 키워드: `{_nk_seed_keywords}`")

                    # 각 시드 키워드별로 연관 키워드 추천
                    _all_suggestions = []
                    _all_context = []
                    for _seed in _nk_seed_keywords:
                        _result = suggest_related_keywords(_seed, max_results=30)
                        if isinstance(_result, dict):
                            _all_suggestions.extend(_result.get("results", []))
                            _all_context.extend(_result.get("context_words", []))
                        elif isinstance(_result, list):
                            _all_suggestions.extend(_result)

                    # 맥락 단어 디버그 표시
                    if _all_context:
                        with st.expander(f"🔍 맥락 단어 ({len(_all_context)}개) — 블로그/카페/쇼핑에서 추출", expanded=False):
                            _ctx_text = ", ".join([f"{kw}({cnt})" for kw, cnt in _all_context[:50]])
                            st.caption(_ctx_text)

                    if _all_suggestions:
                        _nk_naver_df = pd.DataFrame(_all_suggestions)
                        # 중복 키워드 병합: 검색수는 max, 빈도는 sum
                        _agg = {"월간검색수": "max"}
                        if "출현빈도" in _nk_naver_df.columns:
                            _agg["출현빈도"] = "sum"
                        _nk_naver_df = (
                            _nk_naver_df.groupby("keyword", as_index=False).agg(_agg)
                            .sort_values("월간검색수", ascending=False)
                            .reset_index(drop=True)
                        )
                        st.caption(f"📊 추천 키워드: **{len(_nk_naver_df)}개** (블로그/카페/쇼핑 검색 결과 분석)")
                    else:
                        st.warning("연관 키워드를 찾지 못했습니다. 다른 키워드로 시도해보세요.")
                except Exception as _nk_e:
                    st.error(f"키워드 추천 오류: {_nk_e}")
                    st.code(_nk_tb.format_exc())

            st.session_state["_nk_result"] = {
                "product": _nk_product.strip(),
                "category": _nk_cat_val,
                "target": _nk_tgt_val,
                "naver_df": _nk_naver_df,
            }

    # ── 결과 표시
    if "_nk_result" in st.session_state:
        _res = st.session_state["_nk_result"]
        _nk_naver_df = _res["naver_df"]
        _nk_product_res = _res["product"]
        _nk_cat_res = _res["category"]
        _nk_tgt_res = _res["target"]

        st.markdown("---")
        st.markdown("#### 🔗 네이버 연관 키워드")
        if not _nk_naver_df.empty:
            st.success(f"총 **{len(_nk_naver_df)}개** 연관 키워드 발견")
            _fmt = {"월간검색수": "{:,.0f}"}
            if "출현빈도" in _nk_naver_df.columns:
                _fmt["출현빈도"] = "{:,.0f}"
            st.dataframe(
                _nk_naver_df.style.format(_fmt),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("네이버 API 결과 없음")

        # ── Google Sheets 저장
        if st.button("📤 Google Sheets에 저장", type="primary"):
            _today = datetime.now(KST).strftime("%Y-%m-%d")
            _rows_to_save = [
                {
                    "날짜": _today, "제품명": _nk_product_res,
                    "카테고리": _nk_cat_res, "타겟": _nk_tgt_res,
                    "키워드": _r["keyword"], "출처": "네이버API",
                    "월간검색수": int(_r["월간검색수"]),
                }
                for _, _r in _nk_naver_df.iterrows()
            ]
            try:
                save_new_keywords(_rows_to_save)
                st.success(f"✅ {len(_rows_to_save)}개 키워드를 Google Sheets에 저장했습니다.")
            except Exception as _e:
                st.error(f"저장 오류: {_e}")

    # ── 저장 이력 표시
    st.markdown("---")
    st.markdown("#### 📋 신규키워드 개발 이력")
    try:
        _nk_history = read_new_keywords()
        if _nk_history.empty:
            st.info("저장된 이력이 없습니다.")
        else:
            st.dataframe(_nk_history, use_container_width=True, hide_index=True)
    except Exception as _e:
        st.info(f"이력 로드 불가: {_e}")

elif selected_menu == "⚙️ 데이터 관리":
    st.subheader("⚙️ 데이터 관리 & 설정")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📖 키워드사전 (Apps Script 수집)")
        st.caption("Google Sheets의 '키워드사전' 탭에서 데이터를 읽어옵니다. 데이터 수집은 Apps Script에서 실행하세요.")

        _kd = load_keyword_dict()
        if _kd.empty:
            st.warning("키워드사전 탭에 데이터가 없습니다. Apps Script로 수집을 실행하세요.")
        else:
            _meta_cols = [c for c in ["계절", "복종", "연령", "성별", "카테고리", "대표키워드", "키워드"] if c in _kd.columns]
            _week_cols = [c for c in _kd.columns if c not in set(_meta_cols) | {"keyword"}]
            st.success(f"✅ 키워드: **{len(_kd)}개** | 주차 데이터: **{len(_week_cols)}개**")

            # 최신 주차 미리보기
            if _week_cols:
                _latest = _week_cols[-1]
                st.caption(f"최신 주차: `{_latest}`")

            st.dataframe(_kd[_meta_cols[:3] + ["keyword"] + _week_cols[-3:]].head(20) if _week_cols else _kd.head(20),
                         use_container_width=True, hide_index=True)

        if st.button("🔄 캐시 새로고침", type="primary"):
            st.cache_data.clear()
            st.success("캐시를 초기화했습니다. 페이지를 새로고침(F5)하면 최신 데이터가 반영됩니다.")

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
        "키워드사전 시트": config.SHEET_NAME_KEYWORD_DICT,
        "변화율 알림 기준": f"±{config.CHANGE_ALERT_THRESHOLD}%",
        "네이버 검색광고 API": "✅ 설정됨" if config.NAVER_AD_API_LICENSE else "❌ 미설정",
        "네이버 데이터랩 API": "✅ 설정됨" if config.NAVER_CLIENT_ID else "❌ 미설정",
        "Google Credentials": "✅ 설정됨" if config.GOOGLE_CREDENTIALS_FILE else "❌ 미설정",
    })
