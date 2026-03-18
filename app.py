"""
오즈키즈 키워드 검색수 대시보드
Streamlit 기반 - 주간 검색수 트래킹 & 트렌드 분석

실행: streamlit run app.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

import config
from google_sheets import read_weekly_data, read_trend_data, read_rank_data
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

st.sidebar.image("https://brand.naver.com/ozkiz", width=50)  # 로고 대체
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
tab1, tab2, tab3, tab4 = st.tabs(["📈 주간 검색수", "📊 연간 트렌드", "🏆 광고 순위", "⚙️ 데이터 관리"])


# ── TAB 1: 주간 검색수 ──
with tab1:
    weekly_df = load_weekly()

    if weekly_df.empty:
        st.info("아직 수집된 데이터가 없습니다. `fetch_weekly_data.py`를 먼저 실행해주세요.")
    else:
        filtered = apply_filters(weekly_df)
        changes = calc_changes(filtered)
        week_cols = [c for c in filtered.columns if c != "keyword"]

        # 요약 카드
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"""<div class="metric-card">
                <h3>총 키워드 수</h3><p>{len(filtered):,}</p>
            </div>""", unsafe_allow_html=True)
        with col2:
            if week_cols:
                total = filtered[week_cols[-1]].sum()
                st.markdown(f"""<div class="metric-card">
                    <h3>이번 주 총 검색수</h3><p>{total:,}</p>
                </div>""", unsafe_allow_html=True)
        with col3:
            hot = (changes["변화율"] >= config.CHANGE_ALERT_THRESHOLD).sum() if "변화율" in changes.columns else 0
            st.markdown(f"""<div class="metric-card">
                <h3>🔥 급상승 키워드</h3><p>{hot}</p>
            </div>""", unsafe_allow_html=True)
        with col4:
            cold = (changes["변화율"] <= -config.CHANGE_ALERT_THRESHOLD).sum() if "변화율" in changes.columns else 0
            st.markdown(f"""<div class="metric-card">
                <h3>❄️ 급하락 키워드</h3><p>{cold}</p>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # 변화율 큰 키워드 하이라이트 (변화율 계산 가능한 키워드만)
        ranked = changes.dropna(subset=["변화율"]) if "변화율" in changes.columns else pd.DataFrame()
        if not ranked.empty:
            col_left, col_right = st.columns(2)
            with col_left:
                st.subheader("🔥 급상승 TOP 10")
                top_up = ranked.nlargest(10, "변화율")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(
                    top_up.style.format({"이번주": "{:,.0f}", "지난주": "{:,.0f}", "변화량": "{:+,.0f}", "변화율": "{:+.1f}%"})
                    .background_gradient(subset=["변화율"], cmap="Reds"),
                    use_container_width=True, hide_index=True,
                )
            with col_right:
                st.subheader("❄️ 급하락 TOP 10")
                top_down = ranked.nsmallest(10, "변화율")[["keyword", "이번주", "지난주", "변화량", "변화율"]]
                st.dataframe(
                    top_down.style.format({"이번주": "{:,.0f}", "지난주": "{:,.0f}", "변화량": "{:+,.0f}", "변화율": "{:+.1f}%"})
                    .background_gradient(subset=["변화율"], cmap="Blues_r"),
                    use_container_width=True, hide_index=True,
                )

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

    if trend_df.empty:
        st.info("트렌드 데이터가 없습니다. `fetch_weekly_data.py`를 실행하면 자동으로 수집됩니다.")
    else:
        st.subheader("📊 올해 vs 작년 검색 트렌드 비교")
        st.caption("네이버 데이터랩 비율 × 실제 검색수 기반 추정치")

        kw_cols = [c for c in trend_df.columns if c not in ("date", "keyword", "ratio", "estimated_weekly_volume")]

        # 피벗되지 않은 형태인 경우 처리
        if "keyword" in trend_df.columns and "estimated_weekly_volume" in trend_df.columns:
            avail_kws = trend_df["keyword"].unique().tolist()
            filtered_kws = [kw for kw in avail_kws if kw in apply_filters(weekly_df)["keyword"].values] if not weekly_df.empty else avail_kws
            trend_selected = st.multiselect("키워드 선택", filtered_kws, default=filtered_kws[:3], max_selections=5, key="trend_kw")

            if trend_selected:
                plot_data = trend_df[trend_df["keyword"].isin(trend_selected)].copy()
                plot_data["year"] = pd.to_datetime(plot_data["date"]).dt.year.astype(str)
                plot_data["week"] = pd.to_datetime(plot_data["date"]).dt.isocalendar().week.astype(int)

                fig = px.line(
                    plot_data, x="week", y="estimated_weekly_volume",
                    color="keyword", line_dash="year",
                    title="주간 추정 검색수 (올해 실선 / 작년 점선)",
                    labels={"week": "주차", "estimated_weekly_volume": "추정 검색수"},
                    template="plotly_white",
                )
                fig.update_layout(height=500, legend=dict(orientation="h", y=-0.2))
                st.plotly_chart(fig, use_container_width=True)

                # 비율(ratio) 원본 그래프
                with st.expander("📉 데이터랩 원본 비율 보기"):
                    fig2 = px.line(
                        plot_data, x="date", y="ratio", color="keyword",
                        title="데이터랩 검색 비율 (0~100)",
                        template="plotly_white",
                    )
                    st.plotly_chart(fig2, use_container_width=True)
        else:
            # 피벗 형태
            kw_options_trend = [c for c in trend_df.columns if c != "date"]
            trend_selected = st.multiselect("키워드 선택", kw_options_trend, default=kw_options_trend[:3], max_selections=5, key="trend_kw2")

            if trend_selected:
                plot_data = trend_df[["date"] + trend_selected].melt(id_vars="date", var_name="keyword", value_name="value")
                fig = px.line(plot_data, x="date", y="value", color="keyword", template="plotly_white")
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)


# ── TAB 3: 광고 순위 ──
with tab3:
    st.subheader("🏆 광고 순위 (네이버 검색광고 리포트)")

    # 파일 업로드
    uploaded = st.file_uploader(
        "네이버 검색광고 대시보드에서 다운받은 엑셀 파일을 업로드하세요",
        type=["xlsx", "xls", "csv"],
        help="파워링크 또는 쇼핑검색 리포트 파일",
    )

    if uploaded:
        ad_type = st.radio("광고 유형", ["자동 감지", "파워링크", "쇼핑검색"], horizontal=True)
        type_map = {"자동 감지": "auto", "파워링크": "powerlink", "쇼핑검색": "shopping"}

        try:
            report = parse_ad_report(uploaded, ad_type=type_map[ad_type])
            summary = summarize_by_keyword(report)

            col1, col2 = st.columns(2)
            with col1:
                st.metric("파싱된 키워드 수", f"{summary['keyword'].nunique():,}")
            with col2:
                st.metric("데이터 행 수", f"{len(report):,}")

            # 광고 유형별 탭
            for atype in summary["ad_type"].unique():
                st.markdown(f"#### {atype}")
                type_data = summary[summary["ad_type"] == atype].sort_values("avg_rank")
                st.dataframe(
                    type_data.style.format({
                        "avg_rank": "{:.1f}", "total_impressions": "{:,.0f}",
                        "total_clicks": "{:,.0f}", "total_cost": "₩{:,.0f}",
                    }).background_gradient(subset=["avg_rank"], cmap="RdYlGn_r"),
                    use_container_width=True, hide_index=True,
                )

            # Google Sheets에 저장
            if st.button("📤 Google Sheets에 저장"):
                from google_sheets import save_rank_data
                from fetch_weekly_data import get_week_label
                save_rank_data(summary, get_week_label())
                st.success("저장 완료!")

        except Exception as e:
            st.error(f"파일 파싱 실패: {e}")
            st.info("파일 형식이 다를 수 있습니다. 파일의 첫 몇 행을 확인해주세요.")

    # 기존 저장된 순위 데이터 표시
    rank_df = load_rank()
    if not rank_df.empty:
        st.markdown("---")
        st.subheader("📊 저장된 광고 순위 이력")
        st.dataframe(rank_df, use_container_width=True, hide_index=True, height=300)


# ── TAB 4: 데이터 관리 ──
with tab4:
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
