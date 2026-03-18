"""
광고 순위 파서
네이버 검색광고 대시보드에서 다운받은 엑셀 파일을 파싱합니다.

지원 형식:
- 파워링크 리포트
- 쇼핑검색 리포트

파일 형식이 다를 수 있으므로, 컬럼명을 유연하게 매칭합니다.
"""
import pandas as pd


# 컬럼명 매핑 후보 (네이버 검색광고 리포트 형식)
COLUMN_MAPPINGS = {
    "keyword": ["키워드", "keyword", "검색어", "쿼리"],
    "campaign": ["캠페인", "campaign", "캠페인명"],
    "adgroup": ["광고그룹", "adgroup", "그룹"],
    "avg_rank": ["평균노출순위", "평균순위", "avg_rank", "노출순위", "Avg. Position"],
    "impressions": ["노출수", "impressions", "노출"],
    "clicks": ["클릭수", "clicks", "클릭"],
    "cost": ["비용", "cost", "총비용"],
    "ad_type": ["광고유형", "ad_type", "유형"],
}


def _find_column(df_columns: list, candidates: list[str]) -> str | None:
    """DataFrame 컬럼 중 매핑 후보와 일치하는 것을 찾습니다."""
    for col in df_columns:
        col_clean = col.strip().replace(" ", "")
        for candidate in candidates:
            if candidate in col_clean:
                return col
    return None


def parse_ad_report(file_path: str, ad_type: str = "auto") -> pd.DataFrame:
    """
    네이버 검색광고 리포트 엑셀을 파싱합니다.

    Args:
        file_path: 엑셀 파일 경로
        ad_type: 'powerlink', 'shopping', 또는 'auto' (자동 감지)

    Returns:
        DataFrame with columns: keyword, ad_type, avg_rank, impressions, clicks, cost
    """
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
    except Exception:
        try:
            df = pd.read_excel(file_path, engine="xlrd")
        except Exception:
            df = pd.read_csv(file_path, encoding="utf-8-sig")

    # 빈 행/헤더가 아닌 행 건너뛰기 (네이버 리포트는 상단에 요약 행이 있을 수 있음)
    if df.iloc[0].isna().sum() > len(df.columns) // 2:
        for idx in range(min(10, len(df))):
            if df.iloc[idx].notna().sum() >= 3:
                df.columns = df.iloc[idx]
                df = df.iloc[idx + 1:].reset_index(drop=True)
                break

    cols = df.columns.tolist()
    mapped = {}
    for key, candidates in COLUMN_MAPPINGS.items():
        found = _find_column(cols, candidates)
        if found:
            mapped[key] = found

    if "keyword" not in mapped:
        raise ValueError(f"키워드 컬럼을 찾을 수 없습니다. 컬럼: {cols}")

    result = pd.DataFrame()
    result["keyword"] = df[mapped["keyword"]].astype(str).str.strip()

    if ad_type == "auto":
        if "ad_type" in mapped:
            result["ad_type"] = df[mapped["ad_type"]].astype(str)
        elif any("쇼핑" in str(c) for c in cols) or any("shopping" in str(c).lower() for c in cols):
            result["ad_type"] = "쇼핑검색"
        else:
            result["ad_type"] = "파워링크"
    else:
        result["ad_type"] = "파워링크" if ad_type == "powerlink" else "쇼핑검색"

    for key in ["avg_rank", "impressions", "clicks", "cost"]:
        if key in mapped:
            result[key] = pd.to_numeric(df[mapped[key]], errors="coerce").fillna(0)
        else:
            result[key] = 0

    result = result[result["keyword"].str.len() > 0].reset_index(drop=True)
    return result


def summarize_by_keyword(df: pd.DataFrame) -> pd.DataFrame:
    """키워드별 광고 순위 요약"""
    if df.empty:
        return df

    summary = df.groupby(["keyword", "ad_type"]).agg(
        avg_rank=("avg_rank", "mean"),
        total_impressions=("impressions", "sum"),
        total_clicks=("clicks", "sum"),
        total_cost=("cost", "sum"),
    ).reset_index()

    summary["avg_rank"] = summary["avg_rank"].round(1)
    return summary
