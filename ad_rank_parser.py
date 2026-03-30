"""
광고 순위 파서
네이버 검색광고 리포트 CSV를 파싱합니다.

파일 구조:
- 1행: 제목 (날짜 범위 포함, 예: "랭킹키워드_파워링크(2026.03.18.~2026.03.24.)")
- 2행: 컬럼명 (캠페인유형, PC/모바일 매체, 키워드, 검색어, 노출수, 클릭수, 총비용, 평균노출순위 ...)
- 3행~: 데이터

파싱 규칙:
- 모바일 데이터만 사용 ("PC/모바일 매체" == "모바일")
- 플레이스 캠페인 제외
- 쇼핑검색 → "검색어" 컬럼을 키워드로 사용
- 파워링크 → "키워드" 컬럼을 키워드로 사용
"""
import re
import io
import pandas as pd
from datetime import datetime, timedelta


def _find_col(cols: list, candidates: list) -> str | None:
    """컬럼 목록에서 후보와 부분 일치하는 첫 번째 컬럼 반환 (공백 제거 후 비교)."""
    for col in cols:
        clean = str(col).strip().replace(" ", "")
        for cand in candidates:
            if cand in clean:
                return col
    return None


def _to_num(val) -> float:
    """쉼표 포함 숫자 문자열 → float. 변환 불가 시 0."""
    if val is None:
        return 0.0
    try:
        if isinstance(val, float) and pd.isna(val):
            return 0.0
    except Exception:
        pass
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _extract_date_label(title: str) -> str:
    """
    제목 문자열에서 날짜 범위 추출.
    예: "(2026.03.18.~2026.03.24.)" → "2026.03.18-2026.03.24"
    """
    m = re.search(r"(\d{4}\.\d{2}\.\d{2})\.?\s*~\s*(\d{4}\.\d{2}\.\d{2})", title)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return ""


def _default_week_label() -> str:
    """현재 주 월~일 날짜 범위 (예: 2026.03.23-2026.03.29)"""
    now = datetime.now()
    monday = now - timedelta(days=now.weekday())
    sunday = monday + timedelta(days=6)
    return f"{monday.strftime('%Y.%m.%d')}-{sunday.strftime('%Y.%m.%d')}"


def _read_title(file_obj) -> str:
    """1행(제목)을 문자열로 읽어 반환. 날짜 추출에 사용."""
    for enc in ("utf-8-sig", "cp949"):
        try:
            if hasattr(file_obj, "seek"):
                file_obj.seek(0)
            row = pd.read_csv(file_obj, encoding=enc, nrows=1, header=None, dtype=str)
            return " ".join(str(v) for v in row.iloc[0].tolist() if pd.notna(v))
        except Exception:
            continue
    return ""


def _read_raw(file_obj) -> pd.DataFrame:
    """
    파일을 DataFrame으로 읽기.
    skiprows=1: 1행(제목) 건너뜀 → 2행이 컬럼명, 3행~이 데이터.
    읽기 순서: CSV(utf-8-sig) → CSV(cp949) → Excel
    """
    for enc in ("utf-8-sig", "cp949"):
        try:
            if hasattr(file_obj, "seek"):
                file_obj.seek(0)
            return pd.read_csv(file_obj, encoding=enc, skiprows=1, dtype=str)
        except Exception:
            continue

    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    return pd.read_excel(file_obj, skiprows=1, dtype=str)


def parse_ad_report(file_obj, ad_type: str = "auto") -> tuple:
    """
    네이버 검색광고 리포트를 파싱합니다.

    Args:
        file_obj : 파일 객체 (Streamlit UploadedFile 또는 경로)
        ad_type  : 'auto' | 'shopping' | 'powerlink' | 'blog'
                   'auto'이면 파일의 캠페인유형 컬럼으로 자동 구분

    Returns:
        (df, date_label)
        - df         : keyword, ad_type, avg_rank, impressions, clicks, cost 컬럼
        - date_label : "2026.03.18-2026.03.24" 형식
    """
    # ── 1행 제목에서 날짜 추출 (별도 읽기) ──
    title_str = _read_title(file_obj)
    date_label = _extract_date_label(title_str) or _default_week_label()

    # ── 데이터 읽기 (skiprows=1: 제목 건너뜀, 2행=컬럼명, 3행~=데이터) ──
    try:
        df = _read_raw(file_obj)
    except Exception as e:
        raise ValueError(f"파일을 읽을 수 없습니다: {e}")

    if df is None or df.empty:
        return pd.DataFrame(), date_label

    # 컬럼명 공백 정리
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(), date_label

    cols = df.columns.tolist()

    # ── 컬럼 탐색 ──
    campaign_col = _find_col(cols, ["캠페인유형"])
    media_col    = _find_col(cols, ["PC/모바일매체", "PC/모바일", "모바일매체"])
    kw_col       = _find_col(cols, ["키워드"])
    query_col    = _find_col(cols, ["검색어"])
    rank_col     = _find_col(cols, ["평균노출순위", "평균순위", "노출순위", "순위"])
    impr_col     = _find_col(cols, ["노출수"])
    click_col    = _find_col(cols, ["클릭수"])
    cost_col     = _find_col(cols, ["총비용", "비용"])

    # ── 모바일만 필터 ──
    if media_col:
        df = df[df[media_col].astype(str).str.contains("모바일", na=False)]

    # ── 플레이스 제외 ──
    if campaign_col:
        df = df[~df[campaign_col].astype(str).str.contains("플레이스", na=False)]

    df = df.reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(), date_label

    # ── 블로그 타입 ──
    if ad_type == "blog":
        kw_src = kw_col or query_col
        if not kw_src:
            raise ValueError(f"키워드 컬럼을 찾을 수 없습니다. 컬럼: {cols}")
        result = pd.DataFrame({
            "keyword":     df[kw_src].astype(str).str.strip(),
            "ad_type":     "블로그",
            "avg_rank":    df[rank_col].apply(_to_num) if rank_col  else 0.0,
            "impressions": df[impr_col].apply(_to_num) if impr_col  else 0.0,
            "clicks":      df[click_col].apply(_to_num) if click_col else 0.0,
            "cost":        df[cost_col].apply(_to_num)  if cost_col  else 0.0,
        })
        result = result[result["keyword"].str.strip().str.len() > 0].reset_index(drop=True)
        return result, date_label

    # ── auto / shopping / powerlink ──
    rows = []
    for _, row in df.iterrows():
        camp = str(row[campaign_col]).strip() if campaign_col else ""

        # ad_type 결정
        if ad_type == "shopping":
            resolved = "쇼핑검색"
        elif ad_type == "powerlink":
            resolved = "파워링크"
        elif "쇼핑" in camp:
            resolved = "쇼핑검색"
        elif "파워링크" in camp:
            resolved = "파워링크"
        else:
            resolved = camp or "기타"

        # 키워드 소스: 쇼핑검색 → 검색어 컬럼, 파워링크 → 키워드 컬럼
        if resolved == "쇼핑검색":
            kw_src = query_col or kw_col
        else:
            kw_src = kw_col or query_col

        kw = str(row[kw_src]).strip() if kw_src else ""
        if not kw or kw in ("nan", "None", ""):
            continue

        rows.append({
            "keyword":     kw,
            "ad_type":     resolved,
            "avg_rank":    _to_num(row[rank_col])  if rank_col  else 0.0,
            "impressions": _to_num(row[impr_col])  if impr_col  else 0.0,
            "clicks":      _to_num(row[click_col]) if click_col else 0.0,
            "cost":        _to_num(row[cost_col])  if cost_col  else 0.0,
        })

    result = pd.DataFrame(rows)
    if not result.empty:
        for c in ["avg_rank", "impressions", "clicks", "cost"]:
            result[c] = pd.to_numeric(result[c], errors="coerce").fillna(0)

    return result, date_label


def _parse_week_label(week_str: str) -> str:
    """
    '주별' 컬럼 값에서 날짜 추출.
    예: "2026.03.16.(월)주" → "2026.03.16"
    """
    m = re.search(r"(\d{4}\.\d{2}\.\d{2})", week_str)
    if m:
        return m.group(1)
    return week_str.strip()


def parse_ad_report_multiweek(file_obj, ad_type: str = "auto") -> tuple:
    """
    '주별' 컬럼이 있는 CSV를 파싱하여 주차별 데이터 반환.

    Returns:
        (week_dfs, week_labels)
        - week_dfs   : {week_label: summarized_df}  (keyword, ad_type, avg_rank, ...)
        - week_labels: 날짜 오름차순 정렬된 주차 레이블 목록
    """
    try:
        df_raw = _read_raw(file_obj)
    except Exception as e:
        raise ValueError(f"파일을 읽을 수 없습니다: {e}")

    if df_raw is None or df_raw.empty:
        return {}, []

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    df_raw = df_raw.dropna(how="all").reset_index(drop=True)
    cols = df_raw.columns.tolist()

    week_col = _find_col(cols, ["주별"])

    if not week_col:
        # 주별 컬럼 없음 → 단일 주 처리
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        df, date_label = parse_ad_report(file_obj, ad_type)
        if df.empty:
            return {}, []
        return {date_label: summarize_by_keyword(df)}, [date_label]

    # 주별 컬럼 있음 → 고유 주차 추출 및 정렬
    week_raw_map: dict = {}  # label → original value
    for v in df_raw[week_col].dropna().unique():
        label = _parse_week_label(str(v))
        week_raw_map[label] = str(v).strip()

    sorted_labels = sorted(week_raw_map.keys())

    # 공통 컬럼 탐색
    media_col    = _find_col(cols, ["PC/모바일매체", "PC/모바일", "모바일매체"])
    campaign_col = _find_col(cols, ["캠페인유형"])
    kw_col       = _find_col(cols, ["키워드"])
    query_col    = _find_col(cols, ["검색어"])
    rank_col     = _find_col(cols, ["평균노출순위", "평균순위", "노출순위", "순위"])
    impr_col     = _find_col(cols, ["노출수"])
    click_col    = _find_col(cols, ["클릭수"])
    cost_col     = _find_col(cols, ["총비용", "비용"])

    week_dfs: dict = {}
    for label in sorted_labels:
        orig_val = week_raw_map[label]
        week_rows = df_raw[df_raw[week_col].astype(str).str.strip() == orig_val].copy().reset_index(drop=True)

        if media_col:
            week_rows = week_rows[week_rows[media_col].astype(str).str.contains("모바일", na=False)]
        if campaign_col:
            week_rows = week_rows[~week_rows[campaign_col].astype(str).str.contains("플레이스", na=False)]

        week_rows = week_rows.reset_index(drop=True)
        if week_rows.empty:
            continue

        rows = []
        for _, row in week_rows.iterrows():
            camp = str(row[campaign_col]).strip() if campaign_col else ""

            if ad_type == "shopping":
                resolved = "쇼핑검색"
            elif ad_type == "powerlink":
                resolved = "파워링크"
            elif "쇼핑" in camp:
                resolved = "쇼핑검색"
            elif "파워링크" in camp:
                resolved = "파워링크"
            else:
                resolved = camp or "기타"

            kw_src = (query_col or kw_col) if resolved == "쇼핑검색" else (kw_col or query_col)
            kw = str(row[kw_src]).strip() if kw_src else ""
            if not kw or kw in ("nan", "None", ""):
                continue

            rows.append({
                "keyword":     kw,
                "ad_type":     resolved,
                "avg_rank":    _to_num(row[rank_col])  if rank_col  else 0.0,
                "impressions": _to_num(row[impr_col])  if impr_col  else 0.0,
                "clicks":      _to_num(row[click_col]) if click_col else 0.0,
                "cost":        _to_num(row[cost_col])  if cost_col  else 0.0,
            })

        if rows:
            result = pd.DataFrame(rows)
            for c in ["avg_rank", "impressions", "clicks", "cost"]:
                result[c] = pd.to_numeric(result[c], errors="coerce").fillna(0)
            week_dfs[label] = summarize_by_keyword(result)

    return week_dfs, sorted_labels


def summarize_by_keyword(df: pd.DataFrame) -> pd.DataFrame:
    """키워드별 광고 순위 요약 (ad_type별 그룹)"""
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
