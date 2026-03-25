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


def _read_raw(file_obj) -> pd.DataFrame:
    """파일 객체를 헤더 없이 원시 DataFrame으로 읽기. CSV → Excel 순서로 시도."""
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)

    # CSV 시도 (utf-8-sig → utf-8 → cp949 → euc-kr 순)
    if hasattr(file_obj, "read"):
        content = file_obj.read()
        if hasattr(file_obj, "seek"):
            file_obj.seek(0)
        for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
            try:
                buf = io.BytesIO(content) if isinstance(content, bytes) else io.StringIO(content)
                return pd.read_csv(buf, header=None, dtype=str, encoding=enc)
            except (UnicodeDecodeError, Exception):
                continue

    # Excel fallback
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    try:
        return pd.read_excel(file_obj, header=None, dtype=str, engine="openpyxl")
    except Exception:
        pass
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    return pd.read_excel(file_obj, header=None, dtype=str, engine="xlrd")


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
    raw = _read_raw(file_obj)

    if raw is None or raw.empty:
        return pd.DataFrame(), _default_week_label()

    # ── 1행: 제목에서 날짜 추출 ──
    title_str = " ".join(str(v) for v in raw.iloc[0].tolist() if pd.notna(v))
    date_label = _extract_date_label(title_str) or _default_week_label()

    # ── 2행: 컬럼명, 3행~: 데이터 ──
    if len(raw) < 3:
        return pd.DataFrame(), date_label

    header_row = raw.iloc[1].tolist()
    df = raw.iloc[2:].reset_index(drop=True).copy()
    df.columns = [
        str(h).strip() if (h is not None and str(h) != "nan") else f"_col{i}"
        for i, h in enumerate(header_row)
    ]
    df = df.dropna(how="all")
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
