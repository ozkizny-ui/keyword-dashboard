"""
Google Sheets 연동 모듈
- 주간 검색수 데이터 누적 저장
- 트렌드 데이터 저장
- 광고 순위 데이터 저장
"""
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

import config

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    """Google Sheets 클라이언트 생성
    우선순위: GOOGLE_CREDENTIALS_JSON (환경변수 또는 st.secrets) → credentials.json 파일
    config.py가 os.getenv + st.secrets를 통합 처리하므로 여기서는 config만 참조.
    """
    creds_json = config.GOOGLE_CREDENTIALS_JSON
    if creds_json:
        info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_or_create_sheet(spreadsheet, sheet_name: str):
    """시트가 없으면 생성"""
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=sheet_name, rows=5000, cols=50)


def append_weekly_data(df: pd.DataFrame, week_label: str):
    """
    주간 검색수 데이터를 Google Sheets에 누적 저장합니다.
    구조: keyword | 2025-W01 | 2025-W02 | ...
    """
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = _get_or_create_sheet(spreadsheet, config.SHEET_NAME_WEEKLY)

    existing = ws.get_all_values()

    if not existing or len(existing) == 0:
        # 첫 실행: 헤더 + 데이터 생성
        header = ["keyword", week_label]
        rows = [header]
        for _, row in df.iterrows():
            rows.append([str(row["keyword"]).strip(), row["totalSearchCount"]])
        ws.update(range_name="A1", values=rows)
        return

    header = existing[0]
    # 키워드 공백 제거 후 첫 번째 등장 행 기준으로 매핑 (중복 시 첫 행 업데이트)
    keyword_map = {}
    for i, r in enumerate(existing):
        if i == 0:
            continue
        kw = str(r[0]).strip() if r else ""
        if kw and kw not in keyword_map:
            keyword_map[kw] = i

    if week_label in header:
        col_idx = header.index(week_label)
    else:
        col_idx = len(header)
        header.append(week_label)
        ws.update_cell(1, col_idx + 1, week_label)

    cells_to_update = []
    new_rows = []

    for _, row in df.iterrows():
        kw = str(row["keyword"]).strip()
        val = int(row["totalSearchCount"])
        if kw in keyword_map:
            row_idx = keyword_map[kw] + 1  # 1-based
            cells_to_update.append(gspread.Cell(row_idx, col_idx + 1, val))
        else:
            new_row = [kw] + [""] * (col_idx - 1) + [val]
            new_rows.append(new_row)
            keyword_map[kw] = len(existing) + len(new_rows) - 1  # 중복 방지

    if cells_to_update:
        ws.update_cells(cells_to_update)

    if new_rows:
        ws.append_rows(new_rows)


def save_trend_data(df: pd.DataFrame):
    """연간 트렌드 데이터를 Google Sheets에 저장 (덮어쓰기)"""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = _get_or_create_sheet(spreadsheet, config.SHEET_NAME_TREND)

    ws.clear()
    header = df.columns.tolist()
    rows = [header] + df.astype(str).values.tolist()
    ws.update(range_name="A1", values=rows)


def save_rank_data(df: pd.DataFrame, week_label: str):
    """광고 순위 데이터를 Google Sheets에 저장"""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = _get_or_create_sheet(spreadsheet, config.SHEET_NAME_RANK)

    df_with_week = df.copy()
    df_with_week.insert(0, "week", week_label)

    existing = ws.get_all_values()
    if not existing:
        rows = [df_with_week.columns.tolist()] + df_with_week.astype(str).values.tolist()
        ws.update(range_name="A1", values=rows)
    else:
        ws.append_rows(df_with_week.astype(str).values.tolist())


def read_weekly_data() -> pd.DataFrame:
    """Google Sheets에서 주간 검색수 데이터를 읽어옵니다."""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(config.SHEET_NAME_WEEKLY)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    header = data[0]
    n_cols = len(header)
    # 헤더 길이에 맞춰 각 행을 패딩/잘라내기 (빈 셀·병합 셀 대응)
    rows = [row[:n_cols] + [""] * (n_cols - len(row)) for row in data[1:]]
    # 중복 컬럼명 자동 rename: 두 번째 이후 동일 이름에 _2, _3 ... 접미사 부여
    seen: dict = {}
    deduped_header = []
    for col in header:
        if col in seen:
            seen[col] += 1
            deduped_header.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            deduped_header.append(col)
    df = pd.DataFrame(rows, columns=deduped_header)
    # 첫 번째 컬럼을 "keyword"로 정규화 (Sheets에서 헤더가 다를 수 있음)
    df.rename(columns={df.columns[0]: "keyword"}, inplace=True)
    df["keyword"] = df["keyword"].str.strip()
    # 중복 키워드 제거 (첫 번째 값 유지 - 쓰기 경로와 동일하게 첫 행 기준)
    df = df.drop_duplicates(subset="keyword", keep="first").reset_index(drop=True)
    # 숫자 컬럼 변환 (쉼표 제거 후 정수 변환)
    for col in df.columns[1:]:
        series = df[col]
        if isinstance(series, pd.Series):
            df[col] = pd.to_numeric(
                series.str.replace(",", "", regex=False), errors="coerce"
            ).fillna(0).astype(int)
    return df


def read_trend_data() -> pd.DataFrame:
    """Google Sheets에서 연간 트렌드 데이터를 읽어옵니다."""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(config.SHEET_NAME_TREND)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    df = pd.DataFrame(data[1:], columns=data[0])
    # keyword, date 컬럼은 문자열/날짜 유지 - 숫자 변환 제외
    str_cols = {"date", "keyword"}
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in df.columns:
        if col not in str_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def append_rank_history(df: pd.DataFrame, week_label: str, sheet_name: str):
    """
    순위 이력을 계절 | 품목 | keyword | week1 | week2 | ... 구조로 누적 저장합니다.
    df: keyword, avg_rank, 계절(optional), 품목(optional) 컬럼
    """
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = _get_or_create_sheet(spreadsheet, sheet_name)

    rank_df = df[df["keyword"].notna()].copy()
    rank_df = rank_df[pd.to_numeric(rank_df["avg_rank"], errors="coerce").notna()]
    if rank_df.empty:
        return

    # keyword별 집계: avg_rank 평균, 계절/품목은 첫 번째 값
    rank_map = {}
    season_map = {}
    item_map = {}
    for kw, grp in rank_df.groupby("keyword"):
        rank_map[kw] = round(float(grp["avg_rank"].astype(float).mean()), 1)
        if "계절" in grp.columns:
            v = grp["계절"].iloc[0]
            season_map[kw] = "" if pd.isna(v) else str(v)
        if "품목" in grp.columns:
            v = grp["품목"].iloc[0]
            item_map[kw] = "" if pd.isna(v) else str(v)

    existing = ws.get_all_values()

    # 헤더에 "keyword" 컬럼이 없으면 시트 초기화 후 새로 작성
    if not existing or "keyword" not in existing[0]:
        ws.clear()
        header = ["계절", "품목", "keyword", week_label]
        rows = [header]
        for kw, val in rank_map.items():
            rows.append([season_map.get(kw, ""), item_map.get(kw, ""), kw, val])
        ws.update(range_name="A1", values=rows)
        return

    header = existing[0]
    kw_col_idx     = header.index("keyword")
    season_col_idx = header.index("계절")    if "계절"    in header else None
    item_col_idx   = header.index("품목")    if "품목"    in header else None

    # 키워드 공백 제거 후 첫 번째 등장 행 기준으로 매핑 (중복 시 첫 행 업데이트)
    keyword_row = {}
    for i, r in enumerate(existing):
        if i == 0 or len(r) <= kw_col_idx:
            continue
        kw = str(r[kw_col_idx]).strip()
        if kw and kw not in keyword_row:
            keyword_row[kw] = i

    if week_label in header:
        date_col_idx = header.index(week_label)
    else:
        date_col_idx = len(header)
        ws.update_cell(1, date_col_idx + 1, week_label)

    cells_to_update = []
    new_rows = []
    for kw, val in rank_map.items():
        kw = str(kw).strip()
        if kw in keyword_row:
            row_idx = keyword_row[kw] + 1  # 1-based
            cells_to_update.append(gspread.Cell(row_idx, date_col_idx + 1, val))
        else:
            new_row = [""] * (date_col_idx + 1)
            new_row[kw_col_idx] = kw
            new_row[date_col_idx] = val
            if season_col_idx is not None:
                new_row[season_col_idx] = season_map.get(kw, "")
            if item_col_idx is not None:
                new_row[item_col_idx] = item_map.get(kw, "")
            new_rows.append(new_row)
            keyword_row[kw] = len(existing) + len(new_rows) - 1  # 중복 방지

    if cells_to_update:
        ws.update_cells(cells_to_update)
    if new_rows:
        ws.append_rows(new_rows)


def read_rank_history(sheet_name: str) -> pd.DataFrame:
    """순위 이력 읽기 - 계절 | 품목 | keyword | week1 | week2 | ... 구조"""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    df = pd.DataFrame(data[1:], columns=data[0])

    # keyword 컬럼이 없으면 첫 번째 컬럼을 keyword로 (하위호환)
    if "keyword" not in df.columns:
        df.rename(columns={df.columns[0]: "keyword"}, inplace=True)

    df["keyword"] = df["keyword"].str.strip()
    # 중복 키워드 제거 (마지막 값 유지)
    df = df.drop_duplicates(subset="keyword", keep="last").reset_index(drop=True)

    str_cols = {"keyword", "계절", "품목"}
    for col in df.columns:
        if col not in str_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def save_setting(key: str, value: str):
    """'설정' 시트에 key-value 저장. 기존 키면 업데이트, 없으면 추가."""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = _get_or_create_sheet(spreadsheet, config.SHEET_NAME_SETTINGS)
    existing = ws.get_all_values()

    if not existing:
        ws.update(range_name="A1", values=[["key", "value"], [key, str(value)]])
        return

    for i, row in enumerate(existing):
        if i == 0:
            continue
        if row and row[0] == key:
            ws.update_cell(i + 1, 2, str(value))
            return

    ws.append_rows([[key, str(value)]])


def read_setting(key: str, fallback: str = "") -> str:
    """'설정' 시트에서 key의 value 읽기. 없으면 fallback 반환."""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(config.SHEET_NAME_SETTINGS)
    except gspread.WorksheetNotFound:
        return fallback

    for i, row in enumerate(ws.get_all_values()):
        if i == 0:
            continue
        if row and row[0] == key:
            return row[1] if len(row) > 1 else fallback
    return fallback


def read_rank_data() -> pd.DataFrame:
    """Google Sheets에서 광고 순위 데이터를 읽어옵니다."""
    client = _get_client()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    try:
        ws = spreadsheet.worksheet(config.SHEET_NAME_RANK)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()

    data = ws.get_all_values()
    if len(data) < 2:
        return pd.DataFrame()

    return pd.DataFrame(data[1:], columns=data[0])
