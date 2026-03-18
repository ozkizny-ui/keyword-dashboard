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
    """Google Sheets 클라이언트 생성"""
    # 환경변수에 JSON 문자열로 저장된 경우 (GitHub Actions용)
    creds_json = __import__("os").getenv("GOOGLE_CREDENTIALS_JSON")
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
            rows.append([row["keyword"], row["totalSearchCount"]])
        ws.update(range_name="A1", values=rows)
        return

    header = existing[0]
    keyword_map = {r[0]: i for i, r in enumerate(existing) if i > 0}

    if week_label in header:
        col_idx = header.index(week_label)
    else:
        col_idx = len(header)
        header.append(week_label)
        ws.update_cell(1, col_idx + 1, week_label)

    cells_to_update = []
    new_rows = []

    for _, row in df.iterrows():
        kw = row["keyword"]
        val = int(row["totalSearchCount"])
        if kw in keyword_map:
            row_idx = keyword_map[kw] + 1  # 1-based
            cells_to_update.append(gspread.Cell(row_idx, col_idx + 1, val))
        else:
            new_row = [kw] + [""] * (col_idx - 1) + [val]
            new_rows.append(new_row)

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

    df = pd.DataFrame(data[1:], columns=data[0])
    # 첫 번째 컬럼을 "keyword"로 정규화 (Sheets에서 헤더가 다를 수 있음)
    df.rename(columns={df.columns[0]: "keyword"}, inplace=True)
    # 숫자 컬럼 변환
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
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
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in df.columns:
        if col != "date":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


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
