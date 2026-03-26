# -*- coding: utf-8 -*-
"""
Google Sheets 주간검색수 시트 중복 행 정리 스크립트
- 같은 키워드의 중복 행을 하나로 합침 (각 주차 컬럼 최대값 보존)
- 실행: python cleanup_sheets.py
"""
import json
import sys
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

import config

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def ok(msg):   print(f"  [OK]   {msg}")
def fail(msg): print(f"  [FAIL] {msg}")
def info(msg): print(f"  [INFO] {msg}")
def warn(msg): print(f"  [WARN] {msg}")

SEP = "=" * 60

print(f"\n{SEP}")
print("Google Sheets 주간검색수 중복 행 정리")
print(SEP)

# ── 1. 연결 ──
try:
    if config.GOOGLE_CREDENTIALS_JSON:
        creds = Credentials.from_service_account_info(
            json.loads(config.GOOGLE_CREDENTIALS_JSON), scopes=SCOPES
        )
    else:
        creds = Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_FILE, scopes=SCOPES
        )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    ws = spreadsheet.worksheet(config.SHEET_NAME_WEEKLY)
    ok("Google Sheets 연결 성공")
except Exception as e:
    fail(f"연결 실패: {e}")
    sys.exit(1)

# ── 2. 현재 시트 읽기 ──
raw = ws.get_all_values()
if len(raw) < 2:
    fail("데이터 없음")
    sys.exit(1)

header = raw[0]
info(f"헤더: {header}")
info(f"전체 행 수: {len(raw) - 1}행 (헤더 제외)")

df = pd.DataFrame(raw[1:], columns=header)
df.rename(columns={df.columns[0]: "keyword"}, inplace=True)
df["keyword"] = df["keyword"].str.strip()

# 빈 keyword 행 제거
df = df[df["keyword"] != ""]

# 고유 키워드 수 (중복 포함)
total_rows = len(df)
unique_kws = df["keyword"].nunique()
info(f"데이터 행: {total_rows}행 / 고유 키워드: {unique_kws}개")

if total_rows == unique_kws:
    ok("중복 없음 - 정리 불필요")
    sys.exit(0)

warn(f"중복 행 {total_rows - unique_kws}개 발견 -> 병합 시작")

# ── 3. 주차 컬럼 숫자 변환 후 max로 병합 ──
week_cols = [c for c in df.columns if c != "keyword"]
for col in week_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

# 각 키워드별 최대값으로 병합 (데이터 손실 방지)
df_clean = df.groupby("keyword", as_index=False, sort=False)[week_cols].max()

# 원래 키워드 순서 유지 (첫 등장 순서)
first_order = df.drop_duplicates(subset="keyword", keep="first")[["keyword"]].reset_index(drop=True)
df_clean = first_order.merge(df_clean, on="keyword", how="left")

info(f"병합 후 행 수: {len(df_clean)}행")

# ── 4. 시트 클리어 후 재작성 ──
print(f"\n  시트 초기화 및 재작성 중...")
ws.clear()

write_header = ["keyword"] + week_cols
rows = [write_header]
for _, row in df_clean.iterrows():
    r = [str(row["keyword"])]
    for col in week_cols:
        v = row[col]
        r.append(int(v) if v != 0 else "")
    rows.append(r)

ws.update(range_name="A1", values=rows)
ok(f"시트 재작성 완료: {len(df_clean)}행 (헤더 포함 {len(df_clean)+1}행)")

# ── 5. 검증 ──
print(f"\n{SEP}")
print("검증")
print(SEP)
latest_week = week_cols[-1] if week_cols else None
if latest_week:
    non_zero = (df_clean[latest_week] > 0).sum()
    zero = (df_clean[latest_week] == 0).sum()
    info(f"최근 주차({latest_week}) 데이터 있음: {non_zero}개")
    warn(f"최근 주차({latest_week}) 0 또는 빈칸:  {zero}개")
    if zero > 0:
        warn("-> fetch_weekly_data.py를 실행해서 빈칸을 채우세요")

print(SEP)
ok("정리 완료")
print(SEP)
