"""
주간 데이터 수집 스크립트
매주 월요일 자동 실행 (GitHub Actions 또는 Windows 작업 스케줄러)

실행: python fetch_weekly_data.py
"""
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone

import config
from naver_api import fetch_search_volume, fetch_datalab_trend, estimate_weekly_search_volume
from google_sheets import append_weekly_data, save_trend_data, read_weekly_data

KST = timezone(timedelta(hours=9))

MAX_RETRIES = 2  # 빈칸 키워드 재시도 횟수


def get_week_label(dt: datetime = None) -> str:
    """주차 라벨 생성 - 금~목 날짜 범위 (예: 2026.03.21-2026.03.27)"""
    dt = dt or datetime.now(KST)
    # 가장 최근 금요일 기준 (weekday: 0=월 … 4=금). 오늘이 금요일이면 오늘이 시작일.
    days_since_friday = (dt.weekday() - 4) % 7
    friday = dt - timedelta(days=days_since_friday)
    thursday = friday + timedelta(days=6)
    return f"{friday.strftime('%Y.%m.%d')}-{thursday.strftime('%Y.%m.%d')}"


def load_keywords() -> list[str]:
    """키워드 목록 로드"""
    try:
        df = pd.read_excel(config.KEYWORDS_FILE)
        col = df.columns[0]
        keywords = df[col].dropna().astype(str).str.strip().tolist()
        print(f"[INFO] {len(keywords)}개 키워드 로드 완료")
        return keywords
    except FileNotFoundError:
        print(f"[ERROR] {config.KEYWORDS_FILE} 파일을 찾을 수 없습니다.")
        sys.exit(1)


def find_blank_keywords(all_keywords: list[str], week_label: str) -> list[str]:
    """Google Sheets에서 현재 주차 컬럼이 0이거나 누락된 키워드를 반환."""
    df = read_weekly_data()
    if df.empty or week_label not in df.columns:
        return list(all_keywords)

    in_sheets = set(df["keyword"].tolist())

    # 시트에는 있지만 해당 주차 검색수가 0인 키워드
    blank = df[df[week_label].fillna(0) == 0]["keyword"].tolist()

    # 시트에 아예 없는 키워드
    missing = [kw for kw in all_keywords if kw not in in_sheets]

    return list(dict.fromkeys(blank + missing))  # 순서 유지 + 중복 제거


def main():
    print(f"{'='*50}")
    print(f"  키워드 검색수 주간 수집 시작")
    print(f"  실행 시각: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} KST")
    print(f"{'='*50}")

    week_label = get_week_label()
    keywords = load_keywords()

    # ── 1. 월간 검색수 조회 ──
    print(f"\n[1/3] 네이버 검색광고 API - 월간 검색수 조회...")
    volume_df = fetch_search_volume(keywords)
    print(f"  → {len(volume_df)}개 키워드 검색수 조회 완료")

    if volume_df.empty:
        print("[ERROR] 검색수 데이터를 가져오지 못했습니다. API 키를 확인하세요.")
        sys.exit(1)

    # ── 2. Google Sheets에 주간 데이터 저장 ──
    print(f"\n[2/3] Google Sheets에 주간 데이터 저장 ({week_label})...")
    append_weekly_data(volume_df, week_label)
    print(f"  → 저장 완료")

    # ── 2-1. 빈칸 키워드 재시도 (최대 {MAX_RETRIES}회) ──
    for retry in range(1, MAX_RETRIES + 1):
        blank_kws = find_blank_keywords(keywords, week_label)
        if not blank_kws:
            print(f"  → 빈칸 키워드 없음. 재시도 불필요.")
            break

        print(f"\n  [재시도 {retry}/{MAX_RETRIES}] 빈칸 키워드 {len(blank_kws)}개 재수집:")
        for kw in blank_kws:
            print(f"    - {kw}")

        retry_df = fetch_search_volume(blank_kws)
        if not retry_df.empty:
            append_weekly_data(retry_df, week_label)
            print(f"  → 재시도 저장 완료 ({len(retry_df)}개)")
        else:
            print(f"  → 재시도 결과 없음")
            break
    else:
        # 최대 재시도 후에도 남은 빈칸 확인
        remaining = find_blank_keywords(keywords, week_label)
        if remaining:
            print(f"\n  ⚠️ 재시도 {MAX_RETRIES}회 후에도 빈칸 키워드 {len(remaining)}개 남음 (검색수 없는 키워드로 간주):")
            for kw in remaining:
                print(f"    - {kw}")

    # ── 3. 데이터랩 트렌드 조회 ──
    print(f"\n[3/3] 네이버 데이터랩 API - 연간 트렌드 조회...")
    end_date = datetime.now(KST).strftime("%Y-%m-%d")
    start_date = (datetime.now(KST) - timedelta(days=365)).strftime("%Y-%m-%d")

    trend_df = fetch_datalab_trend(keywords, start_date, end_date)
    print(f"  → 트렌드 데이터 수집 완료 ({len(trend_df)} rows)")

    if not trend_df.empty:
        # 추정 주간 검색수 계산
        estimated = estimate_weekly_search_volume(volume_df, trend_df)
        if not estimated.empty:
            save_trend_data(estimated)
            print(f"  → 추정 주간 검색수 Google Sheets 저장 완료")

    print(f"\n{'='*50}")
    print(f"  수집 완료! 주차: {week_label}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
