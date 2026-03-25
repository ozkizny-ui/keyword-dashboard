"""
주간 데이터 수집 스크립트
매주 월요일 자동 실행 (GitHub Actions 또는 Windows 작업 스케줄러)

실행: python fetch_weekly_data.py
"""
import sys
import pandas as pd
from datetime import datetime, timedelta

import config
from naver_api import fetch_search_volume, fetch_datalab_trend, estimate_weekly_search_volume
from google_sheets import append_weekly_data, save_trend_data


def get_week_label(dt: datetime = None) -> str:
    """주차 라벨 생성 - 해당 주 월~일 날짜 범위 (예: 2026.03.23-2026.03.29)"""
    dt = dt or datetime.now()
    monday = dt - timedelta(days=dt.weekday())
    sunday = monday + timedelta(days=6)
    return f"{monday.strftime('%Y.%m.%d')}-{sunday.strftime('%Y.%m.%d')}"


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


def main():
    print(f"{'='*50}")
    print(f"  키워드 검색수 주간 수집 시작")
    print(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

    # ── 3. 데이터랩 트렌드 조회 ──
    print(f"\n[3/3] 네이버 데이터랩 API - 연간 트렌드 조회...")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

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
