"""
네이버 API 모듈
- 검색광고 API: 키워드 검색수 조회
- 데이터랩 API: 검색어 트렌드 비율 조회
"""
import time
import hmac
import hashlib
import base64
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

import config


# ══════════════════════════════════════════════
# 네이버 검색광고 API
# ══════════════════════════════════════════════

def _ad_api_signature(timestamp: str, method: str, uri: str) -> str:
    """검색광고 API 인증 시그니처 생성"""
    message = f"{timestamp}.{method}.{uri}"
    sign = hmac.new(
        config.NAVER_AD_SECRET_KEY.strip().encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(sign).decode("utf-8")


def _ad_api_headers(method: str, uri: str) -> dict:
    """검색광고 API 공통 헤더"""
    timestamp = str(int(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "X-Timestamp": timestamp,
        "X-API-KEY": config.NAVER_AD_API_LICENSE,
        "X-Customer": config.NAVER_AD_CUSTOMER_ID,
        "X-Signature": _ad_api_signature(timestamp, method, uri),
    }


def fetch_search_volume(keywords: list[str], filter_exact: bool = True) -> pd.DataFrame:
    """
    키워드 목록의 월간 검색수를 조회합니다.
    filter_exact=True (기본값): 입력 키워드와 일치하는 결과만 반환
    filter_exact=False: API가 반환하는 모든 연관 키워드를 필터링 없이 반환
    반환 컬럼: keyword, monthlyPcQcCnt, monthlyMobileQcCnt, totalSearchCount
    """
    uri = "/keywordstool"
    url = config.NAVER_AD_BASE_URL + uri
    all_results = []
    total_batches = (len(keywords) + config.AD_API_BATCH_SIZE - 1) // config.AD_API_BATCH_SIZE

    for i in range(0, len(keywords), config.AD_API_BATCH_SIZE):
        batch = keywords[i : i + config.AD_API_BATCH_SIZE]
        batch_num = i // config.AD_API_BATCH_SIZE + 1
        params = {
            "hintKeywords": ",".join(batch),
            "showDetail": "1",
        }

        # 최대 3회 재시도
        for attempt in range(3):
            headers = _ad_api_headers("GET", uri)  # 매 시도마다 새 타임스탬프
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json().get("keywordList", [])
                all_results.extend(data)
                if batch_num % 10 == 0 or batch_num == total_batches:
                    print(f"  → 진행: {batch_num}/{total_batches} 배치 완료")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
                else:
                    print(f"[검색광고 API 오류] batch {batch_num} ({', '.join(batch)}): {e}")
        time.sleep(0.5)

    if not all_results:
        return pd.DataFrame(columns=["keyword", "monthlyPcQcCnt", "monthlyMobileQcCnt", "totalSearchCount"])

    df = pd.DataFrame(all_results)
    # '< 10' 같은 값을 숫자로 변환
    for col in ["monthlyPcQcCnt", "monthlyMobileQcCnt"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace("< 10", 5).infer_objects(copy=False), errors="coerce").fillna(0).astype(int)

    df["totalSearchCount"] = df["monthlyPcQcCnt"] + df["monthlyMobileQcCnt"]
    df = df.rename(columns={"relKeyword": "keyword"})
    df = df[["keyword", "monthlyPcQcCnt", "monthlyMobileQcCnt", "totalSearchCount"]].reset_index(drop=True)

    if filter_exact:
        keyword_set = {k.strip() for k in keywords}
        df = df[df["keyword"].str.strip().isin(keyword_set)].reset_index(drop=True)

    return df


def fetch_shopping_category(keywords: list[str]) -> pd.DataFrame:
    """
    네이버 쇼핑 검색 API로 키워드별 노출 카테고리를 조회합니다.
    (네이버 개발자 센터 '검색 > 쇼핑' API 사용)
    반환 컬럼: keyword, category1, category2, category3, category4
    """
    url = "https://openapi.naver.com/v1/search/shop.json"
    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }
    results = []

    for kw in keywords:
        try:
            resp = requests.get(url, headers=headers, params={"query": kw, "display": 1}, timeout=10)
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if items:
                cats = items[0].get("category1", ""), items[0].get("category2", ""), items[0].get("category3", ""), items[0].get("category4", "")
            else:
                cats = ("", "", "", "")
            results.append({"keyword": kw, "category1": cats[0], "category2": cats[1], "category3": cats[2], "category4": cats[3]})
        except Exception as e:
            print(f"[쇼핑 카테고리 오류] {kw}: {e}")
            results.append({"keyword": kw, "category1": "", "category2": "", "category3": "", "category4": ""})
        time.sleep(0.15)

    return pd.DataFrame(results)


# ══════════════════════════════════════════════
# 네이버 데이터랩 API
# ══════════════════════════════════════════════

def fetch_datalab_trend(
    keywords: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    데이터랩 통합검색어 트렌드 API로 비율 데이터를 조회합니다.
    5개씩 묶어서 호출하고, 기준 키워드(첫 번째)와의 비율로 정규화합니다.

    반환: 날짜(date) × 키워드별 비율(ratio) 피벗 DataFrame
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
        "Content-Type": "application/json",
    }

    all_trends = []

    for i in range(0, len(keywords), config.DATALAB_BATCH_SIZE):
        batch = keywords[i : i + config.DATALAB_BATCH_SIZE]
        keyword_groups = [
            {"groupName": kw, "keywords": [kw]} for kw in batch
        ]
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": "week",
            "keywordGroups": keyword_groups,
        }
        try:
            resp = requests.post(config.NAVER_DATALAB_URL, headers=headers, json=body, timeout=30)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            for r in results:
                kw_name = r["title"]
                for d in r["data"]:
                    all_trends.append({
                        "date": d["period"],
                        "keyword": kw_name,
                        "ratio": d["ratio"],
                    })
        except Exception as e:
            print(f"[데이터랩 오류] batch {i//config.DATALAB_BATCH_SIZE + 1}: {e}")
        time.sleep(config.DATALAB_DELAY_SEC)

    if not all_trends:
        return pd.DataFrame(columns=["date"])

    df = pd.DataFrame(all_trends)
    pivot = df.pivot_table(index="date", columns="keyword", values="ratio", aggfunc="first").reset_index()
    pivot["date"] = pd.to_datetime(pivot["date"])
    return pivot.sort_values("date").reset_index(drop=True)


def estimate_weekly_search_volume(
    monthly_volume: pd.DataFrame, trend_df: pd.DataFrame
) -> pd.DataFrame:
    """
    월간 실제 검색수 × 데이터랩 비율 추세를 결합하여
    주간 추정 검색수를 계산합니다.

    로직:
    1. 각 키워드의 최근 4주 평균 비율(ratio_avg) 계산
    2. 월간 검색수 / (ratio_avg × 4) = 비율 1단위당 주간 검색수
    3. 각 주의 비율 × 스케일 팩터 = 추정 주간 검색수
    """
    if trend_df.empty or monthly_volume.empty:
        return pd.DataFrame()

    keywords = [c for c in trend_df.columns if c != "date"]
    result_rows = []

    for kw in keywords:
        if kw not in trend_df.columns:
            continue

        vol_row = monthly_volume[monthly_volume["keyword"] == kw]
        if vol_row.empty:
            continue

        monthly_total = vol_row.iloc[0]["totalSearchCount"]
        recent_ratios = trend_df[kw].tail(4)
        ratio_avg = recent_ratios.mean()

        if ratio_avg == 0:
            continue

        scale = monthly_total / (ratio_avg * 4)

        for _, row in trend_df.iterrows():
            val = row[kw]
            if pd.isna(val):
                val = 0
            result_rows.append({
                "date": row["date"],
                "keyword": kw,
                "estimated_weekly_volume": 0 if pd.isna(val * scale) else round(val * scale),
                "ratio": val,
            })

    return pd.DataFrame(result_rows)


# ══════════════════════════════════════════════
# 네이버 검색 API — 블로그/카페 순위 조회
# ══════════════════════════════════════════════

def _search_api_headers() -> dict:
    """네이버 검색 API 공통 헤더"""
    return {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }


def fetch_blog_rank(keywords: list, progress_cb=None) -> pd.DataFrame:
    """
    키워드별 네이버 블로그 검색 순위 조회.
    포스트 title 또는 description에 '오즈키즈'가 포함된 첫 번째 결과의 순위 반환.
    없으면 0. 반환 컬럼: keyword, rank
    """
    if not config.NAVER_CLIENT_ID or not config.NAVER_CLIENT_SECRET:
        return pd.DataFrame(columns=["keyword", "rank"])

    url = "https://openapi.naver.com/v1/search/blog.json"
    headers = _search_api_headers()
    results = []

    for idx, kw in enumerate(keywords):
        if progress_cb:
            progress_cb(idx, len(keywords), kw)
        rank = 0
        try:
            resp = requests.get(
                url,
                headers=headers,
                params={"query": kw, "display": 100, "start": 1, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code == 200:
                for i, item in enumerate(resp.json().get("items", []), start=1):
                    title = item.get("title", "")
                    desc  = item.get("description", "")
                    if "오즈키즈" in title or "오즈키즈" in desc:
                        rank = i
                        break
        except Exception:
            rank = 0
        results.append({"keyword": kw, "rank": rank})
        time.sleep(0.2)

    return pd.DataFrame(results)


def fetch_cafe_rank(keywords: list, progress_cb=None) -> pd.DataFrame:
    """
    키워드별 네이버 카페 검색 순위 조회.
    오즈키즈 관련 게시글(title/description에 '오즈키즈' 또는 'ozkiz' 포함)이 몇 번째인지 반환.
    없으면 0. 반환 컬럼: keyword, rank
    """
    if not config.NAVER_CLIENT_ID or not config.NAVER_CLIENT_SECRET:
        return pd.DataFrame(columns=["keyword", "rank"])

    url = "https://openapi.naver.com/v1/search/cafearticle.json"
    headers = _search_api_headers()
    results = []

    for idx, kw in enumerate(keywords):
        if progress_cb:
            progress_cb(idx, len(keywords), kw)
        rank = 0
        try:
            resp = requests.get(
                url,
                headers=headers,
                params={"query": kw, "display": 100, "start": 1, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code == 200:
                for i, item in enumerate(resp.json().get("items", []), start=1):
                    title = item.get("title", "").lower()
                    desc  = item.get("description", "").lower()
                    if "오즈키즈" in title or "ozkiz" in title or \
                       "오즈키즈" in desc  or "ozkiz" in desc:
                        rank = i
                        break
        except Exception:
            rank = 0
        results.append({"keyword": kw, "rank": rank})
        time.sleep(0.2)

    return pd.DataFrame(results)
