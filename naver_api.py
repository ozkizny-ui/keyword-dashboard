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
        config.NAVER_AD_SECRET_KEY.encode("utf-8"),
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


def fetch_search_volume(keywords: list[str], filter_exact: bool = True, progress_cb=None) -> pd.DataFrame:
    """
    키워드 목록의 월간 검색수를 조회합니다.
    filter_exact=True (기본값): 입력 키워드와 일치하는 결과만 반환
    filter_exact=False: API가 반환하는 모든 연관 키워드를 필터링 없이 반환
    progress_cb: 진행상황 콜백 함수 (batch_num, total_batches, status_text) → Streamlit 등에서 활용
    반환 컬럼: keyword, monthlyPcQcCnt, monthlyMobileQcCnt, totalSearchCount
    """
    uri = "/keywordstool"
    url = config.NAVER_AD_BASE_URL + uri
    all_results = []
    errors = []
    total_batches = (len(keywords) + config.AD_API_BATCH_SIZE - 1) // config.AD_API_BATCH_SIZE

    for i in range(0, len(keywords), config.AD_API_BATCH_SIZE):
        batch = [kw.strip() for kw in keywords[i : i + config.AD_API_BATCH_SIZE] if kw.strip()]
        if not batch:
            continue
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
                if resp.status_code != 200:
                    err_msg = f"batch {batch_num}: HTTP {resp.status_code} - {resp.text[:200]}"
                    if attempt < 2:
                        time.sleep(1)
                        continue
                    else:
                        errors.append(err_msg)
                        print(f"[검색광고 API 오류] {err_msg}")
                        break
                data = resp.json().get("keywordList", [])
                all_results.extend(data)
                if progress_cb:
                    progress_cb(batch_num, total_batches, f"배치 {batch_num}/{total_batches} 완료")
                elif batch_num % 10 == 0 or batch_num == total_batches:
                    print(f"  → 진행: {batch_num}/{total_batches} 배치 완료")
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(1)
                else:
                    err_msg = f"batch {batch_num} ({', '.join(batch)}): {e}"
                    errors.append(err_msg)
                    print(f"[검색광고 API 오류] {err_msg}")
        time.sleep(0.5)

    # 에러가 있으면 경고 로그 (호출자가 확인 가능)
    if errors:
        print(f"[검색광고 API] {len(errors)}개 배치 실패: {errors[:3]}")  # 처음 3개만 출력

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
    print(f"[DEBUG] CLIENT_ID 앞4자리: {str(config.NAVER_CLIENT_ID)[:4]!r}")
    print(f"[DEBUG] CLIENT_SECRET 앞4자리: {str(config.NAVER_CLIENT_SECRET)[:4]!r}")
    print(f"[DEBUG] CLIENT_ID 길이: {len(str(config.NAVER_CLIENT_ID))}")
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
            print(f"[DEBUG] batch {i}: status={resp.status_code}, body={resp.text[:300]}")
            resp.raise_for_status()
            results = resp.json().get("results", [])
            print(f"[DEBUG] batch {i}: list길이={len(results)}")
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


# ══════════════════════════════════════════════
# 네이버 검색 API 기반 연관 키워드 추천
# ══════════════════════════════════════════════

# 불용어: 검색 결과에 자주 나오지만 키워드로 의미 없는 단어
_STOPWORDS = {
    # 쇼핑/커머스 관련
    "추천", "후기", "리뷰", "구매", "가격", "비교", "순위", "인기", "최저가",
    "할인", "무료", "배송", "당일", "사용", "방법", "정보", "상품", "제품",
    "주문", "결제", "선택", "옵션", "사이즈", "컬러", "색상",
    # 블로그/SNS 관련
    "네이버", "블로그", "카페", "포스팅", "공유", "이벤트", "소개", "안내",
    "사진", "영상", "동영상", "이미지", "링크", "클릭", "더보기", "자세히",
    "좋아요", "댓글", "공감", "구독", "팔로우", "스토어", "스마트", "브랜드",
    # 일반 감탄/수식어
    "최고", "최신", "진짜", "완전", "정말", "너무", "매우", "아주", "엄청",
    "대박", "강추", "꿀팁", "솔직", "직접",
    "ㅋㅋ", "ㅎㅎ", "하하", "다른", "이런", "그런", "어떤", "모든",
    # 시간/일반 범용어
    "오늘", "내일", "올해", "작년", "이번", "지난", "다음", "요즘", "최근",
    "준비", "필수", "필요", "가능", "확인", "시간", "장소", "날짜", "기간",
    "한번", "처음", "마지막", "정도", "그냥", "바로", "함께", "같이",
    "하나", "가지", "이상", "이하", "이내", "부분", "종류",
    # 육아 블로그 고빈도 범용어 (검색 키워드로 무의미)
    "아기", "아이", "아이들", "엄마", "아빠", "맘", "육아", "우리", "아들", "딸",
    "우리집", "아이랑", "아기랑", "엄마랑",
    # 캐릭터/엔터
    "뽀로로", "핑크퐁", "코코몽", "타요", "캐릭터",
    "메이플", "메이플랜드", "게임", "드라마", "예능",
    # 기타 고빈도 노이즈 (범용 2글자)
    "방통대", "택배", "그림", "부산", "서울", "경기", "인천", "대구", "광주",
    "대전", "울산", "제주", "강남", "홍대",
    "학교", "학원", "수업", "공부", "시험", "과제", "교육",
}


def suggest_related_keywords(
    seed_keyword: str,
    max_results: int = 30,
) -> list[dict]:
    """
    네이버 검색 API를 활용하여 제품의 '구매 맥락' 키워드를 추천합니다.

    목표: "이 제품을 사는 사람이 검색할 키워드" 찾기
    예) "유아목장갑" → 갯벌체험, 고구마캐기, 글램핑, 어린이목장갑 등

    2단계 접근:
    1단계 - 맥락 추출: 블로그/카페 글에서 제품 사용 상황 단어 수집
    2단계 - 키워드 확장: 맥락 단어 + 시드를 검색광고 API hintKeywords로 전달
            → 네이버가 판단하는 연관 키워드와 검색수를 함께 반환

    반환: [{"keyword": str, "월간검색수": int, "출현빈도": int}, ...]
    """
    import re
    from collections import Counter

    if not config.NAVER_CLIENT_ID or not config.NAVER_CLIENT_SECRET:
        print("[연관키워드] 네이버 검색 API 키가 설정되지 않았습니다.")
        return {"results": [], "context_words": []}

    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }

    seed_clean = seed_keyword.replace(" ", "")
    seed_words = set(re.findall(r'[가-힣]{2,}', seed_clean))

    all_texts = []

    # ── 블로그 검색 (제목 + 설명 수집) ──
    for start in [1, 51]:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/blog.json",
                headers=headers,
                params={"query": seed_keyword, "display": 50, "start": start, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    title = re.sub(r'<[^>]+>', '', item.get("title", ""))
                    desc = re.sub(r'<[^>]+>', '', item.get("description", ""))
                    all_texts.append(title)
                    all_texts.append(desc)
        except Exception as e:
            print(f"[블로그 검색 오류] {e}")
        time.sleep(0.2)

    # ── 카페 검색 (제목 + 설명 수집) ──
    for start in [1, 51]:
        try:
            resp = requests.get(
                "https://openapi.naver.com/v1/search/cafearticle.json",
                headers=headers,
                params={"query": seed_keyword, "display": 50, "start": start, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    title = re.sub(r'<[^>]+>', '', item.get("title", ""))
                    desc = re.sub(r'<[^>]+>', '', item.get("description", ""))
                    all_texts.append(title)
                    all_texts.append(desc)
        except Exception as e:
            print(f"[카페 검색 오류] {e}")
        time.sleep(0.2)

    # ── 쇼핑 검색 (상품명 수집) ──
    try:
        resp = requests.get(
            "https://openapi.naver.com/v1/search/shop.json",
            headers=headers,
            params={"query": seed_keyword, "display": 50, "sort": "sim"},
            timeout=10,
        )
        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                title = re.sub(r'<[^>]+>', '', item.get("title", ""))
                all_texts.append(title)
    except Exception as e:
        print(f"[쇼핑 검색 오류] {e}")
    time.sleep(0.2)

    if not all_texts:
        return {"results": [], "context_words": []}

    # ════════════════════════════════════════
    # 1단계: 텍스트에서 맥락 단어 추출
    # ════════════════════════════════════════
    candidate_counter = Counter()

    for text in all_texts:
        words = re.findall(r'[가-힣]{2,8}', text)

        for w in words:
            candidate_counter[w] += 1

        for i in range(len(words) - 1):
            compound = words[i] + words[i + 1]
            if 4 <= len(compound) <= 12:
                candidate_counter[compound] += 1

        for i in range(len(words) - 2):
            compound3 = words[i] + words[i + 1] + words[i + 2]
            if 6 <= len(compound3) <= 14:
                candidate_counter[compound3] += 1

    # 맥락 단어 필터링
    context_words = {}
    for candidate, count in candidate_counter.items():
        if count < 3:
            continue
        if candidate in _STOPWORDS:
            continue
        if len(candidate) < 2:
            continue
        if candidate == seed_clean or candidate in seed_words:
            continue
        context_words[candidate] = count

    if not context_words:
        return {"results": [], "context_words": []}

    # 빈도 기준 상위 맥락 단어 선정
    top_context = sorted(context_words.keys(),
                         key=lambda k: context_words[k], reverse=True)[:50]

    # ════════════════════════════════════════
    # 2단계: 맥락 단어를 hintKeywords로 검색광고 API에 전달
    #        시드 키워드와 맥락 단어를 조합하여 연관 키워드 확장
    # ════════════════════════════════════════

    # 방법 A: 시드 + 맥락 단어를 함께 hintKeywords로 전달
    #   → 네이버 API가 이 조합에서 연관 키워드를 반환
    # 방법 B: 맥락 단어 단독으로도 hintKeywords 전달
    #   → "고구마" → "고구마캐기", "고구마캐기체험" 등 확장

    all_hints = []

    # 시드 + 맥락 조합 (5개씩 배치, 시드 항상 포함)
    for i in range(0, len(top_context), 4):
        batch = [seed_clean] + top_context[i:i + 4]
        all_hints.append(batch)

    # 맥락 단어끼리만 조합 (시드 없이, 사용 상황 키워드 확장)
    for i in range(0, min(len(top_context), 25), 5):
        batch = top_context[i:i + 5]
        if batch:
            all_hints.append(batch)

    # 각 배치를 검색광고 API에 전달
    all_results = []
    seen_keywords = set()

    uri = "/keywordstool"
    url = config.NAVER_AD_BASE_URL + uri

    for hint_batch in all_hints:
        params = {
            "hintKeywords": ",".join(hint_batch),
            "showDetail": "1",
        }
        for attempt in range(2):
            ad_headers = _ad_api_headers("GET", uri)
            try:
                resp = requests.get(url, headers=ad_headers, params=params, timeout=30)
                if resp.status_code == 200:
                    data = resp.json().get("keywordList", [])
                    all_results.extend(data)
                    break
            except Exception:
                if attempt < 1:
                    time.sleep(1)
        time.sleep(0.5)

    if not all_results:
        # API 실패 시 빈도 기준으로 반환
        debug_context = [(kw, context_words[kw]) for kw in top_context]
        return {"results": [
            {"keyword": kw, "월간검색수": 0, "출현빈도": context_words[kw]}
            for kw in top_context[:max_results]
        ], "context_words": debug_context}

    # 결과 DataFrame 생성
    df = pd.DataFrame(all_results)
    if "relKeyword" not in df.columns:
        debug_context = [(kw, context_words[kw]) for kw in top_context]
        return {"results": [], "context_words": debug_context}

    for col in ["monthlyPcQcCnt", "monthlyMobileQcCnt"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].replace("< 10", 5).infer_objects(copy=False),
                errors="coerce"
            ).fillna(0).astype(int)

    df["totalSearchCount"] = df["monthlyPcQcCnt"] + df["monthlyMobileQcCnt"]
    df = df.rename(columns={"relKeyword": "keyword"})

    # ════════════════════════════════════════
    # 3단계: 관련성 필터링
    # ════════════════════════════════════════

    # 관련성 기준: 맥락 단어(3글자+) 또는 시드 단어가 포함된 키워드만
    filter_words = [c for c in top_context if len(c) >= 3]
    filter_words.extend(list(seed_words))
    filter_words.append(seed_clean)

    def is_relevant(kw):
        return any(fw in kw for fw in filter_words)

    df = df[df["keyword"].apply(is_relevant)].copy()

    # 불용어 제거
    df = df[~df["keyword"].isin(_STOPWORDS)]
    # 시드 키워드 자체 제거
    df = df[df["keyword"] != seed_clean]
    # 중복 제거
    df = df.groupby("keyword", as_index=False)["totalSearchCount"].max()

    # ════════════════════════════════════════
    # 4단계: 맥락 단어 강제 포함 + 검색수 병합
    # ════════════════════════════════════════

    # API 결과에서 키워드→검색수 매핑
    vol_map = dict(zip(df["keyword"], df["totalSearchCount"]))

    # 맥락 단어 중 검색수가 확인된 것 + API 결과에 없지만 맥락 단어인 것 모두 포함
    # 맥락 단어 자체도 검색수를 조회해볼 가치가 있음 (이미 hintKeywords로 보냈으므로 vol_map에 있을 수 있음)
    all_keywords = {}

    # API 연관 키워드 결과
    for kw, vol in vol_map.items():
        freq = context_words.get(kw, 0)
        all_keywords[kw] = {"월간검색수": int(vol), "출현빈도": freq}

    # 맥락 단어 강제 포함 (API 결과에 없어도)
    for ctx_kw in top_context:
        if ctx_kw in _STOPWORDS or ctx_kw == seed_clean or ctx_kw in seed_words:
            continue
        if ctx_kw not in all_keywords:
            # API 결과에 없는 맥락 단어 — 검색수 0이지만 출현빈도로 가치 판단
            all_keywords[ctx_kw] = {"월간검색수": vol_map.get(ctx_kw, 0), "출현빈도": context_words.get(ctx_kw, 0)}

    # 결과 정렬: 출현빈도 × 가중치 + 검색수 기준 복합 정렬
    result_list = []
    for kw, info in all_keywords.items():
        result_list.append({
            "keyword": kw,
            "월간검색수": info["월간검색수"],
            "출현빈도": info["출현빈도"],
        })

    # 출현빈도 우선, 같으면 검색수 순
    result_list.sort(key=lambda x: (x["출현빈도"], x["월간검색수"]), reverse=True)
    result = result_list[:max_results]

    # 디버그 정보: 맥락 단어 상위 50개와 빈도
    debug_context = [(kw, context_words[kw]) for kw in top_context]

    return {"results": result, "context_words": debug_context}
