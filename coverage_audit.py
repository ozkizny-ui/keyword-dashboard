# -*- coding: utf-8 -*-
"""
커버 감사 (Coverage Audit)
────────────────────────────────────────────────────────────────
목적: "대표키워드를 시드로 네이버 검색광고 API에 던지면, 그 그룹의
      멤버 키워드들이 실제로 연관결과에 나오는가?"를 전수 측정한다.

배경: 「키워드사전」을 '대표키워드 컬럼 하나'로 단일원천화하고, 이 대표키워드
      고유값을 API 시드로 쓰는 설계로 전환하기 전에, 커버 갭(대표키워드로
      안 잡히는 멤버가 몇 개인지)을 먼저 재는 선행 감사.
      → 갭이 작으면 그대로 전환, 크면 해당 키워드의 대표키워드를 재지정하거나
        자기 자신을 대표키워드(1개짜리 그룹)로 승격.

실행 환경: 로컬은 시계 skew로 네이버 API가 403(Invalid Timestamp)을 냄.
      → GitHub Actions(coverage_audit.yml, 수동 dispatch)에서 실행하거나,
        시각이 정확하고 NAVER_AD_* 시크릿이 설정된 환경에서 실행.

사용:
      python coverage_audit.py                 # 전체 대표키워드 감사
      python coverage_audit.py --limit 20       # 앞 20개만 (스모크 테스트)
      python coverage_audit.py --out gaps.csv   # 미커버 목록 저장 파일명

읽기 소스: 「키워드사전」은 공개 Apps Script API(/exec)로 읽으므로 구글 인증 불필요.
          네이버 검색광고 API 키(NAVER_AD_*)만 있으면 된다.
"""
import argparse
import json
import sys
import time
import urllib.request
from collections import defaultdict

import config
from naver_api import fetch_search_volume

# index.html CONFIG.API_URL 과 동일 (공개 웹앱)
DICT_API_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbwxeD3Ofxr1r5Aq6ZZUmt64B48lVSeq757jh5r0wWJRf1T1tycMU6NN50p7odBgqw_xhw/exec"
)

META_COLS = {"계절", "복종", "연령", "성별", "카테고리", "대표키워드", "키워드"}


def norm(s: str) -> str:
    """비교용 정규화: 공백 제거 (네이버 relKeyword는 공백 없이 오는 경우가 많음)."""
    return "".join(str(s or "").split())


def load_keyword_dict() -> dict:
    """공개 API로 키워드사전을 읽어 {header, rows} 반환."""
    req = urllib.request.Request(
        DICT_API_URL + "?action=keyword_dict",
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)


def build_groups(kd: dict):
    """대표키워드 -> [멤버 키워드], 그리고 멤버 -> 최신 검색량(정렬/리포트용)."""
    H = kd.get("header", [])
    rows = kd.get("rows", [])
    ir = H.index("대표키워드") if "대표키워드" in H else -1
    ik = H.index("키워드") if "키워드" in H else 0
    # 최신 주차 컬럼 = 메타가 아닌 마지막 컬럼
    week_idx = [i for i, c in enumerate(H) if c not in META_COLS and i != ik]
    latest = week_idx[-1] if week_idx else -1

    groups = defaultdict(list)
    volume = {}
    seen = set()
    for r in rows:
        kw = str(r[ik]).strip() if len(r) > ik and r[ik] else ""
        if not kw or kw in seen:
            continue
        seen.add(kw)
        rep = str(r[ir]).strip() if ir >= 0 and len(r) > ir and r[ir] else ""
        groups[rep].append(kw)
        if latest >= 0 and len(r) > latest:
            try:
                volume[kw] = int(float(str(r[latest]).replace(",", "").strip() or 0))
            except ValueError:
                volume[kw] = 0
    return groups, volume


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="앞 N개 대표키워드만 감사 (0=전체)")
    ap.add_argument("--out", default="coverage_gaps.csv", help="미커버 목록 CSV 파일명")
    ap.add_argument("--sleep", type=float, default=0.3, help="시드 조회 간 딜레이(초)")
    args = ap.parse_args()

    if not config.NAVER_AD_API_LICENSE or not config.NAVER_AD_SECRET_KEY:
        print("[ERROR] NAVER_AD_* 키가 없습니다. 시크릿이 설정된 환경에서 실행하세요.")
        sys.exit(1)

    print("[1/3] 키워드사전 로드 중…")
    kd = load_keyword_dict()
    groups, volume = build_groups(kd)
    reps = [r for r in groups.keys() if r]  # 대표키워드 빈칸 제외
    blank_members = groups.get("", [])
    total_members = sum(len(groups[r]) for r in reps)
    print(f"  → 대표키워드 {len(reps)}개, 멤버 키워드 {total_members}개"
          f"{f', 대표키워드 없는 키워드 {len(blank_members)}개' if blank_members else ''}")

    if args.limit:
        reps = reps[: args.limit]
        print(f"  → --limit {args.limit}: 앞 {len(reps)}개 대표키워드만 감사")

    print(f"[2/3] 대표키워드별 시드 조회 + 커버 검증 ({len(reps)}개)…")
    covered, uncovered = [], []
    rep_no_self = []  # 대표키워드 자신조차 결과에 안 나온 경우(시드 부적합 강한 신호)
    for n, rep in enumerate(reps, 1):
        try:
            df = fetch_search_volume([rep], filter_exact=False)
            returned = {norm(k) for k in df["keyword"].astype(str)}
        except Exception as e:  # noqa: BLE001
            print(f"  [WARN] '{rep}' 조회 실패: {e}")
            returned = set()
        if norm(rep) not in returned:
            rep_no_self.append(rep)
        for kw in groups[rep]:
            if norm(kw) in returned or norm(kw) == norm(rep):
                covered.append(kw)
            else:
                uncovered.append((kw, rep, volume.get(kw, 0)))
        if n % 20 == 0 or n == len(reps):
            print(f"  → {n}/{len(reps)} 대표키워드 완료 (미커버 누적 {len(uncovered)})")
        time.sleep(args.sleep)

    checked = len(covered) + len(uncovered)
    print("[3/3] 결과")
    print(f"  검사한 멤버 키워드: {checked}개")
    print(f"  ✅ 커버됨:   {len(covered)}개 ({len(covered)/checked*100:.1f}%)" if checked else "  (검사 대상 없음)")
    print(f"  ❌ 미커버:   {len(uncovered)}개 ({len(uncovered)/checked*100:.1f}%)" if checked else "")
    print(f"  ⚠️ 대표키워드 자신도 미출현(시드 부적합 의심): {len(rep_no_self)}개")

    # 미커버 목록 저장 (검색량 큰 순 = 전환 시 우선 처리 대상)
    uncovered.sort(key=lambda x: -x[2])
    import csv
    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["키워드", "대표키워드", "최신월검색수", "대표키워드자신미출현"])
        no_self = set(rep_no_self)
        for kw, rep, vol in uncovered:
            w.writerow([kw, rep, vol, "Y" if rep in no_self else ""])
    print(f"\n  → 미커버 목록 저장: {args.out} (검색량 큰 순)")
    if uncovered:
        print("  상위 미커버 예시:")
        for kw, rep, vol in uncovered[:10]:
            print(f"    · {kw}  (대표:{rep}, 월{vol:,})")


if __name__ == "__main__":
    main()
