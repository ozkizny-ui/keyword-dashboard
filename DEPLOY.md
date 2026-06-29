# 키워드 대시보드 — 정적 배포 가이드 (Streamlit 탈출)

Streamlit Community Cloud는 트래픽이 없으면 앱이 **잠들어** 콜드스타트가 생깁니다.
이 가이드는 **마케팅 대시보드와 동일한 무서버 구조**(GitHub Pages + Apps Script + Google Sheets)로
옮겨서 **절대 잠들지 않게** 만드는 방법입니다.

```
GitHub Pages (정적 index.html, 안 잠)
   │ GET  ?action=...        (읽기)
   │ POST ?action=...        (쓰기)
   ▼
Google Apps Script 웹앱 (서버리스, 안 잠)  ← Code.gs
   ▼
Google Sheets (1uD-2g…)  ← 기존 그대로
   ▲
GitHub Actions weekly.yml (매주 월 06:00 KST)  ← 주간 수집 그대로 유지
```

데이터 **수집**(네이버 API 대량 호출)은 이미 GitHub Actions로 돌고 있으니 그대로 둡니다.
대시보드는 시트를 **읽어서 보여주기만** 하므로 잠들 이유가 없습니다.

---

## 1) Apps Script 백엔드 배포

1. https://script.google.com → **새 프로젝트**.
2. 기본 `Code.gs` 내용을 지우고, 이 레포의 [`Code.gs`](Code.gs) 전체를 붙여넣기.
3. 상단 `CONFIG` 값 채우기 (값은 **GitHub Actions Secrets / 기존 `.env`** 와 동일):
   - `NAVER_AD_API_LICENSE`, `NAVER_AD_SECRET_KEY`, `NAVER_AD_CUSTOMER_ID`
   - `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`
   - `SPREADSHEET_ID` 은 이미 입력돼 있음.
   > 읽기 전용으로만 먼저 띄우려면 네이버 키는 비워둬도 됩니다(라이브 기능만 비활성).
4. **스프레드시트 권한**: 이 스크립트를 만든 구글 계정이 해당 시트(`1uD-2g…`)에
   편집 권한이 있어야 합니다. (기존 서비스계정과 별개 — 본인 계정으로 접근)
5. **배포 → 새 배포 → 유형: 웹 앱**
   - 실행: **나(본인)**
   - 액세스 권한: **모든 사용자**
   - 배포 → 권한 승인(첫 1회) → **웹 앱 URL**(…/exec) 복사.
6. 동작 확인: 브라우저에서 `…/exec?action=ping` → `{"ok":true,...}` 가 보이면 성공.
   `…/exec?action=keyword_dict` → 시트 데이터 JSON 확인.

> 백엔드를 고치면 **반드시 "배포 관리 → 편집 → 새 버전"** 으로 재배포해야 반영됩니다.

## 2) 프론트엔드 연결

[`docs/index.html`](docs/index.html) 상단 `CONFIG.API_URL` 에 1)에서 복사한 `/exec` URL 입력:

```js
const CONFIG = { API_URL: "https://script.google.com/macros/s/AKfy..../exec" };
```

비워두면 내장 **목업 데이터**로 UI만 미리 볼 수 있습니다.

## 3) GitHub Pages 켜기

이 레포(`keyword-dashboard`)에서:
1. `docs/index.html` 커밋 & 푸시.
2. GitHub → **Settings → Pages** → Source: **Deploy from a branch** →
   Branch: `main`, 폴더: **/docs** → Save.
3. 1~2분 후 `https://<계정>.github.io/keyword-dashboard/` 로 접속.
   (= ad-studio처럼 **푸시가 곧 배포**)

> 비공개 레포의 GitHub Pages는 Pro 플랜이 필요합니다. 무료 계정이면 ad-studio가 올라간
> `ozkizny-ui.github.io` 처럼 공개 레포/계정으로 두세요.

## 4) Streamlit 정리 (선택)

새 대시보드가 잘 뜨면 Streamlit Cloud 앱은 **삭제하거나 비공개** 처리.
`fetch_weekly_data.py` 와 `.github/workflows/weekly.yml`(주간 수집)은 **그대로 유지**합니다.

---

## 동작 범위 (현재 / 다음 단계)

| 페이지 | 상태 | 백엔드 |
|---|---|---|
| 📈 주간 검색수 | ✅ 완료 (차트·테이블·필터·기간) | `?action=keyword_dict` |
| 📊 연간 트렌드 | 🔜 2단계 | `?action=trend` (+수집은 Actions) |
| 🛒 쇼핑검색 순위 | 🔜 2단계 | `?action=rank&type=shopping` |
| 🔗 파워링크 순위 | 🔜 2단계 | `?action=rank&type=powerlink` |
| 📝 블로그/카페 순위 | 🔜 2단계 | `?action=rank&type=blog|cafe` |
| 🆕 신규키워드 개발 | 🔜 2단계 | `?action=related_kw / search_volume` (라이브) |
| ⚙️ 데이터 관리 | 🔜 2단계 | `?action=settings` |

## CORS / 호출 규약 (개발 참고)

- **읽기는 GET** (`fetch(url?action=...)`). Apps Script `/exec` 는 GET 교차출처 허용.
- **쓰기는 POST + `Content-Type: text/plain`** 으로 보냅니다. (application/json 으로 보내면
  프리플라이트가 발생해 Apps Script가 막음 → 반드시 text/plain 으로 JSON 문자열 전송)
- 대량/장시간 작업(블로그·카페 전체 순위, 2년치 데이터랩)은 Apps Script 6분 제한에 걸릴 수
  있어 **GitHub Actions(파이썬)** 에 두는 것을 권장. 대시보드의 라이브 버튼은 소량(신규키워드
  1개 시드, 조합 키워드 수십 개)만 호출.
