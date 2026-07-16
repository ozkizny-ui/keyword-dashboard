/**
 * 오즈키즈 키워드 대시보드 — 웹앱 백엔드 (보여주기 담당)
 * ────────────────────────────────────────────────────────────────
 * ★ 기존 "키워드 대시보드 자동화"(수집기) 프로젝트에 새 파일로 추가해도 안전하도록
 *   모든 코드를 KWWEB 한 덩어리로 감쌌습니다.
 *   전역(다른 파일과 공유)으로 노출되는 이름은 doGet / doPost / KWWEB 단 3개뿐 →
 *   기존 수집기 함수들과 충돌하지 않습니다.
 *   ※ 단, 수집기에 이미 doGet/doPost 가 있으면 그때만 충돌(보통 없음).
 *
 * 역할: 정적 index.html(GitHub Pages)이 호출하는 서버리스 API.
 *   - doGet(action=...)  : 구글시트 읽기 + 가벼운 라이브 네이버 호출
 *   - doPost(action=...) : 구글시트 쓰기(저장)
 *
 * 배포: 배포 → 새 배포 → 웹 앱 (실행: 나 / 액세스: 모든 사용자)
 *   → /exec URL 을 index.html 의 CONFIG.API_URL 에 입력.
 */

// ── 전역 진입점 (이 3개만 전역) ──
function doGet(e)  { return KWWEB.doGet(e); }
function doPost(e) { return KWWEB.doPost(e); }

// 발굴 → 사전추가: 키워드사전에 새 행 append (독립 함수 — 최상위 어디에 둬도 됨, KWWEB 무관)
function appendDictKw(keyword, rep, seed) {
  keyword = String(keyword || '').trim();
  if (!keyword) throw 'keyword required';
  var sh = SpreadsheetApp.openById('1uD-2gHghytC-Gb4ryEWGmhtjeFqGcTY59M90sxrASyI').getSheetByName('키워드사전');
  if (!sh) throw '키워드사전 탭 없음';
  var lastCol = sh.getLastColumn();
  var H = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  var iKw = H.indexOf('키워드'), iRep = H.indexOf('대표키워드'), iSeed = H.indexOf('시드');
  if (iKw < 0) throw '키워드 컬럼 없음';
  var lastRow = sh.getLastRow();
  if (lastRow >= 2) {
    var col = sh.getRange(2, iKw + 1, lastRow - 1, 1).getValues();
    for (var i = 0; i < col.length; i++)
      if (String(col[i][0]).trim() === keyword) return { ok: false, exists: true, keyword: keyword };
  }
  var row = [];
  for (var c = 0; c < lastCol; c++) row.push('');
  row[iKw] = keyword;
  if (iRep >= 0) row[iRep] = String(rep || keyword).trim();
  if (iSeed >= 0) row[iSeed] = seed ? true : false;
  sh.appendRow(row);
  if (iSeed >= 0) { var sc = sh.getRange(sh.getLastRow(), iSeed + 1); sc.insertCheckboxes(); sc.setValue(seed ? true : false); }
  return { ok: true, keyword: keyword };
}

// ── 브랜드보드 네이버 쇼핑 검색순위(제품×키워드) — service_role로 읽음(RLS 우회) ──
//   스크립트 속성 BRANDBOARD_SERVICE_KEY 필요(서버측, 프론트·응답 노출 X).
//   PostgREST max-rows=1000 회피: 원시행을 다 받지 않고 타겟 쿼리로 키워드별 요약 반환.
//   반환 { date, count, values:[{keyword, ozBest, ozCount, ozPrev, top1mall, top1price}] } | { error }
function brandboardRankings() {
  var svcKey = PropertiesService.getScriptProperties().getProperty('BRANDBOARD_SERVICE_KEY');
  if (!svcKey) return { error: 'BRANDBOARD_SERVICE_KEY 스크립트 속성 미설정' };
  var BASE = 'https://wakgrdmdxxuljqkbdplv.supabase.co/rest/v1/naver_keyword_rankings';
  var H = { apikey: svcKey, Authorization: 'Bearer ' + svcKey };
  function get(qs) {
    var res = UrlFetchApp.fetch(BASE + '?' + qs, { headers: H, muteHttpExceptions: true });
    if (res.getResponseCode() >= 400) throw 'API ' + res.getResponseCode() + ': ' + res.getContentText().slice(0, 150);
    return JSON.parse(res.getContentText());
  }
  try {
    var top = get('select=created_at&order=created_at.desc&limit=1');
    if (!top.length) return { date: null, count: 0, values: [] };
    var latestDate = String(top[0].created_at).slice(0, 10);
    var since = latestDate + 'T00:00:00';
    var OZ = encodeURIComponent('오즈키즈');
    // 추적 키워드 목록 + top1 경쟁사 (최신 스냅샷의 rank=1 행 = 키워드당 1줄)
    var listRows = get('select=keyword,mall_name,price,title&rank=eq.1&created_at=gte.' + since + '&limit=1000');
    // 오즈키즈 오가닉 이력(최근) — 최신 상품목록(제목·순위) + 직전 스냅샷 best(증감)
    var oz = get('select=created_at,keyword,rank,title,price,product_url&mall_name=eq.' + OZ + '&order=created_at.desc&limit=1000');
    var ozByKw = {};
    oz.forEach(function (r) {
      var k = r.keyword, dt = String(r.created_at).slice(0, 10);
      ozByKw[k] = ozByKw[k] || {};
      (ozByKw[k][dt] = ozByKw[k][dt] || []).push(r);
    });
    var out = listRows.map(function (lr) {
      var k = lr.keyword, dmap = ozByKw[k] || {};
      var dates = Object.keys(dmap).sort().reverse();
      var latestOz = (dmap[latestDate] || []).slice().sort(function (a, b) { return a.rank - b.rank; });
      var best = latestOz.length ? latestOz[0].rank : null;
      var prevDate = dates.filter(function (d) { return d < latestDate; })[0];
      var prevBest = prevDate ? Math.min.apply(null, dmap[prevDate].map(function (x) { return x.rank; })) : null;
      return {
        keyword: k, ozBest: best, ozCount: latestOz.length, ozPrev: prevBest,
        top1mall: lr.mall_name, top1price: lr.price, top1title: lr.title,
        ozProducts: latestOz.map(function (x) { return { rank: x.rank, title: x.title, price: x.price, url: x.product_url }; })
      };
    });
    return { date: latestDate, count: out.length, values: out };
  } catch (e) { return { error: String(e) }; }
}

// ── 쇼핑 광고요약: CSV 업로드가 덮어씀. 키워드별 상품형/브랜드형 순위 + 광고그룹명 ──
//   rows: [{keyword, prod_rank, prod_group, brand_rank, brand_group}]
function saveAdSummary(rows) {
  var ss = SpreadsheetApp.openById('1uD-2gHghytC-Gb4ryEWGmhtjeFqGcTY59M90sxrASyI');
  var sh = ss.getSheetByName('쇼핑광고요약') || ss.insertSheet('쇼핑광고요약');
  sh.clearContents();
  var header = ['keyword', 'prod_rank', 'prod_group', 'brand_rank', 'brand_group', 'updated'];
  var ts = Utilities.formatDate(new Date(), 'Asia/Seoul', 'yyyy-MM-dd HH:mm');
  var out = [header];
  (rows || []).forEach(function (r) {
    out.push([r.keyword || '', r.prod_rank == null ? '' : r.prod_rank, r.prod_group || '',
              r.brand_rank == null ? '' : r.brand_rank, r.brand_group || '', ts]);
  });
  sh.getRange(1, 1, out.length, header.length).setValues(out);
  return { ok: true, saved: out.length - 1, updated: ts };
}

var KWWEB = {

  // ═══════════════ CONFIG — 값을 채우세요 ═══════════════
  CFG: {
    SPREADSHEET_ID: '1uD-2gHghytC-Gb4ryEWGmhtjeFqGcTY59M90sxrASyI',
    // 네이버 검색광고 API (config.py 의 NAVER_AD_* 와 동일 값) — 읽기전용 1차버전은 비워둬도 됨
    NAVER_AD_API_LICENSE: '',   // X-API-KEY
    NAVER_AD_SECRET_KEY:  '',   // 시그니처 서명용
    NAVER_AD_CUSTOMER_ID: '',   // X-Customer
    NAVER_AD_BASE_URL:    'https://api.searchad.naver.com',
    // 네이버 오픈 API (데이터랩/검색) — developers.naver.com
    NAVER_CLIENT_ID:      '',
    NAVER_CLIENT_SECRET:  '',
    // 시트명 (config.py 와 동일)
    SHEET: {
      weekly: '주간검색수', trend: '연간트렌드',
      shopping: '쇼핑검색순위', powerlink: '파워링크순위',
      shopOrganic: '쇼핑오가닉순위',
      blog: '블로그순위', cafe: '카페순위',
      settings: '설정', newkw: '신규키워드', dict: '키워드사전'
    }
  },

  // 네이버 키를 'Config' 시트에서 자동 로드 (수집기와 동일 시트 재사용 → 레포에 키 노출 X)
  //   Config 시트 행: API_KEY | SECRET_KEY | CUSTOMER_ID | CLIENT_ID | CLIENT_SECRET
  //   CFG 상수에 직접 채워둔 값이 있으면 그게 우선.
  keysLoaded: false,
  loadKeys: function () {
    if (this.keysLoaded) return;
    this.keysLoaded = true;
    try {
      var sh = this.ss().getSheetByName('Config');
      if (!sh || sh.getLastRow() < 1) return;
      var vals = sh.getRange(1, 1, Math.min(sh.getLastRow(), 50), 2).getValues();
      var m = {};
      vals.forEach(function (r) { if (r[0]) m[String(r[0]).trim()] = String(r[1]).trim(); });
      var C = this.CFG;
      C.NAVER_AD_API_LICENSE = C.NAVER_AD_API_LICENSE || m.API_KEY || m.NAVER_AD_API_LICENSE || '';
      C.NAVER_AD_SECRET_KEY  = C.NAVER_AD_SECRET_KEY  || m.SECRET_KEY || m.NAVER_AD_SECRET_KEY || '';
      C.NAVER_AD_CUSTOMER_ID = C.NAVER_AD_CUSTOMER_ID || m.CUSTOMER_ID || m.NAVER_AD_CUSTOMER_ID || '';
      C.NAVER_CLIENT_ID      = C.NAVER_CLIENT_ID      || m.CLIENT_ID || m.NAVER_CLIENT_ID || '';
      C.NAVER_CLIENT_SECRET  = C.NAVER_CLIENT_SECRET  || m.CLIENT_SECRET || m.NAVER_CLIENT_SECRET || '';
    } catch (e) {}
  },

  // ═══════════════ 라우팅 ═══════════════
  doGet: function (e) {
    var p = (e && e.parameter) || {}, S = this.CFG.SHEET;
    this.loadKeys();
    try {
      switch (p.action) {
        case 'ping':              return this.json({ ok: true, ts: new Date().toISOString() });
        case 'keystatus':         return this.json({
          ad_key: !!this.CFG.NAVER_AD_API_LICENSE, ad_secret: !!this.CFG.NAVER_AD_SECRET_KEY,
          ad_customer: !!this.CFG.NAVER_AD_CUSTOMER_ID, client_id: !!this.CFG.NAVER_CLIENT_ID,
          client_secret: !!this.CFG.NAVER_CLIENT_SECRET });
        case 'keyword_dict':      return this.json(this.sheetData(S.dict));
        case 'weekly':            return this.json(this.sheetData(S.weekly));
        case 'trend':             return this.json(this.sheetData(S.trend));
        case 'settings':          return this.json(this.settingsObj());
        case 'newkw':             return this.json(this.sheetData(S.newkw));
        case 'rank':              return this.json(this.sheetData(S[p.type] || S.shopping));
        case 'bootstrap':         return this.json({ keyword_dict: this.sheetData(S.dict), settings: this.settingsObj() });
        // ── 라이브 네이버 (인터랙티브, 소량) ──
        case 'search_volume':     return this.json(this.fetchSearchVolume(this.csv(p.kw), p.exact !== '0'));
        case 'related_kw':        return this.json(this.suggestRelatedKeywords(p.seed || '', parseInt(p.max) || 30));
        case 'shopping_category': return this.json(this.fetchShoppingCategory(this.csv(p.kw)));
        case 'datalab':           return this.json(this.fetchDatalabTrend(this.csv(p.kw), p.start, p.end));
        case 'blog_rank':         return this.json(this.fetchBlogRank(this.csv(p.kw)));
        case 'cafe_rank':         return this.json(this.fetchCafeRank(this.csv(p.kw)));
        case 'brandboard_rank':   return this.json(brandboardRankings(p.days, p.limit));
        case 'ad_summary':        return this.json(this.sheetData('쇼핑광고요약'));
        default:                  return this.json({ error: 'unknown action', actions: ['keyword_dict','weekly','trend','rank','settings','newkw','search_volume','related_kw','shopping_category','datalab','blog_rank','cafe_rank'] });
      }
    } catch (err) { return this.json({ error: String(err && err.stack || err) }); }
  },

  doPost: function (e) {
    var p = (e && e.parameter) || {}, body = {}, S = this.CFG.SHEET;
    this.loadKeys();
    try { body = e.postData && e.postData.contents ? JSON.parse(e.postData.contents) : {}; } catch (x) {}
    var action = p.action || body.action;
    try {
      switch (action) {
        case 'save_setting':      this.saveSetting(body.key, body.value); return this.json({ ok: true });
        case 'save_new_keywords': this.saveNewKeywords(body.rows || []);  return this.json({ ok: true, saved: (body.rows || []).length });
        case 'append_dict':       return this.json(appendDictKw(body.keyword, body.rep, body.seed));
        case 'append_rank':       this.appendRankHistory(body.rows || [], body.week, S[body.rank_type] || S.shopping); return this.json({ ok: true, saved: (body.rows || []).length });
        case 'save_ad_summary':   return this.json(saveAdSummary(body.rows || []));
        case 'collect_rank': {     // 블로그/카페/쇼핑오가닉 순위 라이브 조회 + 저장 (한 번에)
          var ranks = body.kind === 'cafe' ? this.fetchCafeRank(body.kw || [])
                    : body.kind === 'shopOrganic' ? this.fetchShoppingRank(body.kw || [])
                    : this.fetchBlogRank(body.kw || []);
          var rrows = ranks.map(function (r) { return { keyword: r.keyword, avg_rank: r.rank }; });
          this.appendRankHistory(rrows, body.week, S[body.kind] || S.blog);
          return this.json({ ok: true, saved: rrows.length, week: body.week });
        }
        case 'save_trend':        this.saveTrend(body.header || [], body.rows || []); return this.json({ ok: true });
        case 'collect_trend':     return this.json(this.collectTrend());
        default:                  return this.json({ error: 'unknown action' });
      }
    } catch (err) { return this.json({ error: String(err && err.stack || err) }); }
  },

  // ═══════════════ 시트 헬퍼 ═══════════════
  ss: function () { return SpreadsheetApp.openById(this.CFG.SPREADSHEET_ID); },
  sheet: function (name) { return this.ss().getSheetByName(name); },

  sheetData: function (name) {
    var sh = this.sheet(name);
    if (!sh) return { header: [], rows: [] };
    // getDisplayValues: 시트에 "보이는 그대로"(문자열) 읽기 → 날짜가 Date객체로 안 나옴 (Streamlit get_all_values 와 동일)
    var values = sh.getDataRange().getDisplayValues();
    if (!values.length) return { header: [], rows: [] };
    var header = values[0].map(function (h) { return String(h); });
    var rows = values.slice(1).map(function (r) { return r.map(function (c) { return c == null ? '' : c; }); });
    return { header: header, rows: rows };
  },

  settingsObj: function () {
    var d = this.sheetData(this.CFG.SHEET.settings), o = {};
    d.rows.forEach(function (r) { if (r[0]) o[String(r[0])] = r.length > 1 ? String(r[1]) : ''; });
    return o;
  },

  getOrCreate: function (name) {
    var ss = this.ss(), sh = ss.getSheetByName(name);
    if (!sh) sh = ss.insertSheet(name);
    return sh;
  },

  // ═══════════════ 쓰기 (google_sheets.py 포팅) ═══════════════
  saveSetting: function (key, value) {
    if (!key) throw 'key required';
    var sh = this.getOrCreate(this.CFG.SHEET.settings);
    var vals = sh.getDataRange().getValues();
    if (!vals.length) { sh.getRange(1, 1, 2, 2).setValues([['key', 'value'], [key, String(value)]]); return; }
    for (var i = 1; i < vals.length; i++) { if (vals[i][0] === key) { sh.getRange(i + 1, 2).setValue(String(value)); return; } }
    sh.appendRow([key, String(value)]);
  },

  saveNewKeywords: function (rows) {
    if (!rows.length) return;
    var sh = this.getOrCreate(this.CFG.SHEET.newkw);
    var header = ['날짜', '제품명', '카테고리', '타겟', '키워드', '출처', '월간검색수'];
    var vals = sh.getDataRange().getValues();
    if (!vals.length || String(vals[0][0]) !== '날짜') sh.insertRowBefore(1).getRange(1, 1, 1, header.length).setValues([header]);
    var out = rows.map(function (r) {
      return [r['날짜'] || '', r['제품명'] || '', r['카테고리'] || '', r['타겟'] || '', r['키워드'] || '', r['출처'] || '', r['월간검색수'] || ''];
    });
    sh.getRange(sh.getLastRow() + 1, 1, out.length, header.length).setValues(out);
  },

  appendRankHistory: function (rows, week, sheetName) {
    var clean = rows.filter(function (r) { return r.keyword && r.avg_rank !== '' && r.avg_rank != null && !isNaN(r.avg_rank); });
    if (!clean.length) return;
    var sh = this.getOrCreate(sheetName);
    var vals = sh.getDataRange().getValues();
    var rankMap = {}, seasonMap = {}, itemMap = {};
    clean.forEach(function (r) {
      var k = String(r.keyword).trim();
      rankMap[k] = Number(r.avg_rank);
      if (r['계절'] != null) seasonMap[k] = String(r['계절'] || '');
      if (r['품목'] != null) itemMap[k] = String(r['품목'] || '');
    });
    if (!vals.length || vals[0].indexOf('keyword') < 0) {
      var header = ['계절', '품목', 'keyword', week], out = [header];
      Object.keys(rankMap).forEach(function (k) { out.push([seasonMap[k] || '', itemMap[k] || '', k, rankMap[k]]); });
      sh.getRange(1, 1, out.length, header.length).setValues(out);
      return;
    }
    var header2 = vals[0], kwCol = header2.indexOf('keyword'), rowOf = {};
    for (var i = 1; i < vals.length; i++) { var k = String(vals[i][kwCol]).trim(); if (k && !(k in rowOf)) rowOf[k] = i; }
    var wkCol = header2.indexOf(week);
    if (wkCol < 0) { wkCol = header2.length; sh.getRange(1, wkCol + 1).setValue(week); }
    var newRows = [];
    Object.keys(rankMap).forEach(function (k) {
      if (k in rowOf) sh.getRange(rowOf[k] + 1, wkCol + 1).setValue(rankMap[k]);
      else { var nr = []; nr[kwCol] = k; nr[wkCol] = rankMap[k]; for (var c = 0; c < wkCol + 1; c++) if (nr[c] == null) nr[c] = ''; newRows.push(nr); }
    });
    if (newRows.length) sh.getRange(sh.getLastRow() + 1, 1, newRows.length, wkCol + 1).setValues(newRows);
  },

  saveTrend: function (header, rows) {
    var sh = this.getOrCreate(this.CFG.SHEET.trend);
    sh.clear();
    var out = [header].concat(rows);
    sh.getRange(1, 1, out.length, header.length).setValues(out);
  },

  // ═══════════════ 네이버 검색광고 API (naver_api.py 포팅) ═══════════════
  adSig: function (ts, method, uri) {
    var msg = ts + '.' + method + '.' + uri;
    return Utilities.base64Encode(Utilities.computeHmacSha256Signature(msg, this.CFG.NAVER_AD_SECRET_KEY));
  },
  adHeaders: function (method, uri) {
    var ts = String(Date.now());
    return {
      'Content-Type': 'application/json; charset=UTF-8', 'X-Timestamp': ts,
      'X-API-KEY': this.CFG.NAVER_AD_API_LICENSE, 'X-Customer': String(this.CFG.NAVER_AD_CUSTOMER_ID),
      'X-Signature': this.adSig(ts, method, uri)
    };
  },
  toNum: function (v) { if (v === '< 10') return 5; var n = parseInt(String(v).replace(/,/g, ''), 10); return isNaN(n) ? 0 : n; },

  fetchSearchVolume: function (keywords, exact) {
    var uri = '/keywordstool', out = [], BATCH = 5, self = this;
    for (var i = 0; i < keywords.length; i += BATCH) {
      var batch = keywords.slice(i, i + BATCH).map(function (k) { return k.trim(); }).filter(Boolean);
      if (!batch.length) continue;
      var url = self.CFG.NAVER_AD_BASE_URL + uri + '?hintKeywords=' + encodeURIComponent(batch.join(',')) + '&showDetail=1';
      var res = UrlFetchApp.fetch(url, { method: 'get', headers: self.adHeaders('GET', uri), muteHttpExceptions: true });
      if (res.getResponseCode() === 200) {
        ((JSON.parse(res.getContentText()).keywordList) || []).forEach(function (d) {
          var pc = self.toNum(d.monthlyPcQcCnt), mo = self.toNum(d.monthlyMobileQcCnt);
          out.push({ keyword: d.relKeyword, pc: pc, mobile: mo, total: pc + mo });
        });
      }
      Utilities.sleep(300);
    }
    if (exact) { var set = {}; keywords.forEach(function (k) { set[k.trim()] = 1; }); out = out.filter(function (r) { return set[String(r.keyword).trim()]; }); }
    return out;
  },

  fetchShoppingCategory: function (keywords) {
    var url = 'https://openapi.naver.com/v1/search/shop.json';
    var h = { 'X-Naver-Client-Id': this.CFG.NAVER_CLIENT_ID, 'X-Naver-Client-Secret': this.CFG.NAVER_CLIENT_SECRET };
    return keywords.map(function (kw) {
      var o = { keyword: kw, category1: '', category2: '', category3: '', category4: '' };
      try {
        var res = UrlFetchApp.fetch(url + '?query=' + encodeURIComponent(kw) + '&display=1', { headers: h, muteHttpExceptions: true });
        if (res.getResponseCode() === 200) {
          var it = (JSON.parse(res.getContentText()).items || [])[0];
          if (it) { o.category1 = it.category1 || ''; o.category2 = it.category2 || ''; o.category3 = it.category3 || ''; o.category4 = it.category4 || ''; }
        }
      } catch (x) {}
      Utilities.sleep(150);
      return o;
    });
  },

  fetchDatalabTrend: function (keywords, start, end) {
    if (!end) end = Utilities.formatDate(new Date(), 'Asia/Seoul', 'yyyy-MM-dd');
    if (!start) { var d = new Date(); d.setDate(d.getDate() - 365); start = Utilities.formatDate(d, 'Asia/Seoul', 'yyyy-MM-dd'); }
    var h = { 'X-Naver-Client-Id': this.CFG.NAVER_CLIENT_ID, 'X-Naver-Client-Secret': this.CFG.NAVER_CLIENT_SECRET, 'Content-Type': 'application/json' };
    var dateSet = {}, series = {};
    for (var i = 0; i < keywords.length; i += 5) {
      var batch = keywords.slice(i, i + 5);
      var body = { startDate: start, endDate: end, timeUnit: 'week', keywordGroups: batch.map(function (k) { return { groupName: k, keywords: [k] }; }) };
      var res = UrlFetchApp.fetch('https://openapi.naver.com/v1/datalab/search', { method: 'post', headers: h, payload: JSON.stringify(body), muteHttpExceptions: true });
      if (res.getResponseCode() === 200) {
        (JSON.parse(res.getContentText()).results || []).forEach(function (r) {
          series[r.title] = series[r.title] || {};
          r.data.forEach(function (pt) { dateSet[pt.period] = 1; series[r.title][pt.period] = pt.ratio; });
        });
      }
      Utilities.sleep(500);
    }
    var dates = Object.keys(dateSet).sort(), ser = {};
    Object.keys(series).forEach(function (kw) { ser[kw] = dates.map(function (d) { return series[kw][d] != null ? series[kw][d] : null; }); });
    return { dates: dates, series: ser };
  },

  // ═══════════════ 연간 트렌드 수집 (datalab 2년 + 검색량 → 주간추정 → 저장) ═══════════════
  // 키워드 = 기존 '연간트렌드' 시트의 고유 키워드(추적 세트 갱신)
  trendKeywords: function () {
    var d = this.sheetData(this.CFG.SHEET.trend), H = d.header, ik = H.indexOf('keyword');
    if (ik < 0) return [];
    var seen = {}, out = [];
    d.rows.forEach(function (r) { var k = String(r[ik] || '').trim(); if (k && !seen[k]) { seen[k] = 1; out.push(k); } });
    return out;
  },
  // estimate_weekly_search_volume 포팅: 최근4주 평균비율로 월간검색수를 주간으로 분배
  estimateWeekly: function (trend, volMap) {
    var dates = trend.dates, series = trend.series, out = [];
    Object.keys(series).forEach(function (kw) {
      var arr = series[kw], monthly = volMap[kw] || 0;
      var last4 = arr.slice(-4).filter(function (v) { return v != null; });
      if (!last4.length) return;
      var avg = last4.reduce(function (a, b) { return a + b; }, 0) / last4.length;
      if (avg === 0) return;
      var scale = monthly / (avg * 4);
      for (var i = 0; i < dates.length; i++) {
        var val = arr[i] == null ? 0 : arr[i];
        out.push([dates[i], kw, Math.round(val * scale), val]);
      }
    });
    return out;
  },
  collectTrend: function () {
    if (!this.CFG.NAVER_CLIENT_ID || !this.CFG.NAVER_AD_API_LICENSE) return { error: '네이버 키 미설정 (Config 시트 확인)' };
    var kws = this.trendKeywords();
    if (!kws.length) return { error: '수집할 키워드가 없습니다 (연간트렌드 시트가 비어있음).' };
    var vols = this.fetchSearchVolume(kws, true), volMap = {};
    vols.forEach(function (v) { volMap[v.keyword] = v.total; });
    var end = Utilities.formatDate(new Date(), 'Asia/Seoul', 'yyyy-MM-dd');
    var d2 = new Date(); d2.setDate(d2.getDate() - 730);
    var start = Utilities.formatDate(d2, 'Asia/Seoul', 'yyyy-MM-dd');
    var trend = this.fetchDatalabTrend(kws, start, end);
    var rows = this.estimateWeekly(trend, volMap);
    if (!rows.length) return { error: '데이터랩 응답이 비어있습니다 (네이버 키/쿼터 확인).' };
    this.saveTrend(['date', 'keyword', 'estimated_weekly_volume', 'ratio'], rows);
    var ts = Utilities.formatDate(new Date(), 'Asia/Seoul', 'yyyy-MM-dd HH:mm');
    this.saveSetting('trend_last_collected', ts);
    return { ok: true, keywords: kws.length, rows: rows.length, collected: ts };
  },

  // ═══════════════ 네이버 검색 API — 블로그/카페 순위 ═══════════════
  searchHeaders: function () { return { 'X-Naver-Client-Id': this.CFG.NAVER_CLIENT_ID, 'X-Naver-Client-Secret': this.CFG.NAVER_CLIENT_SECRET }; },

  rank: function (url, keywords, matcher) {
    var h = this.searchHeaders();
    return keywords.map(function (kw) {
      var rank = 0;
      try {
        var res = UrlFetchApp.fetch(url + '?query=' + encodeURIComponent(kw) + '&display=100&start=1&sort=sim', { headers: h, muteHttpExceptions: true });
        if (res.getResponseCode() === 200) {
          var items = JSON.parse(res.getContentText()).items || [];
          for (var i = 0; i < items.length; i++) { if (matcher(items[i])) { rank = i + 1; break; } }
        }
      } catch (x) {}
      Utilities.sleep(200);
      return { keyword: kw, rank: rank };
    });
  },
  // rank 병렬판 — UrlFetchApp.fetchAll로 40개씩 동시 조회(수백 개도 1~2분). 대량 collect_rank용.
  rankAll: function (url, keywords, matcher) {
    var h = this.searchHeaders(), out = [], B = 40;
    for (var i = 0; i < keywords.length; i += B) {
      var batch = keywords.slice(i, i + B);
      var reqs = batch.map(function (kw) {
        return { url: url + '?query=' + encodeURIComponent(kw) + '&display=100&start=1&sort=sim', headers: h, muteHttpExceptions: true };
      });
      var resps; try { resps = UrlFetchApp.fetchAll(reqs); } catch (e) { resps = []; }
      batch.forEach(function (kw, j) {
        var rk = 0, res = resps[j];
        try {
          if (res && res.getResponseCode() === 200) {
            var items = JSON.parse(res.getContentText()).items || [];
            for (var k = 0; k < items.length; k++) { if (matcher(items[k])) { rk = k + 1; break; } }
          }
        } catch (x) {}
        out.push({ keyword: kw, rank: rk });
      });
      Utilities.sleep(80);
    }
    return out;
  },
  fetchBlogRank: function (keywords) {
    return this.rankAll('https://openapi.naver.com/v1/search/blog.json', keywords, function (it) {
      return (it.title || '').indexOf('오즈키즈') >= 0 || (it.description || '').indexOf('오즈키즈') >= 0;
    });
  },
  fetchCafeRank: function (keywords) {
    return this.rankAll('https://openapi.naver.com/v1/search/cafearticle.json', keywords, function (it) {
      var t = (it.title || '').toLowerCase(), d = (it.description || '').toLowerCase();
      return t.indexOf('오즈키즈') >= 0 || t.indexOf('ozkiz') >= 0 || d.indexOf('오즈키즈') >= 0 || d.indexOf('ozkiz') >= 0;
    });
  },
  // 네이버 쇼핑 오가닉(비광고) 순위 — shop.json 검색결과에서 '오즈키즈' 상품이 나오는 첫 위치(최대 100)
  fetchShoppingRank: function (keywords) {
    return this.rankAll('https://openapi.naver.com/v1/search/shop.json', keywords, function (it) {
      var s = (it.mallName || '') + ' ' + (it.brand || '') + ' ' + (it.maker || '') + ' ' + (it.title || '');
      return s.indexOf('오즈키즈') >= 0 || s.toLowerCase().indexOf('ozkiz') >= 0;
    });
  },

  // ═══════════════ 연관 키워드 추천 (suggest_related_keywords 포팅) ═══════════════
  STOPWORDS: (function () {
    var o = {};
    ['추천','후기','리뷰','구매','가격','비교','순위','인기','최저가','할인','무료','배송','당일','사용','방법','정보','상품','제품','주문','결제','선택','옵션','사이즈','컬러','색상','네이버','블로그','카페','포스팅','공유','이벤트','소개','안내','사진','영상','동영상','이미지','링크','클릭','더보기','자세히','좋아요','댓글','공감','구독','팔로우','스토어','스마트','브랜드','최고','최신','진짜','완전','정말','너무','매우','아주','엄청','대박','강추','꿀팁','솔직','직접','다른','이런','그런','어떤','모든','오늘','내일','올해','작년','이번','지난','다음','요즘','최근','준비','필수','필요','가능','확인','시간','장소','날짜','기간','한번','처음','마지막','정도','그냥','바로','함께','같이','하나','가지','이상','이하','이내','부분','종류','아기','아이','아이들','엄마','아빠','육아','우리','아들','학교','학원','수업','공부','시험','과제','교육','게임','드라마','예능','택배','그림','부산','서울','경기','인천','대구','광주','대전','울산','제주','강남','홍대'].forEach(function (w) { o[w] = 1; });
    return o;
  })(),

  stripTags: function (s) { return String(s || '').replace(/<[^>]+>/g, ''); },
  hangul: function (s, min, max) { var re = new RegExp('[가-힣]{' + min + ',' + max + '}', 'g'); return String(s).match(re) || []; },

  suggestRelatedKeywords: function (seed, maxResults) {
    var self = this;
    if (!self.CFG.NAVER_CLIENT_ID || !self.CFG.NAVER_CLIENT_SECRET) return { results: [], context_words: [] };
    var h = self.searchHeaders();
    var seedClean = seed.replace(/\s/g, ''), seedWords = {};
    self.hangul(seedClean, 2, 20).forEach(function (w) { seedWords[w] = 1; });
    var texts = [];
    function collect(url, starts) {
      starts.forEach(function (st) {
        try {
          var res = UrlFetchApp.fetch(url + '?query=' + encodeURIComponent(seed) + '&display=50&start=' + st + '&sort=sim', { headers: h, muteHttpExceptions: true });
          if (res.getResponseCode() === 200) (JSON.parse(res.getContentText()).items || []).forEach(function (it) { texts.push(self.stripTags(it.title)); texts.push(self.stripTags(it.description)); });
        } catch (x) {}
        Utilities.sleep(200);
      });
    }
    collect('https://openapi.naver.com/v1/search/blog.json', [1, 51]);
    collect('https://openapi.naver.com/v1/search/cafearticle.json', [1, 51]);
    try {
      var sres = UrlFetchApp.fetch('https://openapi.naver.com/v1/search/shop.json?query=' + encodeURIComponent(seed) + '&display=50&sort=sim', { headers: h, muteHttpExceptions: true });
      if (sres.getResponseCode() === 200) (JSON.parse(sres.getContentText()).items || []).forEach(function (it) { texts.push(self.stripTags(it.title)); });
    } catch (x) {}
    if (!texts.length) return { results: [], context_words: [] };

    var cnt = {};
    texts.forEach(function (t) {
      var words = self.hangul(t, 2, 8);
      words.forEach(function (w) { cnt[w] = (cnt[w] || 0) + 1; });
      for (var i = 0; i < words.length - 1; i++) { var c2 = words[i] + words[i + 1]; if (c2.length >= 4 && c2.length <= 12) cnt[c2] = (cnt[c2] || 0) + 1; }
      for (var j = 0; j < words.length - 2; j++) { var c3 = words[j] + words[j + 1] + words[j + 2]; if (c3.length >= 6 && c3.length <= 14) cnt[c3] = (cnt[c3] || 0) + 1; }
    });
    var ctx = {};
    Object.keys(cnt).forEach(function (k) {
      if (cnt[k] < 3 || self.STOPWORDS[k] || k.length < 2 || k === seedClean || seedWords[k]) return;
      ctx[k] = cnt[k];
    });
    if (!Object.keys(ctx).length) return { results: [], context_words: [] };
    var topCtx = Object.keys(ctx).sort(function (a, b) { return ctx[b] - ctx[a]; }).slice(0, 50);

    var hints = [];
    for (var a = 0; a < topCtx.length; a += 4) hints.push([seedClean].concat(topCtx.slice(a, a + 4)));
    for (var b = 0; b < Math.min(topCtx.length, 25); b += 5) { var bb = topCtx.slice(b, b + 5); if (bb.length) hints.push(bb); }

    var uri = '/keywordstool', volMap = {};
    hints.forEach(function (batch) {
      var url = self.CFG.NAVER_AD_BASE_URL + uri + '?hintKeywords=' + encodeURIComponent(batch.join(',')) + '&showDetail=1';
      var res = UrlFetchApp.fetch(url, { method: 'get', headers: self.adHeaders('GET', uri), muteHttpExceptions: true });
      if (res.getResponseCode() === 200) {
        ((JSON.parse(res.getContentText()).keywordList) || []).forEach(function (d) {
          var total = self.toNum(d.monthlyPcQcCnt) + self.toNum(d.monthlyMobileQcCnt), k = d.relKeyword;
          if (volMap[k] == null || total > volMap[k]) volMap[k] = total;
        });
      }
      Utilities.sleep(400);
    });

    var filterWords = topCtx.filter(function (c) { return c.length >= 3; }).concat(Object.keys(seedWords)).concat([seedClean]);
    function relevant(kw) { return filterWords.some(function (fw) { return kw.indexOf(fw) >= 0; }); }
    var all = {};
    Object.keys(volMap).forEach(function (kw) {
      if (!relevant(kw) || self.STOPWORDS[kw] || kw === seedClean) return;
      all[kw] = { keyword: kw, '월간검색수': volMap[kw], '출현빈도': ctx[kw] || 0 };
    });
    topCtx.forEach(function (ck) { if (self.STOPWORDS[ck] || ck === seedClean || seedWords[ck]) return; if (!all[ck]) all[ck] = { keyword: ck, '월간검색수': volMap[ck] || 0, '출현빈도': ctx[ck] || 0 }; });

    var list = Object.keys(all).map(function (k) { return all[k]; });
    list.sort(function (x, y) { return (y['출현빈도'] - x['출현빈도']) || (y['월간검색수'] - x['월간검색수']); });
    return { results: list.slice(0, maxResults), context_words: topCtx.map(function (k) { return [k, ctx[k]]; }) };
  },

  // ═══════════════ 유틸 ═══════════════
  csv: function (s) { return s ? String(s).split(',').map(function (x) { return x.trim(); }).filter(Boolean) : []; },
  json: function (obj) { return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON); }

};
