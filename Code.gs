/**
 * Issue Logger — Web App + Sheet Builder (robust + fast)
 * ------------------------------------------------------
 * - Caches counts and roster/issues to avoid recompute on every call.
 * - Uses versioned cache keys so writes invalidate caches automatically.
 * - Works as standalone Web App or container-bound add-on.
 */

const APP = {
  PROP_KEY_SS_ID: 'ISSUE_LOGGER_SS_ID',
  DEFAULT_SS_NAME: 'Issue Logger (Data)',

  // Version property and cache key prefixes
  PROP_PREFIX_VER: 'VER:',       // stored as ScriptProperties key by ssId (VER:<ssId>)
  CACHE_PREFIX_DATA: 'D:',       // Roster + Issues aggregate
  CACHE_PREFIX_COUNTS: 'C:',     // Counts snapshot per period
  CACHE_PREFIX_BATH_STATUS: 'BS:', // Bathroom status per period
  CACHE_PREFIX_BATH_ANALYTICS: 'BA:' // Bathroom analytics for today
};

const CONFIG = {
  ROSTER_SHEET: 'Roster',            // Name | Period
  ISSUES_SHEET: 'Issues',            // A: Issue Label
  LOG_SHEET: 'QuickLog',             // A:Timestamp | B:Student | C:Period | D:Issue | E:Notes
  COUNTS_SHEET: 'IssueCounts',       // Optional QUERY view (not required for app speed)
  BATHROOM_LOG_SHEET: 'Bathroom Breaks',
  SETTINGS_SHEET: 'Settings',
  POPUP_WIDTH: 1200,
  POPUP_HEIGHT: 900,

  // Cache TTLs (in seconds)
  CACHE_TTL_DATA: 3600,             // 1 hour for Roster + Issues
  CACHE_TTL_COUNTS: 300,            // 5 minutes for per-period counts (again versioned)
  CACHE_TTL_BATHROOM: 60            // 1 minute for bathroom data
};

/* =========================
 * Robust property storage (User + Script)
 * ========================= */

async function _getUserProps()    { return PropertiesService.getUserProperties(); }
async function _getScriptProps()  { return PropertiesService.getScriptProperties(); }

async function _getStoredSsId() {
  const u = await _getUserProps();
  const uId = u.getProperty(APP.PROP_KEY_SS_ID);
  if (uId) return uId;
  const s = await _getScriptProps();
  const sId = s.getProperty(APP.PROP_KEY_SS_ID);
  return sId || null;
}
async function _setStoredSsId(ssId) {
  if (!ssId) return;
  const up = await _getUserProps();
  up.setProperty(APP.PROP_KEY_SS_ID, ssId);
  const sp = await _getScriptProps();
  sp.setProperty(APP.PROP_KEY_SS_ID, ssId);
}
async function _clearStoredSsId() {
  const up = await _getUserProps();
  up.deleteProperty(APP.PROP_KEY_SS_ID);
  const sp = await _getScriptProps();
  sp.deleteProperty(APP.PROP_KEY_SS_ID);
}

/* =========================
 * Versioning for cache invalidation
 * ========================= */

async function _getVersion_(ssId) {
  if (!ssId) return 0;
  const sp = await _getScriptProps();
  const raw = sp.getProperty(APP.PROP_PREFIX_VER + ssId);
  return raw ? parseInt(raw, 10) || 0 : 0;
}
async function _bumpVersion_(ssId) {
  if (!ssId) return;
  const sp = await _getScriptProps();
  const current = await _getVersion_(ssId);
  sp.setProperty(APP.PROP_PREFIX_VER + ssId, String(current + 1));
}

/* =========================
 * Cache helpers
 * ========================= */

async function _cacheGet_(key) {
  try {
    const cache = CacheService.getUserCache();
    const val = cache.get(key);
    return val ? JSON.parse(val) : null;
  } catch (_) {
    return null;
  }
}
async function _cachePut_(key, obj, ttlSec) {
  try {
    const cache = CacheService.getUserCache();
    cache.put(key, JSON.stringify(obj), ttlSec);
  } catch (_) {}
}

/* =========================
 * Safe lock acquisition for web app + container-bound
 * ========================= */

async function _acquireLock_(ms) {
  const timeout = Math.max(1, ms || 30000);

  // Try document lock first (works in container-bound scripts).
  try {
    const docLock = LockService.getDocumentLock();
    if (docLock) {
      docLock.waitLock(timeout);
      return docLock;
    }
  } catch (e) { /* ignore */ }

  // Fallback to script lock (works in web-app).
  const scriptLock = LockService.getScriptLock();
  scriptLock.waitLock(timeout);
  return scriptLock;
}

/* =========================
 * Spreadsheet attach / resolve
 * ========================= */

async function _isIdTrashed_(id) {
  try {
    const f = DriveApp.getFileById(id);
    return f.isTrashed();
  } catch (e) {
    return null; // unknown or inaccessible
  }
}

async function _clearCachesFor_(ssId) {
  if (!ssId) return;
  try {
    const cache = CacheService.getUserCache();
    const ver = await _getVersion_(ssId);
    const keys = [];
    for (let v = 0; v <= ver; v++) {
      keys.push(APP.CACHE_PREFIX_DATA + ssId + ':v' + v);
      keys.push(APP.CACHE_PREFIX_BATH_ANALYTICS + ssId + ':v' + v);
      keys.push(APP.CACHE_PREFIX_COUNTS + ssId + '::v' + v);
      keys.push(APP.CACHE_PREFIX_BATH_STATUS + ssId + '::v' + v);
      for (let p = 0; p <= 10; p++) {
        keys.push(APP.CACHE_PREFIX_COUNTS + ssId + ':' + p + ':v' + v);
        keys.push(APP.CACHE_PREFIX_BATH_STATUS + ssId + ':' + p + ':v' + v);
      }
    }
    while (keys.length) {
      cache.removeAll(keys.splice(0, 100));
    }
  } catch (e) {}
  try {
    const sp = await _getScriptProps();
    sp.deleteProperty(APP.PROP_PREFIX_VER + ssId);
  } catch (e) {}
}

async function _clearAllCaches_() {
  try {
    const sp = await _getScriptProps();
    const props = sp.getProperties();
    for (const k of Object.keys(props)) {
      if (k.startsWith(APP.PROP_PREFIX_VER)) {
        await _clearCachesFor_(k.substring(APP.PROP_PREFIX_VER.length));
      }
    }
  } catch (e) {}
}

async function _getSpreadsheetOrNull_() {
  const remembered = await _getStoredSsId();
  if (remembered) {
    const trashed = await _isIdTrashed_(remembered);
    if (trashed === false) {
      try {
        return SpreadsheetApp.openById(remembered);
      } catch (e) {}
    } else if (trashed === true) {
      await _clearCachesFor_(remembered);
      await _clearStoredSsId();
    }
  }
  // Container-bound fallback: remember it
  try {
    const active = SpreadsheetApp.getActiveSpreadsheet();
    if (active) {
      const activeTrashed = await _isIdTrashed_(active.getId());
      if (activeTrashed === false) {
        await _setStoredSsId(active.getId());
        return active;
      } else if (activeTrashed === true) {
        await _clearCachesFor_(active.getId());
      }
    }
  } catch (e) {}
  return null;
}

async function _getSpreadsheet_() {
  const ss = await _getSpreadsheetOrNull_();
  if (!ss) {
    throw new Error('No data spreadsheet is attached yet. Use “Build Sheets” first.');
  }
  return ss;
}

async function _createSpreadsheet_(name) {
  const ss = SpreadsheetApp.create(name || APP.DEFAULT_SS_NAME);
  await _setStoredSsId(ss.getId());
  // any new build invalidates caches
  await _bumpVersion_(ss.getId());
  return ss;
}

async function findSpreadsheet(name) {
  const files = DriveApp.getFilesByName(name || APP.DEFAULT_SS_NAME);
  while (files.hasNext()) {
    const f = files.next();
    if (!f.isTrashed()) {
      const ssId = f.getId();
      await _setStoredSsId(ssId);
      return { ok: true, ssId, ssUrl: f.getUrl() };
    }
  }
  return { ok: false, message: 'No spreadsheet found.' };
}

/* =========================
 * App state & builder
 * ========================= */

async function getAppState() {
  const state = {
    attached: false,
    ssId: null,
    ssUrl: null,
    sheets: { roster:false, issues:false, log:false, counts:false },
    hasData: { roster:false, issues:false, log:false }
  };

  const ss = await _getSpreadsheetOrNull_();
  if (!ss) return state;

  state.attached = true;
  state.ssId = ss.getId();
  state.ssUrl = ss.getUrl();

  // ensure new bathroom-tracking fields/sheets exist
  await ensureBathroomTrackerSetup_(ss);

  const roster = ss.getSheetByName(CONFIG.ROSTER_SHEET);
  const issues = ss.getSheetByName(CONFIG.ISSUES_SHEET);
  const log    = ss.getSheetByName(CONFIG.LOG_SHEET);
  const counts = ss.getSheetByName(CONFIG.COUNTS_SHEET);

  state.sheets.roster = !!roster;
  state.sheets.issues = !!issues;
  state.sheets.log    = !!log;
  state.sheets.counts = !!counts;

  if (roster) state.hasData.roster = roster.getLastRow() > 1;
  if (issues) state.hasData.issues = issues.getLastRow() > 1;
  if (log)    state.hasData.log    = log.getLastRow() > 1;

  return state;
}

async function buildSheets(opts) {
  const seed = !!(opts && opts.seed !== false); // default true
  const name = (opts && opts.name) || APP.DEFAULT_SS_NAME;

  let ss = await _getSpreadsheetOrNull_();
  if (!ss) {
    ss = await _createSpreadsheet_(name);
  }

  await ensureRoster_(ss, seed);
  await ensureIssues_(ss, seed);
  await ensureLog_(ss);
  await ensureIssueCountsPivot_(ss);
  await ensureBathroomTrackerSetup_(ss);

  // bump version to invalidate caches (new build)
  await _bumpVersion_(ss.getId());

  return { ok:true, message:'Sheets are ready.', ssId:ss.getId(), ssUrl:ss.getUrl() };
}

async function clearAllLogs() {
  try {
    let ss = await _getSpreadsheetOrNull_();
    if (!ss) {
      ss = await _createSpreadsheet_(APP.DEFAULT_SS_NAME);
    }

    // Ensure sheets exist (created if missing)
    const log = await getSheetByName(ss, CONFIG.LOG_SHEET,
      ['Timestamp','Student','Period','Issue','Notes']);
    const bath = await getSheetByName(ss, CONFIG.BATHROOM_LOG_SHEET,
      ['Timestamp', 'Student ID', 'Student Name', 'Period', 'Direction', 'Duration (minutes)']);

    const lock = await _acquireLock_(30000);
    try {
      const lastRow = log.getLastRow();
      if (lastRow > 1) {
        log.getRange(2, 1, lastRow - 1, Math.max(log.getLastColumn(), 5)).clearContent();
      }

      const bathLast = bath.getLastRow();
      if (bathLast > 1) {
        bath.getRange(2, 1, bathLast - 1, bath.getLastColumn()).clearContent();
      }

      const counts = ss.getSheetByName(CONFIG.COUNTS_SHEET);
      if (counts) counts.clear();
      SpreadsheetApp.flush();
      await ensureIssueCountsPivot_(ss); // rebuild analytics sheet
      await _bumpVersion_(ss.getId()); // invalidate caches
      return { ok:true, message:'All logs cleared.' };
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }
  } catch (e) {
    return { ok:false, message: 'Failed to clear logs: ' + e.message };
  }
}

/* =========================
 * UI menu (container-bound)
 * ========================= */

async function onOpen() {
  await _clearAllCaches_();
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    await ensureBathroomTrackerSetup_(ss);
    SpreadsheetApp.getUi()
      .createMenu('Issue Logger')
      .addItem('Initialize Tracker (build tabs)', 'initializeTracker')
      .addSeparator()
      .addItem('Open Sidebar (narrow)', 'openLoggerSidebar')
      .addItem('Open Popup (large)', 'openLoggerPopup')
      .addItem('Open Full Screen (web app)', 'openFullScreen')
      .addToUi();
  } catch (e) {
    // Not container-bound; ignore.
  }
}

async function initializeTracker() {
  try {
    const ss = await _getSpreadsheet_();
    await ensureRoster_(ss, true);
    await ensureIssues_(ss, true);
    await ensureLog_(ss);
    await ensureIssueCountsPivot_(ss);
    await ensureBathroomTrackerSetup_(ss);
    await _bumpVersion_(ss.getId()); // invalidate caches
    SpreadsheetApp.getUi().alert('Issue Logger is ready.\nUse the menu to open Sidebar / Popup / Full Screen.');
  } catch (e) {
    try { SpreadsheetApp.getUi().alert(e.message); } catch (_) {}
  }
}

async function openLoggerSidebar() {
  const html = HtmlService.createTemplateFromFile('sidebar').evaluate()
    .setTitle('Issue Logger')
    .setSandboxMode(HtmlService.SandboxMode.IFRAME);
  try { SpreadsheetApp.getUi().showSidebar(html); } catch (e) {}
}
async function openLoggerPopup() {
  const html = HtmlService.createTemplateFromFile('sidebar').evaluate()
    .setSandboxMode(HtmlService.SandboxMode.IFRAME)
    .setWidth(CONFIG.POPUP_WIDTH)
    .setHeight(CONFIG.POPUP_HEIGHT);
  try { SpreadsheetApp.getUi().showModalDialog(html, 'Issue Logger'); } catch (e) {}
}
async function doGet() {
  return HtmlService.createTemplateFromFile('sidebar')
    .evaluate()
    .setTitle('Issue Logger')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}
async function openFullScreen() {
  const url = ScriptApp.getService().getUrl();
  if (!url) {
    try {
      SpreadsheetApp.getUi().alert(
        'First-time setup:\nDeploy ▸ Manage deployments ▸ New deployment ▸ Type: Web app\n' +
        'Execute as: User accessing the web app\n' +
        'Who has access: Anyone in your domain (or Anyone with the link)'
      );
    } catch (e) {}
    return;
  }
  const opener = HtmlService.createHtmlOutput(
    `<script>window.open(${JSON.stringify(url)}, "_blank");google.script.host.close();</script>`
  ).setWidth(10).setHeight(10);
  try { SpreadsheetApp.getUi().showModalDialog(opener, 'Opening Issue Logger…'); } catch (e) {}
}

/* =========================
 * Idempotent sheet creators
 * ========================= */

async function ensureRoster_(ss, seed) {
  ss = ss || await _getSpreadsheet_();
  let sh = ss.getSheetByName(CONFIG.ROSTER_SHEET);
  if (!sh) sh = ss.insertSheet(CONFIG.ROSTER_SHEET);

  if (sh.getLastRow() === 0) {
    sh.getRange('A1:B1').setValues([['Name','Period']]);
    sh.setFrozenRows(1);
  }
  if (seed && sh.getLastRow() <= 1) {
    sh.getRange(2,1,6,2).setValues([
      ['Student One','Period 1'],
      ['Student Two','Period 1'],
      ['Student Three','Period 2'],
      ['Student Four','Period 2'],
      ['Student Five','Period 3'],
      ['Student Six','Period 3'],
    ]);
  }
  return sh;
}

async function ensureIssues_(ss, seed) {
  ss = ss || await _getSpreadsheet_();
  let sh = ss.getSheetByName(CONFIG.ISSUES_SHEET);
  if (!sh) sh = ss.insertSheet(CONFIG.ISSUES_SHEET);

  if (sh.getLastRow() === 0) {
    sh.getRange('A1').setValue('Issue Label');
    sh.setFrozenRows(1);
  }
  if (seed && sh.getLastRow() <= 1) {
    sh.getRange(2,1,6,1).setValues([
      ['Off-task'],
      ['Disruptive'],
      ['Missing work'],
      ['Out of seat'],
      ['Refusal'],
      ['Phone use'],
    ]);
  }
  return sh;
}

async function ensureLog_(ss) {
  ss = ss || await _getSpreadsheet_();
  let sh = ss.getSheetByName(CONFIG.LOG_SHEET);
  if (!sh) {
    sh = ss.insertSheet(CONFIG.LOG_SHEET);
    sh.getRange('A1:E1').setValues([['Timestamp','Student','Period','Issue','Notes']]);
    sh.setFrozenRows(1);
    return sh;
  }
  if (sh.getLastRow() === 0) {
    sh.getRange('A1:E1').setValues([['Timestamp','Student','Period','Issue','Notes']]);
    sh.setFrozenRows(1);
    return sh;
  }
  const header = sh.getRange(1,1,1,Math.max(5, sh.getLastColumn())).getValues()[0].map(String);
  if (header.join('|').toLowerCase() === 'timestamp|student|issue|notes') {
    sh.insertColumnBefore(3);
    sh.getRange('A1:E1').setValues([['Timestamp','Student','Period','Issue','Notes']]);
  }
  return sh;
}

async function ensureIssueCountsPivot_(ss) {
  ss = ss || await _getSpreadsheet_();
  let sh = ss.getSheetByName(CONFIG.COUNTS_SHEET);
  if (!sh) sh = ss.insertSheet(CONFIG.COUNTS_SHEET);
  sh.clear();

  sh.getRange('A1').setValue(
    `=QUERY(${CONFIG.LOG_SHEET}!A1:E,
    "select Col2, count(Col4) where Col2 is not null group by Col2 pivot Col4 label count(Col4) ''", 1)`
  );
  sh.setFrozenRows(1);
  sh.setFrozenColumns(1);
  return sh;
}

/* =========================
 * Data fetchers (CACHED)
 * ========================= */

async function getData() {
  const ss = await _getSpreadsheetOrNull_();
  if (!ss) return { periods: [], perMap: {}, issues: [] };

  const ssId = ss.getId();
  const ver = await _getVersion_(ssId);
  const cacheKey = APP.CACHE_PREFIX_DATA + ssId + ':v' + ver;

  const cached = await _cacheGet_(cacheKey);
  if (cached) return cached;

  const rSh = await ensureRoster_(ss, false);
  const iSh = await ensureIssues_(ss, false);

  const rLast = rSh.getLastRow();
  const iLast = iSh.getLastRow();

  let roster = [];
  if (rLast >= 2) {
    const rVals = rSh.getRange(2,1,rLast-1,2).getValues();
    roster = rVals.filter(r => (r[0] && String(r[0]).trim()!==''));
  }

  const periods = Array.from(new Set(
    roster.map(r => String(r[1]||'').trim()).filter(Boolean)
  )).sort();

  const perMap = {};
  periods.forEach(p => perMap[p] = []);
  roster.forEach(r => {
    const name = String(r[0]||'').trim();
    const per  = String(r[1]||'').trim();
    if (name && per) perMap[per].push(name);
  });
  Object.keys(perMap).forEach(p => perMap[p].sort());

  let issues = [];
  if (iLast >= 2) {
    issues = iSh.getRange(2,1,iLast-1,1).getValues()
      .flat().map(v => String(v||'').trim()).filter(Boolean);
  }

  const result = { periods, perMap, issues };
  await _cachePut_(cacheKey, result, CONFIG.CACHE_TTL_DATA);
  return result;
}

/**
 * Counts for a selected Period (CACHED per period).
 * Reads columns B: Student, C: Period, D: Issue only once, tally in memory.
 */
async function getCountsSnapshot(period) {
  const ss = await _getSpreadsheetOrNull_();
  if (!ss) {
    return { issues: [], rows: [], totalsByIssue: [], totalsByStudent: [], totalLogs: 0, zeroStudents: 0, issueVariety: 0 };
  }

  const p = String(period || '');
  const ssId = ss.getId();
  const ver = await _getVersion_(ssId);
  const cacheKey = APP.CACHE_PREFIX_COUNTS + ssId + ':' + p + ':v' + ver;

  const cached = await _cacheGet_(cacheKey);
  if (cached) return cached;

  const rosterSh = ss.getSheetByName(CONFIG.ROSTER_SHEET);
  const issuesSh = ss.getSheetByName(CONFIG.ISSUES_SHEET);
  const logSh = ss.getSheetByName(CONFIG.LOG_SHEET);
  if (!rosterSh || !issuesSh || !logSh) {
    return { issues: [], rows: [], totalsByIssue: [], totalsByStudent: [], totalLogs: 0, zeroStudents: 0, issueVariety: 0 };
  }

  // Issues
  const issuesLast = issuesSh.getLastRow();
  const issues = (issuesLast >= 2)
    ? issuesSh.getRange(2,1,issuesLast-1,1).getValues().flat().map(x=>String(x||'').trim()).filter(Boolean)
    : [];

  // Names in this period
  const rosterLast = rosterSh.getLastRow();
  const namesInPeriod = (rosterLast >= 2)
    ? rosterSh.getRange(2,1,rosterLast-1,2).getValues()
        .filter(r => String(r[1]||'').trim() === p)
        .map(r => String(r[0]||'').trim())
        .filter(Boolean)
    : [];

  if (!issues.length || !namesInPeriod.length) {
    const emptyResult = {
      issues,
      rows: namesInPeriod.map(n => ({ student:n, counts: new Array(issues.length).fill(0) })),
      totalsByIssue: issues.map(lab=>({lab, sum:0})),
      totalsByStudent: namesInPeriod.map(n=>({student:n, sum:0})),
      totalLogs: 0, zeroStudents: namesInPeriod.length, issueVariety: 0
    };
    await _cachePut_(cacheKey, emptyResult, CONFIG.CACHE_TTL_COUNTS);
    return emptyResult;
  }

  const idxByIssue = new Map(issues.map((lab,i)=>[lab,i]));
  const wanted = new Set(namesInPeriod);

  // Read QuickLog minimal columns: B:Student, C:Period, D:Issue
  const lastRow = logSh.getLastRow();
  let lVals = [];
  if (lastRow >= 2) {
    lVals = logSh.getRange(2,2,lastRow-1,3).getValues();
  }

  // Initialize counts per student
  const countsMap = new Map();
  namesInPeriod.forEach(n => countsMap.set(n, new Array(issues.length).fill(0)));

  // Tally only rows that match the requested period
  for (let i = 0; i < lVals.length; i++) {
    const student = String(lVals[i][0]||'').trim();
    const periodCell = String(lVals[i][1]||'').trim();
    const issue = String(lVals[i][2]||'').trim();
    if (periodCell !== p) continue;
    if (!wanted.has(student)) continue;
    const idx = idxByIssue.get(issue);
    if (idx == null) continue;
    const arr = countsMap.get(student);
    if (arr) arr[idx] = (arr[idx] || 0) + 1;
  }

  const rows = namesInPeriod.map(n => ({ student:n, counts: countsMap.get(n) || new Array(issues.length).fill(0) }));

  // Analytics helpers
  const totalsByIssue = issues.map((lab, i) => {
    let sum = 0; for (let r=0; r<rows.length; r++) sum += (rows[r].counts[i] || 0);
    return { lab, sum };
  });
  const totalsByStudent = rows.map(r => ({ student: r.student, sum: (r.counts||[]).reduce((a,b)=>a+(b||0),0) }));
  const totalLogs = totalsByIssue.reduce((a,b)=>a+b.sum,0);
  const zeroStudents = totalsByStudent.filter(s => s.sum === 0).length;
  const issueVariety = totalsByIssue.filter(t => t.sum > 0).length;

  const result = { issues, rows, totalsByIssue, totalsByStudent, totalLogs, zeroStudents, issueVariety };
  await _cachePut_(cacheKey, result, CONFIG.CACHE_TTL_COUNTS);
  return result;
}

/* =========================
 * Logging & Undo (invalidate cache via versioning)
 * ========================= */

async function logEntries(payload) {
  try {
    const entries = (payload && payload.entries) ? payload.entries : [];
    if (!entries.length) return { ok:false, message:'No entries.' };

    const ss = await _getSpreadsheet_();
    const log = ss.getSheetByName(CONFIG.LOG_SHEET) || await ensureLog_(ss);

    // Build Name -> Period map once per call
    const rosterSh = ss.getSheetByName(CONFIG.ROSTER_SHEET) || await ensureRoster_(ss, false);
    const rLast = rosterSh.getLastRow();
    const rVals = rLast >= 2 ? rosterSh.getRange(2,1,rLast-1,2).getValues() : [];
    const nameToPeriod = new Map();
    for (let i=0;i<rVals.length;i++){
      const name = String(rVals[i][0]||'').trim();
      const per  = String(rVals[i][1]||'').trim();
      if (name) nameToPeriod.set(name, per);
    }

    const lock = await _acquireLock_(30000);
    try {
      const now = new Date();
      const rows = entries.map(e => {
        const student = String(e.student||'').trim();
        const issue   = String(e.issue||'').trim();
        const notes   = String(e.notes||'').trim();
        if (!student || !issue) return null;
        const period = nameToPeriod.get(student) || '';
        return [ (payload && payload.ts ? new Date(payload.ts) : now), student, period, issue, notes ];
      }).filter(Boolean);

      if (!rows.length) return { ok:false, message:'No valid entries.' };

      const startRow = Math.max(log.getLastRow()+1, 2);
      log.getRange(startRow, 1, rows.length, 5).setValues(rows);
      SpreadsheetApp.flush();
      // bump version -> invalidates caches (counts and data)
      await _bumpVersion_(ss.getId());
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }
    return { ok:true, message:'Logged.' };
  } catch (e) {
    return { ok:false, message:'Failed to save: ' + e.message };
  }
}

async function deleteLastEntry(payload) {
  try {
    const student = String(payload && payload.student || '').trim();
    const issue   = String(payload && payload.issue || '').trim();
    const period  = String(payload && payload.period || '').trim();
    if (!student || !issue) return { ok:false, message:'Missing student or issue.' };

    const ss = await _getSpreadsheet_();
    const log = ss.getSheetByName(CONFIG.LOG_SHEET) || await ensureLog_(ss);

    const lock = await _acquireLock_(30000);
    try {
      const lastRow = log.getLastRow();
      if (lastRow < 2) return { ok:false, message:'No logs to undo.' };

      // Read B:Student, C:Period, D:Issue, scan from bottom
      const numRows = lastRow - 1;
      const vals = log.getRange(2, 2, numRows, 3).getValues(); // [ [Student, Period, Issue], ... ]
      for (let i = vals.length - 1; i >= 0; i--) {
        const s = String(vals[i][0] || '').trim();
        const p = String(vals[i][1] || '').trim();
        const is = String(vals[i][2] || '').trim();
        const periodMatches = period ? (p === period) : true;
        if (s === student && is === issue && periodMatches) {
          const sheetRow = 2 + i;
          log.deleteRow(sheetRow);
          SpreadsheetApp.flush();
          await _bumpVersion_(ss.getId()); // invalidate caches
          return { ok:true, message:'Deleted last entry.', row: sheetRow };
        }
      }
      return { ok:false, message:'No matching log to undo.' };
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }
  } catch (e) {
    return { ok:false, message:'Failed to undo: ' + e.message };
  }
}

/* =========================
 * Diagnostics
 * ========================= */

async function pingWrite() {
  try {
    const ss = await _getSpreadsheet_();
    const log = ss.getSheetByName(CONFIG.LOG_SHEET) || await ensureLog_(ss);

    const lock = await _acquireLock_(5000);
    try {
      const cell = log.getRange('A1');
      const prev = cell.getNote();
      cell.setNote('ping ' + new Date().toISOString());
      cell.setNote(prev || '');
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }
    return { ok:true, message:'Ping ok', ssId:ss.getId(), ssUrl:ss.getUrl() };
  } catch (e) {
    return { ok:false, message:'Ping failed: ' + e.message };
  }
}

/* =========================
 * Bathroom Tracker
 * ========================= */

async function getSheetByName(ss, name, headers) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    if (headers && headers.length > 0) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      sheet.setFrozenRows(1);
    }
  }
  return sheet;
}

async function addStudentIdColumnToRoster(ss) {
  ss = ss || await _getSpreadsheet_();
  const rosterSheet = await ensureRoster_(ss);
  const headers = rosterSheet.getRange(1, 1, 1, rosterSheet.getLastColumn()).getValues()[0];
  if (headers.indexOf('Student ID') === -1) {
    rosterSheet.getRange(1, headers.length + 1).setValue('Student ID');
  }
}

async function getBathroomBreakLimit(ss) {
  ss = ss || await _getSpreadsheet_();
  const settingsSheet = await getSheetByName(ss, CONFIG.SETTINGS_SHEET, ['Key', 'Value']);
  const data = settingsSheet.getDataRange().getValues();
  for (let i = 1; i < data.length; i++) {
    if (data[i][0] === 'Bathroom Break Limit') {
      return parseInt(data[i][1], 10);
    }
  }
  settingsSheet.appendRow(['Bathroom Break Limit', 3]);
  return 3;
}

async function ensureBathroomTrackerSetup_(ss) {
  ss = ss || await _getSpreadsheet_();
  await addStudentIdColumnToRoster(ss);
  const logSheet = await getSheetByName(ss, CONFIG.BATHROOM_LOG_SHEET, ['Timestamp', 'Student ID', 'Student Name', 'Period', 'Direction', 'Duration (minutes)']);
  const headers = logSheet.getRange(1, 1, 1, logSheet.getLastColumn()).getValues()[0];
  if (headers.indexOf('Period') === -1) {
    logSheet.insertColumnAfter(3);
    logSheet.getRange(1, 4).setValue('Period');
  }
  await getSheetByName(ss, CONFIG.SETTINGS_SHEET, ['Key', 'Value']);
  await getBathroomBreakLimit(ss);
}

async function processBarcode(studentId) {
  try {
    const lock = await _acquireLock_(30000);
    try {
      await ensureBathroomTrackerSetup_();
      return await recordBathroomBreak(studentId);
    } finally {
      try { lock.releaseLock(); } catch (_) {}
    }
  } catch (e) {
    return "Error: " + e.message;
  }
}

async function recordBathroomBreak(studentId) {
  const ss = await _getSpreadsheet_();
  const bathroomLogSheet = await getSheetByName(ss, CONFIG.BATHROOM_LOG_SHEET, ['Timestamp', 'Student ID', 'Student Name', 'Period', 'Direction', 'Duration (minutes)']);
  const rosterSheet = ss.getSheetByName(CONFIG.ROSTER_SHEET);

  // Find student name
  const studentData = rosterSheet.getDataRange().getValues();
  let studentName = null;
  let studentPeriod = '';
  let studentIdCol = -1;
  let studentNameCol = -1;
  let periodCol = -1;

  const headers = studentData[0];
  for(let i=0; i< headers.length; i++) {
    if(headers[i] === 'Student ID') studentIdCol = i;
    if(headers[i] === 'Name') studentNameCol = i;
    if(headers[i] === 'Period') periodCol = i;
  }

  if(studentIdCol === -1) throw new Error("Student ID column not found in Roster.");
  if(studentNameCol === -1) throw new Error("Name column not found in Roster.");


  for (let i = 1; i < studentData.length; i++) {
    if (studentData[i][studentIdCol] == studentId) {
      studentName = studentData[i][studentNameCol];
      studentPeriod = periodCol > -1 ? studentData[i][periodCol] : '';
      break;
    }
  }

  if (!studentName) {
    throw new Error('Student not found in Roster. Please add the student and their ID to the Roster sheet.');
  }

  const logData = bathroomLogSheet.getDataRange().getValues();
  let lastDirection = null;
  let lastOutTime = null;
  let tripsToday = 0;
  const today = new Date().setHours(0, 0, 0, 0);

  for (let i = logData.length - 1; i >= 1; i--) {
    if (logData[i][1] == studentId) {
       const logDate = new Date(logData[i][0]).setHours(0, 0, 0, 0);
       if(logDate === today && logData[i][4] === 'out') {
         tripsToday++;
       }
       if(lastDirection === null) { // only set last direction on the most recent entry
          lastDirection = logData[i][4];
          if(lastDirection === 'out'){
            lastOutTime = new Date(logData[i][0]);
          }
       }
    }
  }


  if (lastDirection === 'out') {
    const now = new Date();
    const duration = Math.round((now - lastOutTime) / 60000);
    bathroomLogSheet.appendRow([now, studentId, studentName, studentPeriod, 'in', duration]);
    await _bumpVersion_(ss.getId());
    return `${studentName} checked back in. Duration: ${duration} minutes.`;
  } else {
    const limit = await getBathroomBreakLimit(ss);
    if (tripsToday >= limit) {
      throw new Error(`${studentName} has reached the bathroom break limit of ${limit}.`);
    }
    bathroomLogSheet.appendRow([new Date(), studentId, studentName, studentPeriod, 'out', '']);
    await _bumpVersion_(ss.getId());
    return `${studentName} checked out for a bathroom break.`;
  }
}

async function getBathroomAnalytics() {
  const ss = await _getSpreadsheet_();
  const ssId = ss.getId();
  const ver = await _getVersion_(ssId);
  const cacheKey = APP.CACHE_PREFIX_BATH_ANALYTICS + ssId + ':v' + ver;
  const cached = await _cacheGet_(cacheKey);
  if (cached) return cached;

  const bathroomLogSheet = ss.getSheetByName(CONFIG.BATHROOM_LOG_SHEET);
  if (!bathroomLogSheet) {
    const empty = { students: {}, periods: {} };
    await _cachePut_(cacheKey, empty, CONFIG.CACHE_TTL_BATHROOM);
    return empty;
  }

  const logData = bathroomLogSheet.getDataRange().getValues();
  const analytics = { students: {}, periods: {} };
  const today = new Date().setHours(0, 0, 0, 0);

  for (let i = 1; i < logData.length; i++) {
    const row = logData[i];
    const logDate = new Date(row[0]).setHours(0, 0, 0, 0);
    if (logDate !== today) continue;
    const studentName = row[2];
    const period = row[3];
    const direction = row[4];
    const duration = parseInt(row[5], 10) || 0;

    if (direction === 'in') {
      if (!analytics.students[studentName]) {
        analytics.students[studentName] = { visits: 0, minutes: 0 };
      }
      if (!analytics.periods[period]) {
        analytics.periods[period] = { visits: 0, minutes: 0 };
      }
      analytics.students[studentName].visits += 1;
      analytics.students[studentName].minutes += duration;
      analytics.periods[period].visits += 1;
      analytics.periods[period].minutes += duration;
    }
  }

  await _cachePut_(cacheKey, analytics, CONFIG.CACHE_TTL_BATHROOM);
  return analytics;
}

async function getBathroomStatus(period) {
  const ss = await _getSpreadsheet_();
  const p = String(period || '');
  const ssId = ss.getId();
  const ver = await _getVersion_(ssId);
  const cacheKey = APP.CACHE_PREFIX_BATH_STATUS + ssId + ':' + p + ':v' + ver;
  const cached = await _cacheGet_(cacheKey);
  if (cached) return cached;

  const logSheet = ss.getSheetByName(CONFIG.BATHROOM_LOG_SHEET);
  if (!logSheet) {
    const empty = { out: [], in: [] };
    await _cachePut_(cacheKey, empty, CONFIG.CACHE_TTL_BATHROOM);
    return empty;
  }

  const today = new Date().setHours(0, 0, 0, 0);
  const data = logSheet.getDataRange().getValues();
  const map = {};
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const ts = new Date(row[0]);
    const day = new Date(ts);
    day.setHours(0, 0, 0, 0);
    if (day.getTime() !== today) continue;
    if (p && row[3] !== p) continue;
    const id = row[1];
    const name = row[2];
    const direction = row[4];
    map[id] = map[id] || { name: name };
    map[id].direction = direction;
    if (direction === 'out') {
      map[id].outTime = ts.toISOString();
    } else if (direction === 'in') {
      map[id].duration = row[5];
    }
  }

  const out = [];
  const inside = [];
  Object.values(map).forEach((info) => {
    if (info.direction === 'out') {
      out.push({ name: info.name, outTime: info.outTime });
    } else if (info.direction === 'in') {
      inside.push({ name: info.name, duration: info.duration });
    }
  });
  out.sort((a, b) => a.name.localeCompare(b.name));
  inside.sort((a, b) => a.name.localeCompare(b.name));

  const result = { out: out, in: inside };
  await _cachePut_(cacheKey, result, CONFIG.CACHE_TTL_BATHROOM);
  return result;
}
