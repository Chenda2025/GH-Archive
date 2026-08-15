const $ = (id) => document.getElementById(id);

const state = {
  poll: null,
  lastResults: null,
  lastPreview: null,
  lastDataset: null,
  lastHdfs: null,
  lastApiState: null,
};

const axisColor = "#64748b";
const gridColor = "rgba(148, 163, 184, 0.25)";

function barChart(canvasId, color) {
  return new Chart($(canvasId).getContext("2d"), {
    type: "bar",
    data: { labels: [], datasets: [{ data: [], backgroundColor: color, borderWidth: 0 }] },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: {
          beginAtZero: true,
          ticks: { color: axisColor, maxTicksLimit: 5 },
          grid: { color: gridColor },
        },
        y: {
          ticks: { color: axisColor, font: { size: 10 } },
          grid: { display: false },
        },
      },
    },
  });
}

function doughnut(canvasId, colors) {
  return new Chart($(canvasId).getContext("2d"), {
    type: "doughnut",
    data: { labels: [], datasets: [{ data: [], backgroundColor: colors, borderWidth: 0 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "right",
          labels: { color: axisColor, boxWidth: 10, font: { size: 10 } },
        },
      },
    },
  });
}

const repoChart = barChart("repoChart", "#2563eb");
const userChart = barChart("userChart", "#ea580c");
const langChart = doughnut("langChart", [
  "#2563eb", "#7c3aed", "#059669", "#ea580c", "#dc2626",
  "#0891b2", "#9333ea", "#16a34a", "#d97706", "#db2777",
]);
const typeChart = doughnut("typeChart", [
  "#2563eb", "#7c3aed", "#059669", "#ea580c", "#dc2626",
  "#0891b2", "#9333ea", "#16a34a", "#d97706", "#db2777",
]);

function fmtSec(v) {
  if (v == null || Number.isNaN(Number(v))) return "—";
  return `${Number(v).toFixed(3)}s`;
}

function tipText(key, vars = {}) {
  let text = t(key);
  Object.entries(vars).forEach(([name, value]) => {
    text = text.replaceAll(`{${name}}`, String(value));
  });
  return text;
}

function setStatTooltip(id, titleKey, bodyKey, vars = {}) {
  const el = $(id);
  if (!el) return;
  const hasRun = vars.hasRun;
  const body = hasRun ? tipText(bodyKey, vars) : t("metric.tip.empty");
  el.innerHTML = `<strong>${t(titleKey)}</strong><p>${body}</p>`;
}

function topChartRow(rows) {
  const list = rows || [];
  if (!list.length) return { name: "—", count: "—" };
  return {
    name: list[0].name || "—",
    count: Number(list[0].count || 0).toLocaleString(),
  };
}

function renderChartTooltips(results) {
  const hasRun = Boolean(results);
  const charts = (results && results.charts) || {};
  const card = (results && results.scorecard) || {};
  const records = Number(card.records || 0).toLocaleString();
  const repos = (charts.top_repos || []).slice(0, 15);
  const users = (charts.top_users || []).slice(0, 15);
  const langs = charts.languages || [];
  const types = charts.event_types || [];

  const repoTop = topChartRow(repos);
  const userTop = topChartRow(users);
  const langTop = topChartRow(langs);
  const typeTop = topChartRow(types);

  setStatTooltip("tipRepoChart", "chart.repos.tipTitle", "chart.repos.tipBody", {
    hasRun,
    topName: repoTop.name,
    topCount: repoTop.count,
    shown: repos.length || 15,
    records,
  });
  setStatTooltip("tipUserChart", "chart.users.tipTitle", "chart.users.tipBody", {
    hasRun,
    topName: userTop.name,
    topCount: userTop.count,
    shown: users.length || 15,
    records,
  });
  setStatTooltip("tipLangChart", "chart.langs.tipTitle", "chart.langs.tipBody", {
    hasRun,
    topName: langTop.name,
    topCount: langTop.count,
    shown: langs.length || 20,
    records,
  });
  setStatTooltip("tipTypeChart", "chart.types.tipTitle", "chart.types.tipBody", {
    hasRun,
    topName: typeTop.name,
    topCount: typeTop.count,
    shown: types.length || 20,
    records,
  });
}

function renderStatTooltips(results) {
  const hasRun = Boolean(results);
  const sd = (results && results.schema_discovery) || {};
  const card = (results && results.scorecard) || {};
  const h = (results && results.hadoop) || {};
  const s = (results && results.spark) || {};
  const records = Number(card.records || h.records || 0).toLocaleString();
  const speedup = Number((results && results.speedup) || 0).toFixed(2);

  setStatTooltip("tipHadoopParse", "metric.hadoop.tipTitle", "metric.hadoop.tipBody", {
    hasRun,
    value: fmtSec(sd.hadoop_parse_s),
    records,
  });
  setStatTooltip("tipSparkSchema", "metric.sparkSchema.tipTitle", "metric.sparkSchema.tipBody", {
    hasRun,
    value: fmtSec(sd.spark_schema_s),
    fields: Number(sd.fields_discovered || s.fields_discovered || 0).toLocaleString(),
  });
  setStatTooltip("tipSparkQuery", "metric.sparkQuery.tipTitle", "metric.sparkQuery.tipBody", {
    hasRun,
    value: fmtSec(sd.spark_query_s),
  });
  setStatTooltip("tipSpeedup", "metric.speedup.tipTitle", "metric.speedup.tipBody", {
    hasRun,
    hadoop: Number(card.hadoop_s || h.total_s || 0).toFixed(3),
    spark: Number(card.spark_s || s.total_s || 0).toFixed(3),
    value: speedup,
    note: t(Number(speedup) >= 1 ? "metric.speedup.noteFast" : "metric.speedup.noteSlow"),
  });
}

function setBusy(busy) {
  ["btnDownload", "btnHdfs", "btnCompare", "btnTrackRun"].forEach((id) => {
    const el = $(id);
    if (el) el.disabled = busy;
  });
}

function setChart(chart, rows) {
  const data = rows || [];
  chart.data.labels = data.map((r) => r.name);
  chart.data.datasets[0].data = data.map((r) => r.count);
  chart.update();
}

function renderPreview(rows) {
  state.lastPreview = rows;
  const body = $("previewBody");
  if (!rows || !rows.length) {
    body.innerHTML = `<tr class="empty"><td colspan="6" data-i18n="preview.empty">${t("preview.empty")}</td></tr>`;
    return;
  }
  body.innerHTML = rows
    .map(
      (r) => `<tr>
        <td>${r.type || ""}</td>
        <td>${r.actor || ""}</td>
        <td>${r.repo || ""}</td>
        <td class="mono">${r.created_at || ""}</td>
        <td>${r.language || "—"}</td>
        <td class="mono">${(r.payload_keys || []).join(", ")}</td>
      </tr>`
    )
    .join("");
}

function renderMeta(dataset, hdfs) {
  state.lastDataset = dataset;
  state.lastHdfs = hdfs;
  const el = $("datasetMeta");
  const bits = [];
  if (dataset) {
    bits.push(`<div><dt>${t("meta.events")}</dt><dd>${Number(dataset.events || 0).toLocaleString()}</dd></div>`);
    bits.push(`<div><dt>${t("meta.source")}</dt><dd>${dataset.source || "—"}</dd></div>`);
    bits.push(`<div><dt>${t("meta.bytes")}</dt><dd>${Number(dataset.bytes || 0).toLocaleString()}</dd></div>`);
  }
  if (hdfs) {
    bits.push(`<div><dt>${t("meta.hdfs")}</dt><dd>${hdfs.mode} ${hdfs.logical_uri}</dd></div>`);
  }
  el.innerHTML = bits.join("");
}

function renderResults(results) {
  if (!results) return;
  state.lastResults = results;
  const sd = results.schema_discovery || {};
  $("mHadoopParse").textContent = fmtSec(sd.hadoop_parse_s);
  $("mSparkSchema").textContent = fmtSec(sd.spark_schema_s);
  $("mSparkQuery").textContent = fmtSec(sd.spark_query_s);
  $("mSpeedup").textContent = `${Number(results.speedup || 0).toFixed(2)}×`;

  const card = results.scorecard || {};
  const formula = $("mSpeedupFormula");
  if (formula) {
    const hs = Number(card.hadoop_s || 0);
    const ss = Number(card.spark_s || 0);
    if (hs > 0 && ss > 0) {
      formula.removeAttribute("data-i18n");
      formula.textContent = `${hs.toFixed(3)}s ÷ ${ss.toFixed(3)}s`;
    } else {
      formula.setAttribute("data-i18n", "metric.speedup.d");
      formula.textContent = t("metric.speedup.d");
    }
  }

  const schemaEl = $("schemaTree");
  schemaEl.removeAttribute("data-i18n");
  schemaEl.textContent = sd.schema_text || t("schema.placeholder");

  const narrativeEl = $("narrative");
  narrativeEl.removeAttribute("data-i18n");
  const km = getLang() === "km";
  narrativeEl.textContent =
    (km ? results.narrative_km : results.narrative) ||
    results.narrative ||
    t("narrative.empty");

  const winnerEl = $("winnerTag");
  winnerEl.removeAttribute("data-i18n");
  winnerEl.textContent = `${card.winner || "Spark"} · ${card.backend || "engine"} · ${Number(
    card.records || 0
  ).toLocaleString()} ${t("records")}`;

  $("timesRow").innerHTML = [
    ["time.hadoopTotal", card.hadoop_s],
    ["time.sparkProcess", card.spark_s],
    ["time.hadoopParse", card.hadoop_parse_s],
    ["time.sparkSchema", card.spark_schema_s],
    ["time.session", sd.spark_session_s],
  ]
    .map(([k, v]) => `<span>${t(k)}: <b>${fmtSec(v)}</b></span>`)
    .join("");

  const charts = results.charts || {};
  setChart(repoChart, (charts.top_repos || []).slice(0, 15));
  setChart(userChart, (charts.top_users || []).slice(0, 15));
  setChart(langChart, charts.languages || []);
  setChart(typeChart, charts.event_types || []);
  renderStatTooltips(results);
  renderChartTooltips(results);
}

function statusLabel(status) {
  if (status === "running") return t("status.running");
  if (status === "error") return t("status.error");
  return t("status.idle");
}

function markStages(ids, activeIndex, completeAll) {
  ids.forEach((id, index) => {
    const el = $(id);
    if (!el) return;
    el.classList.toggle("active", !completeAll && index === activeIndex);
    el.classList.toggle("done", completeAll || index < activeIndex);
  });
}

function renderTracking(data) {
  state.lastApiState = data;
  const progress = data.progress || {};
  const history = data.progress_history || [];
  const results = data.results || null;
  const dataset = data.dataset || null;
  const hdfs = data.hdfs || null;

  $("trackInputText").textContent = hdfs
    ? `${Number((dataset && dataset.events) || 0).toLocaleString()} ${t("records")} · ${hdfs.logical_uri}`
    : t("track.input.wait");

  const hHistory = history.filter((item) => item.stage === "hadoop");
  const sHistory = history.filter((item) => item.stage === "spark");
  const hCurrent = progress.stage === "hadoop";
  const sCurrent = progress.stage === "spark";
  const hDone = Boolean(results) || sHistory.length > 0 || progress.stage === "scorecard";
  const sDone = Boolean(results) || progress.stage === "scorecard";

  $("trackHadoop").classList.toggle("is-running", hCurrent);
  $("trackSpark").classList.toggle("is-running", sCurrent);

  const hState = $("trackHadoopState");
  hState.textContent = t(hDone ? "track.done" : hCurrent ? "track.running" : "track.waiting");
  hState.className = `track-state ${hDone ? "done" : hCurrent ? "running" : ""}`;

  const sState = $("trackSparkState");
  sState.textContent = t(sDone ? "track.done" : sCurrent ? "track.running" : "track.waiting");
  sState.className = `track-state ${sDone ? "done" : sCurrent ? "running" : ""}`;

  const hText = String(progress.status || "").toLowerCase();
  let hIndex = 0;
  if (hText.includes("shuffle")) hIndex = 1;
  if (hText.includes("reduce")) hIndex = 2;
  markStages(["hStageMap", "hStageShuffle", "hStageReduce"], hCurrent ? hIndex : -1, hDone);

  const sText = String(progress.status || "").toLowerCase();
  let sIndex = 0;
  if (sText.includes("read.json") || sText.includes("schema")) sIndex = 1;
  if (sText.includes("catalyst") || sText.includes("filter")) sIndex = 2;
  markStages(["sStageSession", "sStageSchema", "sStageCatalyst"], sCurrent ? sIndex : -1, sDone);

  const h = results && results.hadoop;
  const s = results && results.spark;
  $("trackHRecords").textContent = h ? Number(h.records || 0).toLocaleString() : "—";
  $("trackHTime").textContent = h ? fmtSec(h.total_s) : "—";
  $("trackHParse").textContent = h ? fmtSec(h.parse_s) : "—";
  $("trackSRecords").textContent = s ? Number(s.records || 0).toLocaleString() : "—";
  $("trackSTime").textContent = s ? fmtSec(s.total_s) : "—";
  $("trackSFields").textContent = s ? Number(s.fields_discovered || 0).toLocaleString() : "—";

  $("trackCurrent").textContent = data.status === "running"
    ? String(progress.status || data.message || t("track.running"))
    : statusLabel(data.status || "idle");

  if (results) {
    const winner = (results.scorecard && results.scorecard.winner) || "Spark";
    $("trackResultText").textContent = `${winner} · ${Number(results.speedup || 0).toFixed(2)}×`;
    $("trackSpeed").textContent = `${Number(results.speedup || 0).toFixed(2)}×`;
  } else {
    $("trackResultText").textContent = t("track.output.wait");
    $("trackSpeed").textContent = "—";
  }

  const log = $("trackLog");
  if (!history.length) {
    log.innerHTML = `<li class="empty-log">${t("track.log.empty")}</li>`;
  } else {
    log.innerHTML = history
      .map((item, index) => {
        const stage = item.stage || "pipeline";
        return `<li class="${stage}"><b>${String(index + 1).padStart(2, "0")} · ${stage.toUpperCase()}</b> — ${item.status || ""}</li>`;
      })
      .join("");
  }
}

async function pullState() {
  const res = await fetch("/api/state");
  const data = await res.json();
  const status = data.status || "idle";
  $("statusPill").textContent = statusLabel(status);
  $("statusPill").className = `status ${status}`;

  const msg = $("message");
  if (data.message && status !== "idle") {
    msg.removeAttribute("data-i18n");
    msg.textContent = data.message;
  } else if (status === "idle" && !data.results) {
    msg.setAttribute("data-i18n", "msg.ready");
    msg.textContent = t("msg.ready");
  } else if (data.message) {
    msg.removeAttribute("data-i18n");
    msg.textContent = data.message;
  }

  const err = $("error");
  if (data.error) {
    err.hidden = false;
    err.textContent = data.error;
  } else {
    err.hidden = true;
    err.textContent = "";
  }
  renderPreview(data.preview);
  renderMeta(data.dataset, data.hdfs);
  if (data.results) renderResults(data.results);
  renderTracking(data);
  const busy = status === "running";
  setBusy(busy);
  if (!busy && state.poll) {
    clearInterval(state.poll);
    state.poll = null;
  }
}

function startPoll() {
  if (state.poll) return;
  state.poll = setInterval(pullState, 600);
  pullState();
}

async function post(url, body) {
  setBusy(true);
  $("statusPill").className = "status running";
  $("statusPill").textContent = t("status.running");
  await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  startPoll();
}

$("btnDownload").addEventListener("click", () => {
  post("/api/download", {
    source: $("source").value,
    max_events: Number($("maxEvents").value),
    hours: Number($("hours").value),
    day: $("day").value,
  });
});

$("btnHdfs").addEventListener("click", () => post("/api/hdfs", {}));

$("btnCompare").addEventListener("click", () => {
  const n = Number($("maxEvents").value);
  post("/api/compare", { max_events: n || null });
});

const trackRun = $("btnTrackRun");
if (trackRun) {
  trackRun.addEventListener("click", () => {
    const n = Number($("maxEvents").value);
    post("/api/compare", { max_events: n || null });
  });
}

const TEAM_KEY = "p6_team_data";
const TEAM_CODE_KEY = "p6_team_access_code";
const TEAM_UNLOCK_KEY = "p6_team_unlocked";
const DEFAULT_TEAM_CODE = "GH-P6-2026";
const defaultTeamData = {
  schoolName: "",
  schoolLogo: "",
  lecturerName: "",
  lecturerPhoto: "",
  subject: "",
  students: Array.from({ length: 4 }, () => ({ name: "", skills: "", photo: "" })),
};

function cloneTeam(data) {
  return JSON.parse(JSON.stringify(data));
}

function loadTeamData() {
  try {
    const saved = JSON.parse(localStorage.getItem(TEAM_KEY) || "null");
    return saved && Array.isArray(saved.students)
      ? { ...cloneTeam(defaultTeamData), ...saved }
      : cloneTeam(defaultTeamData);
  } catch {
    return cloneTeam(defaultTeamData);
  }
}

let teamData = loadTeamData();
let teamDraft = cloneTeam(teamData);

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setProfileImage(imageId, fallbackId, source) {
  const image = $(imageId);
  const fallback = $(fallbackId);
  if (!image || !fallback) return;
  image.hidden = !source;
  fallback.hidden = Boolean(source);
  if (source) image.src = source;
  else image.removeAttribute("src");
}

function renderTeam() {
  const hasSchool = Boolean(teamData.schoolName);
  const schoolHero = $("schoolNameHero");
  schoolHero.textContent = hasSchool ? teamData.schoolName : t("team.subtitle");
  schoolHero.toggleAttribute("data-i18n", !hasSchool);
  if (!hasSchool) schoolHero.dataset.i18n = "team.subtitle";

  const schoolName = $("schoolNameDisplay");
  schoolName.textContent = teamData.schoolName || t("team.pending");
  schoolName.toggleAttribute("data-i18n", !teamData.schoolName);
  if (!teamData.schoolName) schoolName.dataset.i18n = "team.pending";

  const logoStatus = $("schoolLogoStatus");
  logoStatus.textContent = teamData.schoolLogo ? t("track.done") : t("team.pendingImage");
  logoStatus.removeAttribute("data-i18n");
  setProfileImage("schoolLogoImage", "schoolLogoFallback", teamData.schoolLogo);

  const lecturerName = $("lecturerNameDisplay");
  lecturerName.textContent = teamData.lecturerName || t("team.pending");
  lecturerName.removeAttribute("data-i18n");
  const subject = $("subjectDisplay");
  subject.textContent = teamData.subject || t("team.pending");
  subject.removeAttribute("data-i18n");
  setProfileImage("lecturerPhotoImage", "lecturerPhotoFallback", teamData.lecturerPhoto);

  const students = teamData.students.length ? teamData.students : defaultTeamData.students;
  $("studentGrid").innerHTML = students
    .map((student, index) => `
      <article class="student-card">
        <div class="student-avatar">
          ${student.photo
            ? `<img src="${student.photo}" alt="${escapeHtml(student.name || t("team.student.placeholder"))}" />`
            : `<span>${t("team.photo")}</span>`}
        </div>
        <span class="student-number">${String(index + 1).padStart(2, "0")}</span>
        <h3>${escapeHtml(student.name || t("team.student.placeholder"))}</h3>
        <p>${escapeHtml(student.skills || t("team.skill.placeholder"))}</p>
      </article>`)
    .join("");
}

function imageFromFile(file) {
  return new Promise((resolve, reject) => {
    if (!file) return resolve("");
    const reader = new FileReader();
    reader.onerror = reject;
    reader.onload = () => {
      const image = new Image();
      image.onerror = reject;
      image.onload = () => {
        const max = 720;
        const scale = Math.min(1, max / Math.max(image.width, image.height));
        const canvas = document.createElement("canvas");
        canvas.width = Math.max(1, Math.round(image.width * scale));
        canvas.height = Math.max(1, Math.round(image.height * scale));
        canvas.getContext("2d").drawImage(image, 0, 0, canvas.width, canvas.height);
        resolve(canvas.toDataURL("image/jpeg", 0.82));
      };
      image.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

function renderStudentEditors() {
  const list = $("studentEditorList");
  list.innerHTML = teamDraft.students
    .map((student, index) => `
      <div class="student-editor-row" data-student-index="${index}">
        <span class="student-editor-index">${index + 1}</span>
        <label>
          <span class="label-text">${t("team.editor.name")}</span>
          <input class="student-name-input" type="text" value="${escapeHtml(student.name)}"
            placeholder="${escapeHtml(t("team.editor.studentPlaceholder"))}" />
        </label>
        <label>
          <span class="label-text">${t("team.editor.skills")}</span>
          <input class="student-skills-input" type="text" value="${escapeHtml(student.skills)}"
            placeholder="${escapeHtml(t("team.editor.skillsPlaceholder"))}" />
        </label>
        <label>
          <span class="label-text">${t("team.photo")}</span>
          <input class="student-photo-input" type="file" accept="image/*" />
        </label>
        <button type="button" class="btn btn-ghost remove-student">${t("team.editor.remove")}</button>
      </div>`)
    .join("");

  list.querySelectorAll(".student-editor-row").forEach((row) => {
    const index = Number(row.dataset.studentIndex);
    row.querySelector(".student-name-input").addEventListener("input", (event) => {
      teamDraft.students[index].name = event.target.value;
    });
    row.querySelector(".student-skills-input").addEventListener("input", (event) => {
      teamDraft.students[index].skills = event.target.value;
    });
    row.querySelector(".student-photo-input").addEventListener("change", async (event) => {
      if (event.target.files[0]) {
        teamDraft.students[index].photo = await imageFromFile(event.target.files[0]);
      }
    });
    row.querySelector(".remove-student").addEventListener("click", () => {
      teamDraft.students.splice(index, 1);
      renderStudentEditors();
    });
  });
}

function openTeamEditor() {
  teamDraft = cloneTeam(teamData);
  $("editSchoolName").value = teamDraft.schoolName;
  $("editLecturerName").value = teamDraft.lecturerName;
  $("editSubject").value = teamDraft.subject;
  $("teamSaveMessage").textContent = "";
  $("teamCodeMessage").textContent = "";
  $("editNewCode").value = "";
  $("editConfirmCode").value = "";
  renderStudentEditors();
  $("teamEditor").hidden = false;
  $("teamCodeSection").hidden = false;
  updateTeamLockUi();
  $("teamEditor").scrollIntoView({ behavior: "smooth", block: "start" });
}

function getAccessCode() {
  return localStorage.getItem(TEAM_CODE_KEY) || DEFAULT_TEAM_CODE;
}

function setAccessCode(code) {
  localStorage.setItem(TEAM_CODE_KEY, code);
}

function isTeamUnlocked() {
  return sessionStorage.getItem(TEAM_UNLOCK_KEY) === "1";
}

function unlockTeam() {
  sessionStorage.setItem(TEAM_UNLOCK_KEY, "1");
  updateTeamLockUi();
}

function lockTeam() {
  sessionStorage.removeItem(TEAM_UNLOCK_KEY);
  $("teamEditor").hidden = true;
  updateTeamLockUi();
}

function updateTeamLockUi() {
  const badge = $("teamLockBadge");
  const lockBtn = $("btnLockTeam");
  const codeSection = $("teamCodeSection");
  if (!badge) return;

  const unlocked = isTeamUnlocked();
  badge.classList.toggle("locked", !unlocked);
  badge.classList.toggle("unlocked", unlocked);
  badge.dataset.i18n = unlocked ? "team.lock.unlocked" : "team.lock.locked";
  badge.textContent = t(unlocked ? "team.lock.unlocked" : "team.lock.locked");

  if (lockBtn) lockBtn.hidden = !unlocked;
  if (codeSection) codeSection.hidden = !unlocked || $("teamEditor").hidden;
}

function showTeamCodeModal() {
  const modal = $("teamCodeModal");
  const input = $("teamCodeInput");
  const error = $("teamCodeError");
  if (!modal || !input) return;
  input.value = "";
  if (error) error.hidden = true;
  modal.hidden = false;
  input.focus();
}

function hideTeamCodeModal() {
  const modal = $("teamCodeModal");
  const input = $("teamCodeInput");
  const error = $("teamCodeError");
  if (modal) modal.hidden = true;
  if (input) input.value = "";
  if (error) error.hidden = true;
}

function verifyTeamCode(input) {
  return String(input || "").trim() === getAccessCode();
}

function requestTeamEdit() {
  if (isTeamUnlocked()) {
    openTeamEditor();
    return;
  }
  showTeamCodeModal();
}

function confirmTeamCodeEntry() {
  const input = $("teamCodeInput");
  const error = $("teamCodeError");
  if (!input) return;
  if (verifyTeamCode(input.value)) {
    hideTeamCodeModal();
    unlockTeam();
    openTeamEditor();
    return;
  }
  if (error) error.hidden = false;
  input.focus();
  input.select();
}

$("btnEditTeam").addEventListener("click", requestTeamEdit);
$("btnLockTeam").addEventListener("click", lockTeam);
$("btnConfirmTeamCode").addEventListener("click", confirmTeamCodeEntry);
$("btnCancelTeamCode").addEventListener("click", hideTeamCodeModal);
$("teamCodeBackdrop").addEventListener("click", hideTeamCodeModal);
$("teamCodeInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    confirmTeamCodeEntry();
  }
  if (event.key === "Escape") hideTeamCodeModal();
});

$("btnSaveTeamCode").addEventListener("click", () => {
  const next = $("editNewCode").value.trim();
  const confirm = $("editConfirmCode").value.trim();
  const message = $("teamCodeMessage");
  if (next.length < 4) {
    message.textContent = t("team.code.tooShort");
    return;
  }
  if (next !== confirm) {
    message.textContent = t("team.code.mismatch");
    return;
  }
  setAccessCode(next);
  $("editNewCode").value = "";
  $("editConfirmCode").value = "";
  message.textContent = t("team.code.saved");
});
$("btnCloseTeamEditor").addEventListener("click", () => {
  $("teamEditor").hidden = true;
  updateTeamLockUi();
});
$("btnAddStudent").addEventListener("click", () => {
  teamDraft.students.push({ name: "", skills: "", photo: "" });
  renderStudentEditors();
});

$("editSchoolName").addEventListener("input", (event) => { teamDraft.schoolName = event.target.value; });
$("editLecturerName").addEventListener("input", (event) => { teamDraft.lecturerName = event.target.value; });
$("editSubject").addEventListener("input", (event) => { teamDraft.subject = event.target.value; });
$("editSchoolLogo").addEventListener("change", async (event) => {
  if (event.target.files[0]) teamDraft.schoolLogo = await imageFromFile(event.target.files[0]);
});
$("editLecturerPhoto").addEventListener("change", async (event) => {
  if (event.target.files[0]) teamDraft.lecturerPhoto = await imageFromFile(event.target.files[0]);
});

$("teamEditor").addEventListener("submit", (event) => {
  event.preventDefault();
  teamData = cloneTeam(teamDraft);
  try {
    localStorage.setItem(TEAM_KEY, JSON.stringify(teamData));
    $("teamSaveMessage").textContent = t("team.editor.saved");
    renderTeam();
  } catch {
    $("teamSaveMessage").textContent = "Images are too large for browser storage.";
  }
});

$("btnResetTeam").addEventListener("click", () => {
  teamData = cloneTeam(defaultTeamData);
  teamDraft = cloneTeam(defaultTeamData);
  localStorage.removeItem(TEAM_KEY);
  renderStudentEditors();
  renderTeam();
  $("editSchoolName").value = "";
  $("editLecturerName").value = "";
  $("editSubject").value = "";
  $("teamSaveMessage").textContent = "";
});

document.addEventListener("langchange", () => {
  if (state.lastPreview) renderPreview(state.lastPreview);
  else renderPreview([]);
  renderMeta(state.lastDataset, state.lastHdfs);
  if (state.lastResults) renderResults(state.lastResults);
  else {
    renderStatTooltips(null);
    renderChartTooltips(null);
  }
  if (state.lastApiState) renderTracking(state.lastApiState);
  renderTeam();
  updateTeamLockUi();
  const pill = $("statusPill");
  if (pill) {
    const cls = [...pill.classList].find((c) => ["idle", "running", "error"].includes(c)) || "idle";
    pill.textContent = statusLabel(cls);
  }
});

const VIEW_KEY = "p6_view";
const TAB_KEY = "p6_tab";

function switchView(name) {
  const views = ["benchmark", "tracking", "howto", "team"];
  document.querySelectorAll(".nav-link").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.view === name);
  });
  views.forEach((view) => {
    const el = $(`view${view.charAt(0).toUpperCase()}${view.slice(1)}`);
    if (!el) return;
    const active = view === name;
    el.hidden = !active;
    el.classList.toggle("is-active", active);
  });
  localStorage.setItem(VIEW_KEY, name);
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function switchTab(name) {
  const tabs = { overview: "tabOverview", hadoop: "tabHadoop", spark: "tabSpark", compare: "tabCompare" };
  document.querySelectorAll(".board-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.tab === name);
  });
  Object.entries(tabs).forEach(([key, id]) => {
    const el = $(id);
    if (el) el.hidden = key !== name;
  });
  localStorage.setItem(TAB_KEY, name);
}

document.querySelectorAll(".nav-link").forEach((btn) => {
  btn.addEventListener("click", () => switchView(btn.dataset.view));
});

document.querySelectorAll(".board-tab").forEach((btn) => {
  btn.addEventListener("click", () => switchTab(btn.dataset.tab));
});

["btnGoBenchmark1", "btnGoBenchmark2"].forEach((id) => {
  const btn = $(id);
  if (btn) btn.addEventListener("click", () => switchView("benchmark"));
});

switchView(localStorage.getItem(VIEW_KEY) || "benchmark");
switchTab(localStorage.getItem(TAB_KEY) || "overview");
renderTeam();
updateTeamLockUi();
renderStatTooltips(null);
renderChartTooltips(null);

pullState();
