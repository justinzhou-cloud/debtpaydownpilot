/* global Chart, ChartDataLabels */
(function () {
  "use strict";

  let attainmentChart = null;
  let flowChart = null;
  let datalabelsRegistered = false;

  function getData() {
    if (window.__PILOT_DASHBOARD__) return window.__PILOT_DASHBOARD__;
    throw new Error("Missing data: load data.js before this script.");
  }

  function $(id) {
    return document.getElementById(id);
  }

  function setAttainCardMin(px) {
    document.documentElement.style.setProperty("--dm-attain-min", px + "px");
  }

  /**
   * Chart.js can leave a canvas in a bad state after destroy; replacing the node
   * avoids "chart disappears and never comes back" when switching weeks.
   */
  function replaceCanvas(id) {
    const old = $(id);
    if (!old || !old.parentNode) return null;
    const neu = document.createElement("canvas");
    neu.id = id;
    const aria = old.getAttribute("aria-label");
    if (aria) neu.setAttribute("aria-label", aria);
    old.parentNode.replaceChild(neu, old);
    return neu;
  }

  function destroyChartSafe(canvas) {
    if (!canvas || typeof Chart === "undefined") return;
    const ch = Chart.getChart ? Chart.getChart(canvas) : null;
    if (ch) {
      ch.destroy();
    }
  }

  function ref100Plugin(refLine) {
    return {
      id: "dmRef100",
      afterDraw(chart) {
        const sc = chart.scales.x;
        if (!sc) return;
        let x;
        try {
          x = sc.getPixelForValue(100);
        } catch {
          return;
        }
        if (!Number.isFinite(x)) return;
        const top = chart.chartArea.top;
        const bottom = chart.chartArea.bottom;
        if (x < chart.chartArea.left || x > chart.chartArea.right) return;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = refLine;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.stroke();
        ctx.restore();
      },
    };
  }

  function zeroLinePlugin(zeroColor) {
    return {
      id: "dmZero",
      afterDraw(chart) {
        const sc = chart.scales.y;
        if (!sc) return;
        let y0;
        try {
          y0 = sc.getPixelForValue(0);
        } catch {
          return;
        }
        if (!Number.isFinite(y0)) return;
        const left = chart.chartArea.left;
        const right = chart.chartArea.right;
        if (y0 < chart.chartArea.top || y0 > chart.chartArea.bottom) return;
        const ctx = chart.ctx;
        ctx.save();
        ctx.strokeStyle = zeroColor;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(left, y0);
        ctx.lineTo(right, y0);
        ctx.stroke();
        ctx.restore();
      },
    };
  }

  function fmtMoneyLabel(v) {
    const a = Math.abs(v);
    if (a < 0.005) return "";
    return a >= 1000
      ? "$" + a.toLocaleString(undefined, { maximumFractionDigits: 0 })
      : "$" + a.toFixed(2);
  }

  /** Signed currency for outflow labels (always show − for negative amounts). */
  function fmtMoneySignedNegative(val) {
    const n = Number(val);
    if (!Number.isFinite(n) || Math.abs(n) < 0.005) return "";
    const a = Math.abs(n);
    const mag =
      a >= 1000
        ? "$" + a.toLocaleString(undefined, { maximumFractionDigits: 0 })
        : "$" + a.toFixed(2);
    return "−" + mag;
  }

  function attainmentOutsideLabelsPlugin(barColors, teal) {
    return {
      id: "attainmentOutsidePct",
      afterDatasetsDraw(chart) {
        const ds = chart.data.datasets[0];
        const meta = chart.getDatasetMeta(0);
        if (!meta || !meta.data || !ds) return;
        const ctx = chart.ctx;
        ctx.save();
        ctx.textAlign = "left";
        ctx.textBaseline = "middle";
        ctx.font = '700 11px "Nunito Sans", system-ui, sans-serif';
        meta.data.forEach((el, i) => {
          if (!el) return;
          const v = Number(ds.data[i]);
          if (!Number.isFinite(v) || v <= 0.05) return;
          const col = barColors[i] || teal;
          ctx.fillStyle = col;
          const props = el.getProps(["x", "y", "base"], true);
          const xEnd = Math.max(props.x, props.base);
          const y = props.y;
          ctx.fillText(Math.round(v) + "%", xEnd + 10, y);
        });
        ctx.restore();
      },
    };
  }

  /** Draw outflow $ labels just below the negative bar tip (vertical stacked chart). */
  function flowOutflowBelowBarPlugin(theme) {
    return {
      id: "flowOutflowBelowBar",
      afterDatasetsDraw(chart) {
        const meta = chart.getDatasetMeta(0);
        const ds = chart.data.datasets[0];
        if (!meta || !meta.data || !ds || ds.label !== "Outflows") return;
        const ctx = chart.ctx;
        ctx.save();
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        ctx.font = '800 9px "Nunito Sans", system-ui, sans-serif';
        ctx.fillStyle = theme.chartRed;
        meta.data.forEach((el, i) => {
          if (!el) return;
          const v = Number(ds.data[i]);
          if (!Number.isFinite(v) || Math.abs(v) < 0.005) return;
          const props = el.getProps(["x", "y", "base"], true);
          const tipY = Math.max(props.y, props.base);
          ctx.fillText(fmtMoneySignedNegative(v), props.x, tipY + 4);
        });
        ctx.restore();
      },
    };
  }

  function renderAttainment(canvasId, att, theme) {
    const oldCanvas = $(canvasId);
    destroyChartSafe(oldCanvas);

    const canvas = replaceCanvas(canvasId);
    if (!canvas) return;

    const rawLabels = (att && att.labels) || [];
    const rawPcts = (att && att.pcts) || [];
    const rawHours = (att && att.hours) || [];
    const rawGoals = (att && att.goals) || [];
    const n = Math.min(rawLabels.length, rawPcts.length);
    const pairs = [];
    for (let i = 0; i < n; i++) {
      pairs.push({
        label: rawLabels[i],
        pct: rawPcts[i] == null ? 0 : Number(rawPcts[i]),
        h: rawHours[i],
        g: rawGoals[i],
      });
    }
    // Chart.js vertical *category* y-axis: index 0 is drawn at the TOP (CategoryScale inverts pixels).
    // Descending pct → highest attainment first → top of chart, then down to lowest.
    pairs.sort((a, b) => b.pct - a.pct);
    const labels = pairs.map((p) => p.label);
    const pcts = pairs.map((p) => p.pct);
    const hours = pairs.map((p) => p.h);
    const goals = pairs.map((p) => p.g);
    const xMax = (att && att.xMax) || 100;
    const h = (att && att.chartHeight) || 280;
    const wrap = canvas.parentElement;
    /* minHeight from data; height unset so flex can stretch chart to match participants card */
    wrap.style.minHeight = h + "px";
    wrap.style.height = "";

    if (!labels.length) {
      return;
    }

    const teal = theme.chartTeal;
    const red = theme.chartRed;
    const barColors = pcts.map((p) => {
      const n = Number(p);
      return Number.isFinite(n) && n < 100 ? red : teal;
    });

    try {
      attainmentChart = new Chart(canvas, {
        type: "bar",
        plugins: [ref100Plugin(theme.refLine), attainmentOutsideLabelsPlugin(barColors, teal)],
        data: {
          labels,
          datasets: [
            {
              label: "Attainment",
              data: pcts,
              backgroundColor: barColors,
              borderWidth: 0,
              borderRadius: 3,
              barPercentage: 0.72,
              categoryPercentage: 0.88,
              datalabels: {
                display: false,
              },
            },
          ],
        },
        options: {
          animation: false,
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          layout: { padding: { right: 64, left: 4, top: 8, bottom: 4 } },
          plugins: {
            datalabels: {
              clip: false,
            },
            legend: { display: false },
            tooltip: {
              callbacks: {
                title: (items) => (items[0] ? items[0].label : ""),
                label: (ctx) => {
                  const x = ctx.parsed.x != null ? ctx.parsed.x.toFixed(1) : "";
                  return "Attainment: " + x + "%";
                },
                afterLabel: (ctx) => {
                  const i = ctx.dataIndex;
                  const hh = hours[i] != null ? Number(hours[i]).toFixed(1) : "";
                  const gg = goals[i] != null ? Number(goals[i]).toFixed(1) : "";
                  return ["Dasher hours: " + hh, "Goal hours: " + gg];
                },
              },
            },
          },
          scales: {
            x: {
              type: "linear",
              min: 0,
              max: xMax,
              title: {
                display: true,
                text: "Percent attainment",
                font: { size: 10, weight: "600" },
                color: theme.tickMuted,
              },
              grid: { display: false },
              ticks: {
                stepSize: 50,
                callback: (v) => Math.round(Number(v)) + "%",
                font: { size: 10 },
                color: theme.tickMuted,
              },
            },
            y: {
              reverse: false,
              grid: { display: false },
              ticks: { font: { size: 10.5 }, color: theme.tickMuted },
            },
          },
        },
      });
      const bumpResize = () => {
        if (attainmentChart) {
          attainmentChart.resize();
        }
      };
      requestAnimationFrame(() => {
        bumpResize();
        requestAnimationFrame(bumpResize);
      });
    } catch (e) {
      console.error("renderAttainment failed", e);
    }
  }

  function renderFlow(canvasId, labels, ins, outs, theme) {
    const oldCanvas = $(canvasId);
    destroyChartSafe(oldCanvas);

    const canvas = replaceCanvas(canvasId);
    if (!canvas) return;

    let labs = labels && labels.length ? labels.slice() : ["—"];
    let insArr = (ins || []).map((x) => Number(x) || 0);
    let outRaw = (outs || []).map((o) => Number(o) || 0);
    while (insArr.length < labs.length) insArr.push(0);
    while (outRaw.length < labs.length) outRaw.push(0);
    if (insArr.length > labs.length) insArr = insArr.slice(0, labs.length);
    if (outRaw.length > labs.length) outRaw = outRaw.slice(0, labs.length);
    const outNeg = outRaw.map((o) => -Math.abs(o));

    function ceilToStep(x, step) {
      if (!Number.isFinite(x) || x <= 0) return 0;
      return Math.ceil(x / step) * step;
    }

    const maxIn = insArr.length ? Math.max(0, ...insArr) : 0;
    const minOut = outNeg.length ? Math.min(0, ...outNeg) : 0;
    const needMag = Math.max(maxIn, Math.abs(minOut), 0);
    /* Symmetric ±R in $25 increments (25, 50, 75, …). */
    const BOUND = 25;
    const rCore = Math.max(BOUND, ceilToStep(needMag, BOUND));
    const R =
      rCore === BOUND
        ? BOUND
        : ceilToStep(rCore + BOUND, BOUND);

    /* Grid / ticks: $25 spacing below $100 axis max; $50 when max is $100+ (if it aligns). */
    const gridStep =
      R < 100 ? BOUND : R % 50 === 0 ? 50 : BOUND;

    function yTickFmt(v) {
      const n = Number(v);
      if (!Number.isFinite(n) || Math.abs(n) < 1e-9) return "$0";
      const a = Math.abs(n);
      const body =
        a >= 1000
          ? a.toLocaleString(undefined, { maximumFractionDigits: 0 })
          : String(Math.round(a));
      return n < 0 ? "−$" + body : "$" + body;
    }

    try {
      flowChart = new Chart(canvas, {
        type: "bar",
        plugins: [zeroLinePlugin(theme.refLine), flowOutflowBelowBarPlugin(theme)],
        data: {
          labels: labs,
          datasets: [
            {
              label: "Outflows",
              data: outNeg,
              stack: "jar",
              backgroundColor: theme.chartRed,
              borderWidth: 0,
              borderRadius: 3,
              datalabels: {
                display: false,
              },
            },
            {
              label: "Inflows",
              data: insArr,
              stack: "jar",
              backgroundColor: theme.chartGreen,
              borderWidth: 0,
              borderRadius: 3,
              datalabels: {
                display: (ctx) => Math.abs(Number(ctx.dataset.data[ctx.dataIndex])) >= 0.005,
                color: theme.chartGreen,
                font: { weight: "600", size: 9 },
                anchor: "end",
                align: "end",
                offset: 6,
                formatter: (val) => fmtMoneyLabel(Number(val)),
              },
            },
          ],
        },
        options: {
          animation: false,
          responsive: true,
          maintainAspectRatio: false,
          layout: { padding: { top: 36, bottom: 28, left: 4, right: 10 } },
          plugins: {
            legend: {
              display: true,
              position: "top",
              align: "start",
              labels: {
                boxWidth: 10,
                boxHeight: 10,
                color: theme.tickMuted,
                generateLabels(chart) {
                  return chart.data.datasets.map((dataset, i) => {
                    const text = dataset.label || "";
                    const outflows = text === "Outflows";
                    return {
                      text,
                      fillStyle: dataset.backgroundColor,
                      strokeStyle: dataset.borderColor,
                      lineWidth: dataset.borderWidth || 0,
                      hidden: !chart.isDatasetVisible(i),
                      index: i,
                      datasetIndex: i,
                      font: { size: 10, weight: outflows ? "800" : "600" },
                    };
                  });
                },
              },
            },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const y = ctx.parsed.y != null ? ctx.parsed.y : 0;
                  const raw = ctx.datasetIndex === 0 ? -y : y;
                  return (ctx.dataset.label || "") + ": $" + raw.toFixed(2);
                },
              },
            },
            datalabels: {
              clip: false,
            },
          },
          scales: {
            x: {
              stacked: true,
              grid: { display: false },
              ticks: { font: { size: 10 }, color: theme.tickMuted },
              title: {
                display: true,
                text: "Day",
                font: { size: 10, weight: "600" },
                color: theme.tickMuted,
              },
            },
            y: {
              stacked: true,
              min: -R,
              max: R,
              title: {
                display: true,
                text: "Amount ($)",
                font: { size: 10, weight: "600" },
                color: theme.tickMuted,
              },
              grid: { color: theme.grid },
              ticks: {
                font: { size: 10 },
                color: theme.tickMuted,
                stepSize: gridStep,
                callback: (v) => yTickFmt(v),
              },
            },
          },
        },
      });
    } catch (e) {
      console.error("renderFlow failed", e);
    }
  }

  function getView(data) {
    let weekKey = $("week-select").value;
    let v = data.views[weekKey];
    if (!v && data.default_week_key != null) {
      $("week-select").value = data.default_week_key;
      weekKey = data.default_week_key;
      v = data.views[weekKey];
    }
    if (!v) {
      const keys = Object.keys(data.views || {});
      console.warn("No dashboard view for week:", weekKey, "available:", keys);
    }
    return v;
  }

  function renderView(data) {
    const v = getView(data);
    if (!v) return;

    const theme = data.theme;
    setAttainCardMin(v.card_min_px || 360);

    $("participant-slot").innerHTML = v.participant_table_html || "";
    $("kpi-slot").innerHTML = v.kpi_html;

    renderAttainment("chart-attain", v.attainment, theme);

    const part = $("flow-participant").value;
    const flows = v.flows || {};
    const f =
      flows[part] ||
      flows["All participants"] || {
        labels: [],
        ins: [],
        outs: [],
      };
    const n = (f.labels && f.labels.length) || 1;
    const fh = Math.min(520, Math.max(280, 36 + n * 36));
    const wrap = $("flow-chart-wrap");
    if (wrap) {
      wrap.style.height = fh + "px";
      wrap.style.minHeight = fh + "px";
    }
    renderFlow("chart-flow", f.labels, f.ins, f.outs, theme);
  }

  function formatDataRefreshed(iso) {
    if (!iso) return "—";
    const d = new Date(String(iso));
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
  }

  function populateSelectors(data) {
    const el = $("data-refreshed-at");
    if (el) {
      const ts = data.snowflake_refreshed_at;
      if (ts) {
        el.style.display = "";
        el.textContent = "Data last refreshed: " + formatDataRefreshed(ts);
      } else {
        el.textContent = "";
        el.style.display = "none";
      }
    }

    const sel = $("week-select");
    sel.innerHTML = "";
    data.week_options.forEach((o) => {
      const opt = document.createElement("option");
      opt.value = o.key;
      opt.textContent = o.label;
      sel.appendChild(opt);
    });
    if (data.default_week_key != null && data.views[data.default_week_key]) {
      sel.value = data.default_week_key;
    }

    const psel = $("flow-participant");
    psel.innerHTML = "";
    data.participants.forEach((p) => {
      const opt = document.createElement("option");
      opt.value = p;
      opt.textContent = p;
      psel.appendChild(opt);
    });
  }

  function parseDataJsText(text) {
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start < 0 || end <= start) throw new Error("could not parse data.js");
    return JSON.parse(text.slice(start, end + 1));
  }

  async function fetchDashboardPayload() {
    const res = await fetch("data.js?t=" + Date.now(), { cache: "no-store" });
    if (!res.ok) throw new Error("fetch failed: " + res.status);
    return parseDataJsText(await res.text());
  }

  function init() {
    let data;
    try {
      data = getData();
    } catch (e) {
      $("app-root").innerHTML =
        '<p class="err">Could not load dashboard data. Run <code>python build_dashboard.py</code> and open this page via HTTP (e.g. <code>python serve_dashboard.py</code>) — opening <code>index.html</code> as a file cannot load <code>data.js</code>.</p>';
      console.error(e);
      return;
    }

    if (typeof ChartDataLabels !== "undefined" && typeof Chart !== "undefined" && !datalabelsRegistered) {
      Chart.register(ChartDataLabels);
      datalabelsRegistered = true;
    }

    populateSelectors(data);

    const sel = $("week-select");
    sel.addEventListener("change", () => renderView(getData()));
    $("flow-participant").addEventListener("change", () => renderView(getData()));

    renderView(data);
  }

  async function bootstrap() {
    try {
      window.__PILOT_DASHBOARD__ = await fetchDashboardPayload();
      init();
    } catch (e) {
      console.error("bootstrap", e);
      const root = $("app-root");
      if (root) {
        root.innerHTML =
          '<p class="err">Could not load dashboard data. Run <code>python build_dashboard.py</code> then open via HTTP (e.g. <code>python serve_dashboard.py</code>).</p>';
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
  } else {
    bootstrap();
  }
})();
