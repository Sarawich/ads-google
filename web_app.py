from __future__ import annotations

import os
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import List, Optional

from flask import Flask, redirect, render_template_string, request, url_for

import export_campaigns as ads


app = Flask(__name__)
DB_PATH = os.path.join(os.path.dirname(__file__), "ads_history.db")
SCHEDULER_INTERVAL_SECONDS = 60
SCHEDULER_DAYS = 30
SCHEDULER_ENABLED = True
_scheduler_started = False
_scheduler_stop_event = threading.Event()
_scheduler_backoff_until: Optional[datetime] = None
_scheduler_forced_enabled = False

TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Google Ads Campaigns</title>
    <style>
      :root { color-scheme: light; }
      body {
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        margin: 24px;
        color: #1f2937;
      }
      h1 { margin: 0 0 8px; }
      form { margin: 16px 0 24px; display: flex; gap: 12px; flex-wrap: wrap; }
      label { font-weight: 600; }
      input {
        padding: 6px 10px;
        border: 1px solid #cbd5f1;
        border-radius: 6px;
      }
      button {
        background: #1f2937;
        color: white;
        border: none;
        border-radius: 6px;
        padding: 8px 14px;
        cursor: pointer;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
      }
      th, td {
        border-bottom: 1px solid #e5e7eb;
        padding: 8px 10px;
        text-align: right;
        white-space: nowrap;
      }
      th:first-child, td:first-child,
      th:nth-child(2), td:nth-child(2),
      th:nth-child(3), td:nth-child(3) {
        text-align: left;
      }
      tr.total-row {
        font-weight: 700;
        background: #f8fafc;
      }
      .meta { color: #6b7280; font-size: 13px; margin-bottom: 8px; }
      .error {
        background: #fee2e2;
        border: 1px solid #fecaca;
        padding: 12px;
        border-radius: 8px;
        color: #991b1b;
      }
      .mini-game {
        margin-top: 16px;
        padding: 12px;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
        background: #f9fafb;
        max-width: 420px;
      }
      .mini-game h2 {
        margin: 0 0 8px;
        font-size: 16px;
      }
      .mini-game .score {
        font-weight: 700;
      }
      .mini-game .row {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
      }
      .mini-game canvas {
        display: block;
        background: #0f172a;
        border: 1px solid #111827;
        border-radius: 6px;
      }
    </style>
  </head>
  <body>
    <h1>Google Ads Campaigns</h1>
    <div class="meta">Fetched at {{ fetched_at }}</div>
    <div class="meta"><a href="/history">View history</a></div>
    <div class="meta">
      Scheduler: {{ scheduler_status }}{% if backoff_until %} | Backoff until {{ backoff_until }}{% endif %}
    </div>
    {% if message %}
      <div class="meta">{{ message }}</div>
    {% endif %}

    <form method="get">
      <div>
        <label>Customer ID</label><br/>
        <input name="customer_id" value="{{ customer_id or '' }}" placeholder="177-690-3111" />
      </div>
      <div>
        <label>Days</label><br/>
        <input name="days" type="number" min="1" max="365" value="{{ days }}" />
      </div>
      <div style="align-self: end;">
        <button type="submit">Refresh</button>
      </div>
    </form>

    <form method="post" action="/control" style="margin-top: 8px;">
      <input type="hidden" name="customer_id" value="{{ customer_id or '' }}" />
      <button name="action" value="start" type="submit">Start Auto Fetch</button>
      <button name="action" value="stop" type="submit">Stop Auto Fetch</button>
      <button name="action" value="run_once" type="submit">Run Once</button>
    </form>

    <div class="mini-game">
      <h2>Mini Game: XO</h2>
      <canvas id="xo-canvas" width="240" height="240"></canvas>
      <div class="row" style="margin-top: 8px;">
        <button id="xo-reset" type="button">Reset</button>
        <div>Winner: <span class="score" id="xo-winner">-</span></div>
        <div>Turn: <span class="score" id="xo-turn">X</span></div>
      </div>
      <div class="meta">Click a cell to place X or O. Local only.</div>
    </div>

    {% if error %}
      <div class="error">{{ error }}</div>
    {% elif rows %}
      <table>
        <thead>
          <tr>
            {% for header in headers %}
              <th>{{ header }}</th>
            {% endfor %}
          </tr>
        </thead>
        <tbody>
          {% for row in rows %}
            <tr class="{{ 'total-row' if row.campaign_name == 'TOTAL' else '' }}">
              <td>{{ row.campaign_id }}</td>
              <td>{{ row.campaign_name }}</td>
              <td>{{ row.status }}</td>
              <td>{{ row.impressions }}</td>
              <td>{{ row.clicks }}</td>
              <td>{{ row.ctr_percent }}</td>
              <td>{{ row.search_impression_share }}</td>
              <td>{{ row.average_cpc }}</td>
              <td>{{ row.cost_per_click }}</td>
              <td>{{ row.cost }}</td>
              <td>{{ row.conversions }}</td>
              <td>{{ row.conversion_value }}</td>
              <td>{{ row.cost_per_conversion }}</td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    {% else %}
      <div class="meta">Enter a customer ID to load campaigns.</div>
    {% endif %}

    <script>
      (function () {
        const canvas = document.getElementById("xo-canvas");
        const ctx = canvas.getContext("2d");
        const resetBtn = document.getElementById("xo-reset");
        const winnerEl = document.getElementById("xo-winner");
        const turnEl = document.getElementById("xo-turn");

        const size = 3;
        const cell = 80;
        const board = Array.from({ length: size }, () => Array(size).fill(""));
        let current = "X";
        let winner = "";

        const drawGrid = () => {
          ctx.clearRect(0, 0, canvas.width, canvas.height);
          ctx.strokeStyle = "#e5e7eb";
          ctx.lineWidth = 2;
          for (let i = 1; i < size; i++) {
            ctx.beginPath();
            ctx.moveTo(i * cell, 0);
            ctx.lineTo(i * cell, canvas.height);
            ctx.stroke();
            ctx.beginPath();
            ctx.moveTo(0, i * cell);
            ctx.lineTo(canvas.width, i * cell);
            ctx.stroke();
          }
        };

        const drawMarks = () => {
          ctx.font = "48px Arial";
          ctx.textAlign = "center";
          ctx.textBaseline = "middle";
          for (let y = 0; y < size; y++) {
            for (let x = 0; x < size; x++) {
              const value = board[y][x];
              if (!value) continue;
              ctx.fillStyle = value === "X" ? "#e2e8f0" : "#60a5fa";
              ctx.fillText(value, x * cell + cell / 2, y * cell + cell / 2);
            }
          }
        };

        const checkWinner = () => {
          const lines = [];
          for (let i = 0; i < size; i++) {
            lines.push(board[i]);
            lines.push([board[0][i], board[1][i], board[2][i]]);
          }
          lines.push([board[0][0], board[1][1], board[2][2]]);
          lines.push([board[0][2], board[1][1], board[2][0]]);
          for (const line of lines) {
            if (line.every((v) => v === "X")) return "X";
            if (line.every((v) => v === "O")) return "O";
          }
          return "";
        };

        const isDraw = () => board.flat().every((v) => v);

        const render = () => {
          drawGrid();
          drawMarks();
          winnerEl.textContent = winner || "-";
          turnEl.textContent = current;
        };

        const reset = () => {
          for (let y = 0; y < size; y++) {
            for (let x = 0; x < size; x++) board[y][x] = "";
          }
          current = "X";
          winner = "";
          render();
        };

        canvas.addEventListener("click", (event) => {
          if (winner) return;
          const rect = canvas.getBoundingClientRect();
          const x = Math.floor((event.clientX - rect.left) / cell);
          const y = Math.floor((event.clientY - rect.top) / cell);
          if (board[y][x]) return;
          board[y][x] = current;
          winner = checkWinner();
          if (!winner && isDraw()) winner = "Draw";
          if (!winner) current = current === "X" ? "O" : "X";
          render();
        });

        resetBtn.addEventListener("click", reset);
        render();
      })();
    </script>
  </body>
</html>
"""

HISTORY_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Google Ads History</title>
    {% if auto_refresh %}
    <meta http-equiv="refresh" content="{{ refresh_seconds }}">
    {% endif %}
    <style>
      :root { color-scheme: light; }
      body {
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        margin: 24px;
        color: #1f2937;
      }
      h1 { margin: 0 0 8px; }
      h2 { margin: 16px 0 8px; }
      a { color: #1f2937; }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
        margin-bottom: 24px;
      }
      th, td {
        border-bottom: 1px solid #e5e7eb;
        padding: 8px 10px;
        text-align: right;
        white-space: nowrap;
      }
      th:first-child, td:first-child,
      th:nth-child(2), td:nth-child(2) {
        text-align: left;
      }
      .meta { color: #6b7280; font-size: 13px; margin-bottom: 8px; }
      .total-row { font-weight: 700; background: #f8fafc; }
      .chart {
        width: 100%;
        max-width: 900px;
        margin: 8px 0 24px;
      }
      .chart svg {
        width: 100%;
        height: 140px;
        display: block;
        background: #f8fafc;
        border: 1px solid #e5e7eb;
        border-radius: 8px;
      }
      .chart .line {
        fill: none;
        stroke: #111827;
        stroke-width: 2;
      }
      .chart .dot {
        fill: #111827;
      }
      .pager {
        display: flex;
        gap: 10px;
        align-items: center;
        margin-bottom: 16px;
      }
      .pager a {
        text-decoration: none;
        padding: 4px 10px;
        border: 1px solid #d1d5db;
        border-radius: 6px;
      }
    </style>
  </head>
  <body>
    <h1>Google Ads History</h1>
    <div class="meta"><a href="/">Back to live view</a></div>
    <div class="meta">
      Auto-refresh: {{ "on" if auto_refresh else "off" }}
      {% if auto_refresh %}
      ({{ refresh_seconds }}s)
      {% endif %}
      |
      <a href="/history?page={{ page }}&page_size={{ page_size }}&auto_refresh={{ 0 if auto_refresh else 1 }}{% if run_id %}&run_id={{ run_id }}{% endif %}">
        {{ "Pause refresh" if auto_refresh else "Resume refresh" }}
      </a>
      |
      Total runs: {{ stats.total_runs }} |
      Total campaigns: {{ stats.total_campaigns }} |
      Last run: {{ stats.last_run or "-" }}
    </div>
    {% if stats.latest_run_id %}
      <div class="meta">
        Latest run: #{{ stats.latest_run_id }} |
        Campaigns: {{ stats.latest_campaigns }} |
        Customer: {{ stats.latest_customer_id }}
      </div>
    {% endif %}

    <h2>Runs per 5 Minutes (latest buckets)</h2>
    <div class="chart">
      {% if five_min_series.points %}
        <svg viewBox="0 0 {{ five_min_series.width }} {{ five_min_series.height }}">
          <polyline class="line" points="{{ five_min_series.points }}" />
          {% for dot in five_min_series.dots %}
            <circle class="dot" cx="{{ dot.x }}" cy="{{ dot.y }}" r="3">
              <title>{{ dot.label }}: {{ dot.count }}</title>
            </circle>
          {% endfor %}
        </svg>
        <div class="meta">
          {{ five_min_series.start_label }} to {{ five_min_series.end_label }} |
          max {{ five_min_series.max_value }}
        </div>
      {% else %}
        <div class="meta">No data</div>
      {% endif %}
    </div>

    <h2>Runs per Hour (latest buckets)</h2>
    <div class="chart">
      {% if hourly_series.points %}
        <svg viewBox="0 0 {{ hourly_series.width }} {{ hourly_series.height }}">
          <polyline class="line" points="{{ hourly_series.points }}" />
          {% for dot in hourly_series.dots %}
            <circle class="dot" cx="{{ dot.x }}" cy="{{ dot.y }}" r="3">
              <title>{{ dot.label }}: {{ dot.count }}</title>
            </circle>
          {% endfor %}
        </svg>
        <div class="meta">
          {{ hourly_series.start_label }} to {{ hourly_series.end_label }} |
          max {{ hourly_series.max_value }}
        </div>
      {% else %}
        <div class="meta">No data</div>
      {% endif %}
    </div>

    <h2>Runs per Day</h2>
    <div class="chart">
      {% if daily_series.points %}
        <svg viewBox="0 0 {{ daily_series.width }} {{ daily_series.height }}">
          <polyline class="line" points="{{ daily_series.points }}" />
          {% for dot in daily_series.dots %}
            <circle class="dot" cx="{{ dot.x }}" cy="{{ dot.y }}" r="3">
              <title>{{ dot.label }}: {{ dot.count }}</title>
            </circle>
          {% endfor %}
        </svg>
        <div class="meta">
          {{ daily_series.start_label }} to {{ daily_series.end_label }} |
          max {{ daily_series.max_value }}
        </div>
      {% else %}
        <div class="meta">No data</div>
      {% endif %}
    </div>

    <h2>Runs</h2>
    <div class="pager">
      <div>Page {{ page }} / {{ total_pages }}</div>
      {% if prev_page %}
        <a href="/history?page={{ prev_page }}&page_size={{ page_size }}&auto_refresh={{ 1 if auto_refresh else 0 }}{% if run_id %}&run_id={{ run_id }}{% endif %}">Prev</a>
      {% endif %}
      {% if next_page %}
        <a href="/history?page={{ next_page }}&page_size={{ page_size }}&auto_refresh={{ 1 if auto_refresh else 0 }}{% if run_id %}&run_id={{ run_id }}{% endif %}">Next</a>
      {% endif %}
    </div>
    <table>
      <thead>
        <tr>
          <th>run_id</th>
          <th>fetched_at</th>
          <th>customer_id</th>
          <th>days</th>
          <th>campaigns</th>
        </tr>
      </thead>
      <tbody>
        {% for run in runs %}
          <tr>
            <td><a href="/history?run_id={{ run.id }}&page={{ page }}&page_size={{ page_size }}&auto_refresh={{ 1 if auto_refresh else 0 }}">{{ run.id }}</a></td>
            <td>{{ run.fetched_at }}</td>
            <td>{{ run.customer_id }}</td>
            <td>{{ run.days }}</td>
            <td>{{ run.campaign_count }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>

    {% if rows %}
      <h2>Run {{ run_id }}</h2>
      <table>
        <thead>
          <tr>
            {% for header in headers %}
              <th>{{ header }}</th>
            {% endfor %}
          </tr>
        </thead>
        <tbody>
          {% for row in rows %}
            <tr class="{{ 'total-row' if row.campaign_name == 'TOTAL' else '' }}">
              <td>{{ row.campaign_id }}</td>
              <td>{{ row.campaign_name }}</td>
              <td>{{ row.status }}</td>
              <td>{{ row.impressions }}</td>
              <td>{{ row.clicks }}</td>
              <td>{{ row.ctr_percent }}</td>
              <td>{{ row.search_impression_share }}</td>
              <td>{{ row.average_cpc }}</td>
              <td>{{ row.cost_per_click }}</td>
              <td>{{ row.cost }}</td>
              <td>{{ row.conversions }}</td>
              <td>{{ row.conversion_value }}</td>
              <td>{{ row.cost_per_conversion }}</td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    {% endif %}
  </body>
</html>
"""


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              fetched_at TEXT NOT NULL,
              customer_id TEXT NOT NULL,
              days INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id INTEGER NOT NULL,
              campaign_id TEXT,
              campaign_name TEXT,
              status TEXT,
              impressions INTEGER,
              clicks INTEGER,
              ctr_percent REAL,
              search_impression_share REAL,
              average_cpc REAL,
              cost_per_click REAL,
              cost REAL,
              conversions REAL,
              conversion_value REAL,
              cost_per_conversion REAL,
              FOREIGN KEY (run_id) REFERENCES runs(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_fetched_at ON runs(fetched_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_campaigns_run_id ON campaigns(run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_campaigns_name ON campaigns(campaign_name)"
        )


def _persist_run(customer_id: str, days: int, rows: List[ads.CampaignRow]) -> int:
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (fetched_at, customer_id, days) VALUES (?, ?, ?)",
            (fetched_at, customer_id, days),
        )
        run_id = cur.lastrowid
        for row in rows:
            conn.execute(
                """
                INSERT INTO campaigns (
                  run_id, campaign_id, campaign_name, status,
                  impressions, clicks, ctr_percent, search_impression_share,
                  average_cpc, cost_per_click, cost, conversions,
                  conversion_value, cost_per_conversion
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row.campaign_id,
                    row.campaign_name,
                    row.status,
                    row.impressions,
                    row.clicks,
                    row.ctr_percent,
                    row.search_impression_share,
                    row.average_cpc,
                    row.cost_per_click,
                    row.cost,
                    row.conversions,
                    row.conversion_value,
                    row.cost_per_conversion,
                ),
            )
    print(
        f"[scheduler] run_id={run_id} fetched_at={fetched_at} "
        f"customer_id={customer_id} days={days} rows={len(rows)}"
    )
    return int(run_id)


def _count_runs() -> int:
    with _get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM runs").fetchone()
        return int(row["cnt"]) if row else 0


def _fetch_runs_page(page: int, page_size: int) -> List[sqlite3.Row]:
    offset = (page - 1) * page_size
    with _get_conn() as conn:
        return conn.execute(
            """
            SELECT runs.id, runs.fetched_at, runs.customer_id, runs.days,
                   SUM(CASE WHEN campaigns.campaign_name = 'TOTAL' THEN 0 ELSE 1 END)
                     AS campaign_count
            FROM runs
            LEFT JOIN campaigns ON campaigns.run_id = runs.id
            GROUP BY runs.id
            ORDER BY runs.id DESC
            LIMIT ? OFFSET ?
            """,
            (page_size, offset),
        ).fetchall()


def _fetch_history_stats() -> dict:
    with _get_conn() as conn:
        stats = conn.execute(
            """
            SELECT COUNT(*) AS total_runs,
                   MAX(fetched_at) AS last_run
            FROM runs
            """
        ).fetchone()
        total_campaigns = conn.execute(
            """
            SELECT COUNT(*) AS total_campaigns
            FROM campaigns
            WHERE campaign_name != 'TOTAL'
            """
        ).fetchone()
        latest_run = conn.execute(
            """
            SELECT id, customer_id
            FROM runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_campaigns = None
        if latest_run:
            latest_campaigns = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM campaigns
                WHERE run_id = ? AND campaign_name != 'TOTAL'
                """,
                (latest_run["id"],),
            ).fetchone()

    return {
        "total_runs": stats["total_runs"] if stats else 0,
        "last_run": stats["last_run"] if stats else None,
        "total_campaigns": total_campaigns["total_campaigns"] if total_campaigns else 0,
        "latest_run_id": latest_run["id"] if latest_run else None,
        "latest_customer_id": latest_run["customer_id"] if latest_run else None,
        "latest_campaigns": latest_campaigns["cnt"] if latest_campaigns else 0,
    }


def _fetch_run_rows(run_id: int) -> List[sqlite3.Row]:
    with _get_conn() as conn:
        return conn.execute(
            """
            SELECT campaign_id, campaign_name, status, impressions, clicks,
                   ctr_percent, search_impression_share, average_cpc,
                   cost_per_click, cost, conversions, conversion_value,
                   cost_per_conversion
            FROM campaigns
            WHERE run_id = ?
            ORDER BY id ASC
            """,
            (run_id,),
        ).fetchall()


def _fetch_runs_per_day(limit_days: int = 30) -> List[sqlite3.Row]:
    with _get_conn() as conn:
        return conn.execute(
            """
            SELECT substr(fetched_at, 1, 10) AS day, COUNT(*) AS runs
            FROM runs
            GROUP BY day
            ORDER BY day DESC
            LIMIT ?
            """,
            (limit_days,),
        ).fetchall()


def _fetch_runs_per_hour(limit_hours: int = 24) -> List[sqlite3.Row]:
    with _get_conn() as conn:
        return conn.execute(
            """
            SELECT substr(fetched_at, 1, 13) AS hour, COUNT(*) AS runs
            FROM runs
            GROUP BY hour
            ORDER BY hour DESC
            LIMIT ?
            """,
            (limit_hours,),
        ).fetchall()


def _fetch_runs_per_5min(limit_buckets: int = 72) -> List[sqlite3.Row]:
    with _get_conn() as conn:
        return conn.execute(
            """
            SELECT
              substr(fetched_at, 1, 14) ||
              printf('%02d', (CAST(substr(fetched_at, 15, 2) AS INTEGER) / 5) * 5)
              AS bucket,
              COUNT(*) AS runs
            FROM runs
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT ?
            """,
            (limit_buckets,),
        ).fetchall()


def _build_line_series(
    rows: List[sqlite3.Row],
    label_key: str,
    count_key: str = "runs",
    width: int = 900,
    height: int = 140,
    padding: int = 10,
) -> dict:
    if not rows:
        return {
            "points": "",
            "dots": [],
            "width": width,
            "height": height,
            "start_label": None,
            "end_label": None,
            "max_value": 0,
        }

    max_count = max(row[count_key] for row in rows) or 1
    count = len(rows)
    span_x = max(width - padding * 2, 1)
    span_y = max(height - padding * 2, 1)

    dots = []
    for idx, row in enumerate(rows):
        x = padding + (span_x * idx / (count - 1 if count > 1 else 1))
        y = padding + (span_y * (1 - (row[count_key] / max_count)))
        dots.append(
            {
                "x": round(x, 2),
                "y": round(y, 2),
                "label": row[label_key],
                "count": row[count_key],
            }
        )

    points = " ".join(f"{dot['x']},{dot['y']}" for dot in dots)
    return {
        "points": points,
        "dots": dots,
        "width": width,
        "height": height,
        "start_label": rows[0][label_key],
        "end_label": rows[-1][label_key],
        "max_value": max_count,
    }


def _build_client() -> ads.GoogleAdsClient:
    args = SimpleNamespace(
        google_ads_yaml=None,
        env_file=None,
        developer_token=None,
        client_id=None,
        client_secret=None,
        refresh_token=None,
        login_customer_id=None,
    )
    return ads._build_client(args)


def _load_env_if_present() -> None:
    args = SimpleNamespace(env_file=None)
    env_file = ads._resolve_env_file(args)
    if env_file:
        ads._load_env_file(env_file)


def _load_scheduler_config() -> None:
    global SCHEDULER_INTERVAL_SECONDS, SCHEDULER_DAYS, SCHEDULER_ENABLED
    SCHEDULER_INTERVAL_SECONDS = int(
        os.getenv("GOOGLE_ADS_POLL_INTERVAL_SECONDS", "60")
    )
    SCHEDULER_DAYS = int(os.getenv("GOOGLE_ADS_HISTORY_DAYS", "30"))
    SCHEDULER_ENABLED = os.getenv("GOOGLE_ADS_SCHEDULER_ENABLED", "1") == "1"


def _get_default_customer_id() -> str | None:
    return os.getenv("GOOGLE_ADS_DEFAULT_CUSTOMER_ID")


def _scheduler_is_enabled() -> bool:
    return SCHEDULER_ENABLED or _scheduler_forced_enabled


def _stop_scheduler() -> None:
    global _scheduler_started, _scheduler_stop_event, _scheduler_backoff_until
    if not _scheduler_started:
        return
    _scheduler_stop_event.set()
    _scheduler_started = False
    _scheduler_backoff_until = None
    _scheduler_stop_event = threading.Event()


def _run_once(customer_id: str, days: int) -> None:
    client = _build_client()
    rows: List[ads.CampaignRow] = ads._fetch_campaigns(client, customer_id, days)
    rows = ads._rows_with_total(rows)
    _persist_run(customer_id, days, rows)


def _scheduler_loop() -> None:
    while not _scheduler_stop_event.is_set():
        try:
            if not _scheduler_is_enabled():
                _scheduler_stop_event.wait(1)
                continue
            if _scheduler_backoff_until and datetime.now() < _scheduler_backoff_until:
                _scheduler_stop_event.wait(5)
                continue
            customer_id = _get_default_customer_id()
            if customer_id:
                _run_once(customer_id, SCHEDULER_DAYS)
        except Exception as ex:
            _apply_backoff(ex)
            print(f"[scheduler] error={ex}")
        _scheduler_stop_event.wait(SCHEDULER_INTERVAL_SECONDS)


def _start_scheduler() -> None:
    global _scheduler_started
    _load_env_if_present()
    _load_scheduler_config()
    if _scheduler_started or not _scheduler_is_enabled():
        return
    _init_db()
    thread = threading.Thread(target=_scheduler_loop, daemon=True)
    thread.start()
    _scheduler_started = True


def _apply_backoff(ex: Exception) -> None:
    global _scheduler_backoff_until
    message = str(ex)
    if "Resource has been exhausted" not in message and "429" not in message:
        return
    match = re.search(r"Retry in (\\d+) seconds", message)
    seconds = int(match.group(1)) if match else 3600
    _scheduler_backoff_until = datetime.now() + timedelta(seconds=seconds)
    print(
        f"[scheduler] backoff seconds={seconds} "
        f"until={_scheduler_backoff_until.strftime('%Y-%m-%d %H:%M:%S')}"
    )


@app.route("/", methods=["GET"])
def index():
    _start_scheduler()
    _load_env_if_present()
    _load_scheduler_config()
    customer_id = request.args.get("customer_id") or _get_default_customer_id()
    days = int(request.args.get("days", SCHEDULER_DAYS))
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = request.args.get("message")
    scheduler_status = "running" if _scheduler_started else "stopped"
    backoff_until = (
        _scheduler_backoff_until.strftime("%Y-%m-%d %H:%M:%S")
        if _scheduler_backoff_until
        else None
    )

    if not customer_id:
        return render_template_string(
            TEMPLATE,
            customer_id=customer_id,
            days=days,
            fetched_at=fetched_at,
            headers=[],
            rows=[],
            error=None,
            message=message,
            scheduler_status=scheduler_status,
            backoff_until=backoff_until,
        )

    try:
        client = _build_client()
        rows: List[ads.CampaignRow] = ads._fetch_campaigns(client, customer_id, days)
        rows = ads._rows_with_total(rows)
        headers = [
            "campaign_id",
            "campaign_name",
            "status",
            "impressions",
            "clicks",
            "ctr_percent",
            "search_impression_share",
            "average_cpc",
            "cost_per_click",
            "cost",
            "conversions",
            "conversion_value",
            "cost_per_conversion",
        ]
        return render_template_string(
            TEMPLATE,
            customer_id=customer_id,
            days=days,
            fetched_at=fetched_at,
            headers=headers,
            rows=rows,
            error=None,
            message=message,
            scheduler_status=scheduler_status,
            backoff_until=backoff_until,
        )
    except Exception as ex:
        return render_template_string(
            TEMPLATE,
            customer_id=customer_id,
            days=days,
            fetched_at=fetched_at,
            headers=[],
            rows=[],
            error=str(ex),
            message=message,
            scheduler_status=scheduler_status,
            backoff_until=backoff_until,
        )


@app.route("/control", methods=["POST"])
def control():
    global _scheduler_forced_enabled
    _load_env_if_present()
    _load_scheduler_config()
    action = request.form.get("action")
    customer_id = request.form.get("customer_id") or _get_default_customer_id()
    message = None

    if action == "start":
        _scheduler_forced_enabled = True
        _start_scheduler()
        message = "Scheduler started"
    elif action == "stop":
        _scheduler_forced_enabled = False
        _stop_scheduler()
        message = "Scheduler stopped"
    elif action == "run_once":
        if not customer_id:
            message = "Missing customer_id"
        else:
            _run_once(customer_id, SCHEDULER_DAYS)
            message = "Run complete"
    else:
        message = "Unknown action"

    return redirect(url_for("index", customer_id=customer_id, message=message))


@app.route("/history", methods=["GET"])
def history():
    _start_scheduler()
    _init_db()
    auto_refresh_raw = request.args.get("auto_refresh", "1")
    auto_refresh = auto_refresh_raw != "0"
    page_raw = request.args.get("page", "1")
    page_size_raw = request.args.get("page_size", "100")
    try:
        page = max(int(page_raw), 1)
    except ValueError:
        page = 1
    try:
        page_size = int(page_size_raw)
    except ValueError:
        page_size = 100
    page_size = min(max(page_size, 20), 500)

    total_runs = _count_runs()
    total_pages = max((total_runs + page_size - 1) // page_size, 1)
    if page > total_pages:
        page = total_pages

    runs = _fetch_runs_page(page, page_size)
    stats = _fetch_history_stats()
    daily_raw = _fetch_runs_per_day()
    hourly_raw = _fetch_runs_per_hour()
    five_min_raw = _fetch_runs_per_5min()
    run_id_raw = request.args.get("run_id")
    rows: List[sqlite3.Row] = []
    run_id: Optional[int] = None
    if run_id_raw:
        try:
            run_id = int(run_id_raw)
            rows = _fetch_run_rows(run_id)
        except ValueError:
            run_id = None

    headers = [
        "campaign_id",
        "campaign_name",
        "status",
        "impressions",
        "clicks",
        "ctr_percent",
        "search_impression_share",
        "average_cpc",
        "cost_per_click",
        "cost",
        "conversions",
        "conversion_value",
        "cost_per_conversion",
    ]
    daily_raw = list(reversed(daily_raw))
    hourly_raw = list(reversed(hourly_raw))
    five_min_raw = list(reversed(five_min_raw))
    daily_series = _build_line_series(daily_raw, "day")
    hourly_series = _build_line_series(hourly_raw, "hour")
    five_min_series = _build_line_series(five_min_raw, "bucket")
    return render_template_string(
        HISTORY_TEMPLATE,
        runs=runs,
        run_id=run_id,
        rows=rows,
        headers=headers,
        stats=stats,
        refresh_seconds=SCHEDULER_INTERVAL_SECONDS,
        auto_refresh=auto_refresh,
        daily_series=daily_series,
        hourly_series=hourly_series,
        five_min_series=five_min_series,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        prev_page=(page - 1) if page > 1 else None,
        next_page=(page + 1) if page < total_pages else None,
    )


if __name__ == "__main__":
    _start_scheduler()
    app.run(debug=False)
