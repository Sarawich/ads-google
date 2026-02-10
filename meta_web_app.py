from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace
from typing import List

from flask import Flask, request, render_template_string

import meta_export_campaigns as meta


app = Flask(__name__)

TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Meta Ads Campaigns</title>
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
        background: #111827;
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
    </style>
  </head>
  <body>
    <h1>Meta Ads Campaigns</h1>
    <div class="meta">Fetched at {{ fetched_at }}</div>

    <form method="get">
      <div>
        <label>Ad Account ID</label><br/>
        <input name="ad_account_id" value="{{ ad_account_id or '' }}" placeholder="act_123..." />
      </div>
      <div>
        <label>Days</label><br/>
        <input name="days" type="number" min="1" max="365" value="{{ days }}" />
      </div>
      <div style="align-self: end;">
        <button type="submit">Refresh</button>
      </div>
    </form>

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
              <td>{{ row.objective }}</td>
              <td>{{ row.impressions }}</td>
              <td>{{ row.clicks }}</td>
              <td>{{ row.inline_link_clicks }}</td>
              <td>{{ row.unique_clicks }}</td>
              <td>{{ row.reach }}</td>
              <td>{{ row.frequency }}</td>
              <td>{{ row.ctr_percent }}</td>
              <td>{{ row.cpc }}</td>
              <td>{{ row.cpm }}</td>
              <td>{{ row.spend }}</td>
              <td>{{ row.conversions }}</td>
              <td>{{ row.conversion_value }}</td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    {% else %}
      <div class="meta">Enter an ad account ID to load campaigns.</div>
    {% endif %}
  </body>
</html>
"""


def _load_env_if_present() -> None:
    args = SimpleNamespace(env_file=None)
    env_file = meta._resolve_env_file(args)
    if env_file:
        meta._load_env_file(env_file)


def _default_ad_account_id() -> str | None:
    return os.getenv("META_AD_ACCOUNT_ID")


@app.route("/", methods=["GET"])
def index():
    _load_env_if_present()
    ad_account_id = request.args.get("ad_account_id") or _default_ad_account_id()
    days = int(request.args.get("days", 30))
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not ad_account_id:
        return render_template_string(
            TEMPLATE,
            ad_account_id=ad_account_id,
            days=days,
            fetched_at=fetched_at,
            headers=[],
            rows=[],
            error=None,
        )

    try:
        access_token = os.getenv("META_ACCESS_TOKEN")
        api_version = os.getenv("META_API_VERSION") or "v22.0"
        conversion_action = os.getenv("META_CONVERSION_ACTION") or "offsite_conversion"
        if not access_token:
            raise RuntimeError("Missing META_ACCESS_TOKEN (set it in .env)")

        ad_account_id = meta._ensure_act_prefix(ad_account_id)
        campaigns = meta._fetch_campaigns(api_version, ad_account_id, access_token)
        insights = meta._fetch_insights(api_version, ad_account_id, access_token, days)
        rows: List[meta.MetaCampaignRow] = meta._merge_rows(
            campaigns, insights, conversion_action
        )
        rows = rows + [meta._build_total_row(rows)]

        headers = [
            "campaign_id",
            "campaign_name",
            "status",
            "objective",
            "impressions",
            "clicks",
            "inline_link_clicks",
            "unique_clicks",
            "reach",
            "frequency",
            "ctr_percent",
            "cpc",
            "cpm",
            "spend",
            "conversions",
            "conversion_value",
        ]

        return render_template_string(
            TEMPLATE,
            ad_account_id=ad_account_id,
            days=days,
            fetched_at=fetched_at,
            headers=headers,
            rows=rows,
            error=None,
        )
    except Exception as ex:
        return render_template_string(
            TEMPLATE,
            ad_account_id=ad_account_id,
            days=days,
            fetched_at=fetched_at,
            headers=[],
            rows=[],
            error=str(ex),
        )


if __name__ == "__main__":
    app.run(debug=False)
