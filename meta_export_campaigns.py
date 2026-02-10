from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional


@dataclass
class MetaCampaignRow:
    campaign_id: str
    campaign_name: str
    status: str
    objective: str
    impressions: int
    clicks: int
    inline_link_clicks: int
    unique_clicks: int
    reach: int
    frequency: Optional[float]
    ctr_percent: Optional[float]
    cpc: Optional[float]
    cpm: Optional[float]
    spend: Optional[float]
    conversions: Optional[float]
    conversion_value: Optional[float]


def _load_env_file(path: Optional[str]) -> None:
    if not path:
        return
    if not os.path.exists(path):
        raise FileNotFoundError(f"Env file not found: {path}")

    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if not value:
                continue
            os.environ.setdefault(key, value)


def _resolve_env_file(args: argparse.Namespace) -> Optional[str]:
    env_file = args.env_file or os.getenv("META_ENV_FILE")
    if env_file:
        return env_file
    default_env = os.path.join(os.getcwd(), ".env")
    if os.path.exists(default_env):
        return default_env
    return None


def _ensure_act_prefix(ad_account_id: str) -> str:
    ad_account_id = ad_account_id.strip()
    if not ad_account_id.startswith("act_"):
        return f"act_{ad_account_id}"
    return ad_account_id


def _date_range(days: int) -> Dict[str, str]:
    end = date.today()
    start = end - timedelta(days=days - 1)
    return {"since": start.isoformat(), "until": end.isoformat()}


def _to_int(value: Optional[str]) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def _to_float(value: Optional[str], digits: int = 2) -> Optional[float]:
    if value in (None, ""):
        return None
    return round(float(value), digits)


def _get_action_value(actions: Optional[List[dict]], action_type: str) -> float:
    if not actions:
        return 0.0
    for item in actions:
        if item.get("action_type") == action_type:
            try:
                return float(item.get("value", 0) or 0)
            except ValueError:
                return 0.0
    return 0.0


def _require_requests():
    try:
        import requests  # noqa: F401
    except ImportError as ex:
        raise RuntimeError("Missing dependency. Install: pip install requests") from ex


def _graph_get(version: str, path: str, params: Dict[str, str], access_token: str):
    import requests

    url = f"https://graph.facebook.com/{version}/{path}"
    params = dict(params)
    params["access_token"] = access_token
    response = requests.get(url, params=params, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(f"Graph API error: {data}")
    if "error" in data:
        raise RuntimeError(f"Graph API error: {data['error']}")
    return data


def _graph_get_url(url: str):
    import requests

    response = requests.get(url, timeout=30)
    data = response.json()
    if response.status_code >= 400:
        raise RuntimeError(f"Graph API error: {data}")
    if "error" in data:
        raise RuntimeError(f"Graph API error: {data['error']}")
    return data


def _paginate(version: str, path: str, params: Dict[str, str], access_token: str):
    data = _graph_get(version, path, params, access_token)
    while True:
        for item in data.get("data", []):
            yield item
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        data = _graph_get_url(next_url)


def _fetch_campaigns(
    version: str,
    ad_account_id: str,
    access_token: str,
) -> Dict[str, dict]:
    fields = "id,name,status,effective_status,objective"
    params = {"fields": fields, "limit": "200"}
    campaigns: Dict[str, dict] = {}
    for item in _paginate(version, f"{ad_account_id}/campaigns", params, access_token):
        campaigns[item["id"]] = item
    return campaigns


def _fetch_insights(
    version: str,
    ad_account_id: str,
    access_token: str,
    days: int,
) -> List[dict]:
    fields = ",".join(
        [
            "campaign_id",
            "campaign_name",
            "impressions",
            "clicks",
            "inline_link_clicks",
            "unique_clicks",
            "reach",
            "frequency",
            "ctr",
            "cpc",
            "cpm",
            "spend",
            "actions",
            "action_values",
        ]
    )

    params = {
        "level": "campaign",
        "fields": fields,
        "time_range": json.dumps(_date_range(days)),
        "limit": "200",
    }

    return list(_paginate(version, f"{ad_account_id}/insights", params, access_token))


def _merge_rows(
    campaigns: Dict[str, dict],
    insights: List[dict],
    conversion_action: str,
) -> List[MetaCampaignRow]:
    rows: List[MetaCampaignRow] = []
    for row in insights:
        campaign_id = row.get("campaign_id") or ""
        campaign_meta = campaigns.get(campaign_id, {})
        status = campaign_meta.get("effective_status") or campaign_meta.get("status") or ""
        objective = campaign_meta.get("objective") or ""
        conversions = _get_action_value(row.get("actions"), conversion_action)
        conversion_value = _get_action_value(
            row.get("action_values"), conversion_action
        )

        rows.append(
            MetaCampaignRow(
                campaign_id=campaign_id,
                campaign_name=row.get("campaign_name", ""),
                status=status,
                objective=objective,
                impressions=_to_int(row.get("impressions")),
                clicks=_to_int(row.get("clicks")),
                inline_link_clicks=_to_int(row.get("inline_link_clicks")),
                unique_clicks=_to_int(row.get("unique_clicks")),
                reach=_to_int(row.get("reach")),
                frequency=_to_float(row.get("frequency"), 2),
                ctr_percent=_to_float(row.get("ctr"), 2),
                cpc=_to_float(row.get("cpc"), 2),
                cpm=_to_float(row.get("cpm"), 2),
                spend=_to_float(row.get("spend"), 2),
                conversions=_to_float(str(conversions), 2),
                conversion_value=_to_float(str(conversion_value), 2),
            )
        )
    return rows


def _build_total_row(rows: List[MetaCampaignRow]) -> MetaCampaignRow:
    total_impressions = sum(row.impressions for row in rows)
    total_clicks = sum(row.clicks for row in rows)
    total_inline_link_clicks = sum(row.inline_link_clicks for row in rows)
    total_unique_clicks = sum(row.unique_clicks for row in rows)
    total_reach = sum(row.reach for row in rows)
    total_spend = round(sum(row.spend or 0.0 for row in rows), 2)
    total_conversions = round(sum(row.conversions or 0.0 for row in rows), 2)
    total_conversion_value = round(
        sum(row.conversion_value or 0.0 for row in rows), 2
    )

    ctr_percent = (
        round((total_clicks / total_impressions) * 100, 2)
        if total_impressions
        else None
    )
    cpc = round(total_spend / total_clicks, 2) if total_clicks else None
    cpm = round((total_spend / total_impressions) * 1000, 2) if total_impressions else None
    frequency = round(total_impressions / total_reach, 2) if total_reach else None

    return MetaCampaignRow(
        campaign_id="TOTAL",
        campaign_name="TOTAL",
        status="",
        objective="",
        impressions=total_impressions,
        clicks=total_clicks,
        inline_link_clicks=total_inline_link_clicks,
        unique_clicks=total_unique_clicks,
        reach=total_reach,
        frequency=frequency,
        ctr_percent=ctr_percent,
        cpc=cpc,
        cpm=cpm,
        spend=total_spend,
        conversions=total_conversions,
        conversion_value=total_conversion_value,
    )


def _write_csv(path: str, rows: Iterable[MetaCampaignRow]) -> None:
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

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [
                    row.campaign_id,
                    row.campaign_name,
                    row.status,
                    row.objective,
                    row.impressions,
                    row.clicks,
                    row.inline_link_clicks,
                    row.unique_clicks,
                    row.reach,
                    row.frequency,
                    row.ctr_percent,
                    row.cpc,
                    row.cpm,
                    row.spend,
                    row.conversions,
                    row.conversion_value,
                ]
            )


def _write_xlsx(path: str, rows: Iterable[MetaCampaignRow]) -> None:
    try:
        from openpyxl import Workbook
    except ImportError as ex:
        raise RuntimeError("Missing dependency. Install: pip install openpyxl") from ex

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

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Meta Campaigns"
    sheet.append(headers)
    for row in rows:
        sheet.append(
            [
                row.campaign_id,
                row.campaign_name,
                row.status,
                row.objective,
                row.impressions,
                row.clicks,
                row.inline_link_clicks,
                row.unique_clicks,
                row.reach,
                row.frequency,
                row.ctr_percent,
                row.cpc,
                row.cpm,
                row.spend,
                row.conversions,
                row.conversion_value,
            ]
        )

    workbook.save(path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Meta (Facebook) Ads campaign insights to CSV/XLSX."
    )
    parser.add_argument("--ad-account-id", help="Meta ad account ID (act_...)")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to include (default: 30)",
    )
    parser.add_argument(
        "--output",
        default="meta_campaigns_last_30_days.csv",
        help="CSV output file path",
    )
    parser.add_argument("--output-xlsx", help="XLSX output file path (optional)")
    parser.add_argument(
        "--conversion-action",
        default="offsite_conversion",
        help="Action type to use as conversions (default: offsite_conversion)",
    )
    parser.add_argument("--access-token", help="Meta access token")
    parser.add_argument(
        "--api-version",
        help="Graph API version (e.g., v22.0). If omitted, uses META_API_VERSION or v22.0.",
    )
    parser.add_argument(
        "--env-file",
        help="Path to .env file with META_* credentials (optional)",
    )
    return parser


def main() -> None:
    _require_requests()
    parser = _build_parser()
    args = parser.parse_args()

    env_file = _resolve_env_file(args)
    if env_file:
        _load_env_file(env_file)

    access_token = args.access_token or os.getenv("META_ACCESS_TOKEN")
    ad_account_id = args.ad_account_id or os.getenv("META_AD_ACCOUNT_ID")
    api_version = args.api_version or os.getenv("META_API_VERSION") or "v22.0"
    conversion_action = (
        args.conversion_action
        or os.getenv("META_CONVERSION_ACTION")
        or "offsite_conversion"
    )

    if not access_token:
        raise SystemExit("Missing META access token (use --access-token or META_ACCESS_TOKEN)")
    if not ad_account_id:
        raise SystemExit("Missing Meta ad account id (use --ad-account-id or META_AD_ACCOUNT_ID)")

    ad_account_id = _ensure_act_prefix(ad_account_id)

    campaigns = _fetch_campaigns(api_version, ad_account_id, access_token)
    insights = _fetch_insights(api_version, ad_account_id, access_token, args.days)
    rows = _merge_rows(campaigns, insights, conversion_action)
    rows_with_total = rows + [_build_total_row(rows)]

    _write_csv(args.output, rows_with_total)
    print(f"CSV written: {args.output} ({len(rows)} campaigns + total row)")

    if args.output_xlsx:
        _write_xlsx(args.output_xlsx, rows_with_total)
        print(f"XLSX written: {args.output_xlsx} ({len(rows)} campaigns + total row)")


if __name__ == "__main__":
    main()
