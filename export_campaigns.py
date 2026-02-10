from __future__ import annotations

import argparse
import csv
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


@dataclass
class CampaignRow:
    campaign_id: int
    campaign_name: str
    status: str
    impressions: int
    clicks: int
    ctr_percent: Optional[float]
    search_impression_share: Optional[float]
    average_cpc: Optional[float]
    cost_per_click: Optional[float]
    cost: Optional[float]
    conversions: Optional[float]
    conversion_value: Optional[float]
    cost_per_conversion: Optional[float]


def _normalize_customer_id(value: str) -> str:
    return value.replace("-", "").strip()


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
    env_file = args.env_file or os.getenv("GOOGLE_ADS_ENV_FILE")
    if env_file:
        return env_file
    default_env = os.path.join(os.getcwd(), ".env")
    if os.path.exists(default_env):
        return default_env
    return None


def _micros_to_currency(micros: Optional[float]) -> Optional[float]:
    if micros is None:
        return None
    return round(micros / 1_000_000, 2)


def _ratio_to_percent(ratio: Optional[float]) -> Optional[float]:
    if ratio is None:
        return None
    return round(float(ratio) * 100, 2)


def _ratio_raw(ratio: Optional[float]) -> Optional[float]:
    if ratio is None:
        return None
    return round(float(ratio), 4)


def _round_float(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _date_range_clause(days: int) -> str:
    end = date.today()
    start = end - timedelta(days=days - 1)
    return f"segments.date BETWEEN '{start}' AND '{end}'"


def _build_client(args: argparse.Namespace) -> GoogleAdsClient:
    if args.google_ads_yaml:
        return GoogleAdsClient.load_from_storage(args.google_ads_yaml)

    env_file = _resolve_env_file(args)
    if env_file:
        _load_env_file(env_file)

    developer_token = args.developer_token or os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
    client_id = args.client_id or os.getenv("GOOGLE_ADS_CLIENT_ID")
    client_secret = args.client_secret or os.getenv("GOOGLE_ADS_CLIENT_SECRET")
    refresh_token = args.refresh_token or os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
    login_customer_id = args.login_customer_id or os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")

    missing = [
        name
        for name, value in [
            ("developer_token", developer_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("refresh_token", refresh_token),
            ("login_customer_id", login_customer_id),
        ]
        if not value
    ]
    if missing:
        raise ValueError(
            "Missing required credentials (args or env): " + ", ".join(missing)
        )

    config = {
        "developer_token": developer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "login_customer_id": _normalize_customer_id(login_customer_id),
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def _fetch_campaigns(
    client: GoogleAdsClient, customer_id: str, days: int
) -> List[CampaignRow]:
    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.search_impression_share,
          metrics.average_cpc,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_per_conversion
        FROM campaign
        WHERE {_date_range_clause(days)}
          AND campaign.status = ENABLED
          AND campaign.advertising_channel_type = SEARCH
        ORDER BY metrics.cost_micros DESC
    """.strip()

    service = client.get_service("GoogleAdsService")
    customer_id = _normalize_customer_id(customer_id)

    rows: List[CampaignRow] = []
    try:
        stream = service.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                metrics = row.metrics
                rows.append(
                    CampaignRow(
                        campaign_id=row.campaign.id,
                        campaign_name=row.campaign.name,
                        status=row.campaign.status.name,
                        impressions=int(metrics.impressions or 0),
                        clicks=int(metrics.clicks or 0),
                        ctr_percent=_ratio_to_percent(metrics.ctr),
                        search_impression_share=_ratio_raw(
                            metrics.search_impression_share
                        ),
                        average_cpc=_micros_to_currency(metrics.average_cpc),
                        cost_per_click=_micros_to_currency(metrics.average_cpc),
                        cost=_micros_to_currency(metrics.cost_micros),
                        conversions=_round_float(metrics.conversions, 2) or 0.0,
                        conversion_value=_round_float(metrics.conversions_value, 2),
                        cost_per_conversion=_micros_to_currency(
                            metrics.cost_per_conversion
                        ),
                    )
                )
    except GoogleAdsException as ex:
        raise RuntimeError(_format_google_ads_error(ex)) from ex

    return rows


def _format_google_ads_error(ex: GoogleAdsException) -> str:
    lines = [f"Request failed: {ex.error.code().name}"]
    for error in ex.failure.errors:
        lines.append(f"- {error.message}")
    return "\n".join(lines)


def _write_csv(path: str, rows: Iterable[CampaignRow]) -> None:
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

    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(
                [
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
                ]
            )


def _write_google_sheet(
    sheet_id: str,
    sheet_name: str,
    service_account_path: str,
    rows: Iterable[CampaignRow],
) -> None:
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        from gspread.exceptions import WorksheetNotFound
    except ImportError as ex:
        raise RuntimeError(
            "Missing Google Sheets dependencies. Install: "
            "pip install gspread google-auth"
        ) from ex

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(service_account_path, scopes=scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")

    data = [
        [
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
    ]

    for row in rows:
        data.append(
            [
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
            ]
        )

    worksheet.clear()
    worksheet.update("A1", data, value_input_option="USER_ENTERED")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export Google Ads campaign metrics to CSV and Google Sheets."
    )
    parser.add_argument("--customer-id", required=True, help="Target client account ID")
    parser.add_argument(
        "--login-customer-id",
        help="MCC account ID (required for manager access)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to include (default: 30)",
    )
    parser.add_argument(
        "--output",
        default="campaigns_last_30_days.csv",
        help="CSV output file path",
    )
    parser.add_argument(
        "--output-xlsx",
        help="XLSX output file path (optional)",
    )
    parser.add_argument(
        "--google-ads-yaml",
        help="Path to google-ads.yaml (optional)",
    )
    parser.add_argument(
        "--env-file",
        help="Path to .env file with GOOGLE_ADS_* credentials (optional)",
    )

    parser.add_argument("--developer-token")
    parser.add_argument("--client-id")
    parser.add_argument("--client-secret")
    parser.add_argument("--refresh-token")

    parser.add_argument("--sheet-id", help="Google Sheet ID")
    parser.add_argument(
        "--sheet-name",
        default="Campaigns",
        help="Worksheet name (default: Campaigns)",
    )
    parser.add_argument(
        "--service-account",
        help="Path to Google service account JSON (for Sheets)",
    )

    return parser


def _write_xlsx(path: str, rows: Iterable[CampaignRow]) -> None:
    try:
        from openpyxl import Workbook
    except ImportError as ex:
        raise RuntimeError(
            "Missing Excel dependency. Install: pip install openpyxl"
        ) from ex

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

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Campaigns"
    sheet.append(headers)
    for row in rows:
        sheet.append(
            [
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
            ]
        )

    workbook.save(path)


def _build_total_row(rows: List[CampaignRow]) -> CampaignRow:
    total_impressions = sum(row.impressions for row in rows)
    total_clicks = sum(row.clicks for row in rows)
    total_cost = round(sum(row.cost or 0.0 for row in rows), 2)
    total_conversions = round(sum(row.conversions or 0.0 for row in rows), 2)
    total_conversion_value = round(sum(row.conversion_value or 0.0 for row in rows), 2)

    ctr_percent = (
        _ratio_to_percent(total_clicks / total_impressions)
        if total_impressions
        else None
    )
    average_cpc = round(total_cost / total_clicks, 2) if total_clicks else None
    cost_per_click = average_cpc
    cost_per_conversion = (
        round(total_cost / total_conversions, 2) if total_conversions else None
    )

    return CampaignRow(
        campaign_id=0,
        campaign_name="TOTAL",
        status="",
        impressions=total_impressions,
        clicks=total_clicks,
        ctr_percent=ctr_percent,
        search_impression_share=None,
        average_cpc=average_cpc,
        cost_per_click=cost_per_click,
        cost=total_cost,
        conversions=total_conversions,
        conversion_value=total_conversion_value,
        cost_per_conversion=cost_per_conversion,
    )


def _rows_with_total(rows: List[CampaignRow]) -> List[CampaignRow]:
    return rows + [_build_total_row(rows)]


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    env_file = _resolve_env_file(args)
    if env_file:
        _load_env_file(env_file)

    if not args.login_customer_id and not args.google_ads_yaml:
        args.login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")

    if not args.login_customer_id and not args.google_ads_yaml:
        raise SystemExit(
            "Missing --login-customer-id (required for MCC access) or google-ads.yaml"
        )

    client = _build_client(args)
    rows = _fetch_campaigns(client, args.customer_id, args.days)
    rows_with_total = _rows_with_total(rows)

    _write_csv(args.output, rows_with_total)
    print(
        f"CSV written: {args.output} ({len(rows)} campaigns + total row)"
    )

    if args.output_xlsx:
        _write_xlsx(args.output_xlsx, rows_with_total)
        print(
            f"XLSX written: {args.output_xlsx} ({len(rows)} campaigns + total row)"
        )

    if args.sheet_id:
        if not args.service_account:
            raise SystemExit("--service-account is required when using --sheet-id")
        _write_google_sheet(
            args.sheet_id, args.sheet_name, args.service_account, rows_with_total
        )
        print(f"Google Sheet updated: {args.sheet_id} ({args.sheet_name})")


if __name__ == "__main__":
    main()
