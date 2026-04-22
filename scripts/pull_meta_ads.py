"""
Pull Meta Ads performance data for ASH Cooling & Heating campaigns.
Outputs structured JSON for analysis by the monitoring agent.

Usage:
    python pull_meta_ads.py                    # Last 7 days
    python pull_meta_ads.py --days 14          # Last 14 days
    python pull_meta_ads.py --campaign-id 123  # Specific campaign
"""

import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path


def load_api_key():
    """Load Meta access token from api-keys.env"""
    env_path = Path.home() / ".claude" / "config" / "api-keys.env"
    if not env_path.exists():
        print(f"Error: {env_path} not found", file=sys.stderr)
        sys.exit(1)

    keys = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            keys[k.strip()] = v.strip()

    token = keys.get("META_ADS_ACCESS_TOKEN")
    account_id = keys.get("META_AD_ACCOUNT_ID")

    if not token or not account_id:
        print("Error: META_ADS_ACCESS_TOKEN or META_AD_ACCOUNT_ID not found", file=sys.stderr)
        sys.exit(1)

    return token, account_id


def api_get(endpoint, params, token):
    """Make a GET request to the Meta Graph API."""
    params["access_token"] = token
    url = f"https://graph.facebook.com/v21.0/{endpoint}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"API Error {e.code}: {error_body}", file=sys.stderr)
        return None


def pull_campaign_insights(token, account_id, days=7, campaign_id=None):
    """Pull campaign-level insights."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    if campaign_id:
        endpoint = f"{campaign_id}/insights"
    else:
        endpoint = f"{account_id}/insights"

    params = {
        "fields": "campaign_name,campaign_id,spend,impressions,reach,frequency,clicks,ctr,cpc,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "campaign",
        "filtering": json.dumps([{"field": "campaign.delivery_info", "operator": "IN", "value": ["active", "inactive", "completed"]}]),
        "limit": 100,
    }

    return api_get(endpoint, params, token)


def pull_ad_insights(token, account_id, days=7, campaign_id=None):
    """Pull ad-level insights."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    params = {
        "fields": "ad_name,ad_id,adset_name,campaign_name,campaign_id,spend,impressions,reach,frequency,clicks,ctr,cpc,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "ad",
        "limit": 500,
    }

    if campaign_id:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}])

    return api_get(f"{account_id}/insights", params, token)


def pull_daily_insights(token, account_id, days=7, campaign_id=None):
    """Pull daily breakdown for trend analysis."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    params = {
        "fields": "campaign_name,campaign_id,spend,impressions,reach,frequency,clicks,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "level": "campaign",
        "limit": 500,
    }

    if campaign_id:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}])

    return api_get(f"{account_id}/insights", params, token)


def extract_leads_and_cpl(row):
    """Extract lead count and CPL from actions/cost_per_action_type.

    NOTE: The insights API's 'lead' action_type does NOT reliably include
    offline contact events from the offline dataset. Offline attribution
    (the 'Contacts' column in Ads Manager) appears to use statistical
    modeling and is not exposed in the actions array reliably.

    For now, use 'lead' which represents website form fills.
    To get offline contact attribution, query the offline dataset directly
    or use a separate attribution endpoint.
    """
    leads = 0
    cpl = 0

    for a in row.get("actions", []):
        if a.get("action_type") == "lead":
            leads = int(a.get("value", 0))
            break

    for c in row.get("cost_per_action_type", []):
        if c.get("action_type") == "lead":
            cpl = float(c.get("value", 0))
            break

    return leads, cpl


def format_report(campaign_data, ad_data, daily_data, days):
    """Format all data into a structured report dict."""
    report = {
        "generated_at": datetime.now().isoformat(),
        "period": f"Last {days} days",
        "campaigns": [],
        "ads": [],
        "daily_trend": [],
    }

    # Campaign level
    for row in (campaign_data or {}).get("data", []):
        leads, cpl = extract_leads_and_cpl(row)
        report["campaigns"].append({
            "name": row.get("campaign_name"),
            "id": row.get("campaign_id"),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "reach": int(row.get("reach", 0)),
            "frequency": float(row.get("frequency", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "cpc": float(row.get("cpc", 0)),
            "leads": leads,
            "cpl": cpl,
        })

    # Ad level
    for row in (ad_data or {}).get("data", []):
        leads, cpl = extract_leads_and_cpl(row)
        report["ads"].append({
            "name": row.get("ad_name"),
            "id": row.get("ad_id"),
            "adset": row.get("adset_name"),
            "campaign": row.get("campaign_name"),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "reach": int(row.get("reach", 0)),
            "frequency": float(row.get("frequency", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "cpc": float(row.get("cpc", 0)),
            "leads": leads,
            "cpl": cpl,
        })

    # Daily trend
    for row in (daily_data or {}).get("data", []):
        leads, cpl = extract_leads_and_cpl(row)
        report["daily_trend"].append({
            "date": row.get("date_start"),
            "campaign": row.get("campaign_name"),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "leads": leads,
            "cpl": cpl,
        })

    return report


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pull Meta Ads performance data")
    parser.add_argument("--days", type=int, default=7, help="Number of days to pull (default: 7)")
    parser.add_argument("--campaign-id", type=str, default=None, help="Filter to specific campaign ID")
    parser.add_argument("--output", "-o", type=str, default=None, help="Output file path (default: stdout)")
    args = parser.parse_args()

    token, account_id = load_api_key()

    campaign_data = pull_campaign_insights(token, account_id, args.days, args.campaign_id)
    ad_data = pull_ad_insights(token, account_id, args.days, args.campaign_id)
    daily_data = pull_daily_insights(token, account_id, args.days, args.campaign_id)

    report = format_report(campaign_data, ad_data, daily_data, args.days)

    output = json.dumps(report, indent=2)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Report saved to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
