"""
Full monthly revenue attribution report for ASH ads.

Pulls HouseCall Pro data for ALL leads tagged with campaign tags
(e.g. 'beat-a-quote-lp', 'summer-sale-2026'), matches against their jobs,
and produces a full revenue attribution report including:
  - Per-campaign lead count, matched jobs, and revenue
  - True CPL based on ad spend (from Meta API) vs. actual HCP jobs
  - Cross-reference with the offline audit log (logs/offline-leads.jsonl)
    to mark which leads also got Meta offline attribution
  - Per-lead detail table with contact, job status, revenue

Usage:
    python enrich_offline_leads.py                # Default: 60 day window
    python enrich_offline_leads.py --days 30      # Custom window
    python enrich_offline_leads.py --dry-run      # Preview without writing
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = REPO_DIR / "logs" / "offline-leads.jsonl"
VAULT_DIR = Path.home() / "Documents" / "Obsidian Vault" / "Business" / "HVAC Lead Gen" / "Clients" / "Ash Cooling & Heating" / "Monthly Reports"
HCP_BASE_URL = "https://api.housecallpro.com"

# Map campaign tag in HCP → Meta campaign name
# Extend this when new campaigns launch
CAMPAIGN_TAG_MAP = {
    "beat-a-quote-lp": "Beat A Quote - New - 3/26",
    "summer-sale-2026": "Summer Sale - LEADS - 4/26",
    "blowout-pack-unit-lp": "Pack Unit Blowout Offer - LEADS - 2/26",
    "valentines-sale": "Valentines Offer - LEADS - 2/26",
}


def load_api_keys():
    env_path = Path.home() / ".claude" / "config" / "api-keys.env"
    keys = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            keys[k.strip()] = v.strip()
    return keys


def normalize_phone(phone):
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def normalize_email(email):
    return (email or "").strip().lower()


def hcp_api_get(path, api_key, params=None):
    """Single HCP API GET."""
    url = f"{HCP_BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Token {api_key}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"HCP API error {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"HCP API exception: {e}", file=sys.stderr)
        return {}


def hcp_paginate(path, api_key, key, since_date=None, date_field="created_at"):
    """Paginate through a HCP endpoint that returns a list."""
    items = []
    page = 1
    while True:
        data = hcp_api_get(path, api_key, {"page": page, "page_size": 100})
        page_items = data.get(key, [])
        if not page_items:
            break

        older = False
        for item in page_items:
            if since_date:
                d = item.get(date_field, "")
                if d:
                    try:
                        item_date = datetime.fromisoformat(d.replace("Z", "+00:00"))
                        if item_date < since_date:
                            older = True
                            continue
                    except ValueError:
                        pass
            items.append(item)

        total_pages = data.get("total_pages", 1)
        print(f"  {path} page {page}/{total_pages} ({len(page_items)} items, {len([i for i in page_items if i in items or True])} kept)", file=sys.stderr)

        if page >= total_pages or older:
            break
        page += 1
        time.sleep(0.1)

    return items


def hcp_get_all_customers(api_key, since_date=None):
    return hcp_paginate("/customers", api_key, "customers", since_date, "created_at")


def hcp_get_all_jobs(api_key, since_date=None):
    return hcp_paginate("/jobs", api_key, "jobs", since_date, "created_at")


def index_jobs_by_customer(jobs):
    """Group jobs by customer ID."""
    idx = defaultdict(list)
    for job in jobs:
        cid = (job.get("customer") or {}).get("id")
        if cid:
            idx[cid].append(job)
    return idx


def classify_customer(customer):
    """Return the campaign tag a customer belongs to, if any."""
    tags = customer.get("tags") or []
    for tag in tags:
        if tag in CAMPAIGN_TAG_MAP:
            return tag
    return None


def load_audit_log(days):
    """Load offline audit log entries within the time window."""
    if not LOG_PATH.exists():
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    entries = []

    for line in LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            ts = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
            if ts >= cutoff:
                entries.append(entry)
        except (json.JSONDecodeError, KeyError, ValueError):
            continue

    return entries


def build_audit_index(audit_entries):
    """Index audit entries by normalized phone and email."""
    by_phone = {}
    by_email = {}
    for entry in audit_entries:
        p = normalize_phone(entry.get("phone", ""))
        if p:
            by_phone[p] = entry
        e = normalize_email(entry.get("email", ""))
        if e:
            by_email[e] = entry
    return by_phone, by_email


def pull_meta_spend_by_campaign(token, account_id, days):
    """Query Meta insights API for per-campaign spend in the window."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "campaign_name,spend",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "campaign",
        "limit": 100,
        "access_token": token,
    }
    url = f"https://graph.facebook.com/v21.0/{account_id}/insights?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"Meta API error: {e}", file=sys.stderr)
        return {}

    return {row["campaign_name"]: float(row.get("spend", 0)) for row in data.get("data", [])}


def pull_latest_log():
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=REPO_DIR, check=True, capture_output=True, timeout=30)
        subprocess.run(["git", "checkout", "origin/main", "--", "logs/offline-leads.jsonl"], cwd=REPO_DIR, check=True, capture_output=True, timeout=30)
    except Exception as e:
        print(f"Warning: git pull failed: {e}", file=sys.stderr)


def build_monthly_report(campaign_data, audit_entries_total, days):
    today = datetime.now().strftime("%B %d, %Y")

    lines = [
        f"# ASH Ads Monthly Revenue Attribution — {today}",
        "",
        f"**Lookback window:** Last {days} days",
        f"**Campaigns covered:** {len(campaign_data)}",
        f"**Offline audit log entries in window:** {audit_entries_total}",
        "",
        "## Campaign Summary",
        "",
        "| Campaign | Leads | Jobs Scheduled | Total Revenue | Ad Spend | True CPL | Revenue/$ Spent |",
        "|----------|-------|----------------|---------------|----------|----------|-----------------|",
    ]

    grand_leads = 0
    grand_jobs = 0
    grand_revenue = 0.0
    grand_spend = 0.0

    for tag, data in sorted(campaign_data.items()):
        leads = len(data["customers"])
        jobs_scheduled = data["jobs_scheduled"]
        revenue = data["revenue"]
        spend = data["ad_spend"]
        true_cpl = (spend / leads) if leads > 0 else 0
        roas = (revenue / spend) if spend > 0 else 0

        grand_leads += leads
        grand_jobs += jobs_scheduled
        grand_revenue += revenue
        grand_spend += spend

        campaign_name = data["campaign_name"]
        lines.append(
            f"| {campaign_name} ({tag}) | {leads} | {jobs_scheduled} | ${revenue:,.2f} | ${spend:,.2f} | ${true_cpl:,.0f} | {roas:.2f}x |"
        )

    total_cpl = (grand_spend / grand_leads) if grand_leads > 0 else 0
    total_roas = (grand_revenue / grand_spend) if grand_spend > 0 else 0
    lines.append(
        f"| **TOTAL** | **{grand_leads}** | **{grand_jobs}** | **${grand_revenue:,.2f}** | **${grand_spend:,.2f}** | **${total_cpl:,.0f}** | **{total_roas:.2f}x** |"
    )
    lines.append("")

    # Per-campaign detail
    for tag, data in sorted(campaign_data.items()):
        lines.append(f"## {data['campaign_name']} ({tag})")
        lines.append("")
        lines.append(f"**Ad spend:** ${data['ad_spend']:,.2f}")
        lines.append(f"**Leads tagged in HCP:** {len(data['customers'])}")
        lines.append(f"**Scheduled jobs:** {data['jobs_scheduled']}")
        lines.append(f"**Total revenue:** ${data['revenue']:,.2f}")
        lines.append("")
        lines.append("| Name | Phone | Email | City | Created | Jobs | Revenue | Offline Match |")
        lines.append("|------|-------|-------|------|---------|------|---------|---------------|")

        for lead in sorted(data["leads"], key=lambda l: l["created_at"], reverse=True):
            name = f"{lead['first_name']} {lead['last_name']}".strip() or "—"
            phone = lead["phone"] or "—"
            email = lead["email"] or "—"
            city = lead["city"] or "—"
            created = lead["created_at"].split("T")[0] if lead["created_at"] else "—"
            jobs_count = len(lead["jobs"])
            revenue = sum((float(j.get("total_amount", 0) or 0) / 100) for j in lead["jobs"])
            revenue_str = f"${revenue:,.2f}" if revenue > 0 else "—"
            offline = "✓" if lead["in_offline_audit"] else "—"
            lines.append(f"| {name} | {phone} | {email} | {city} | {created} | {jobs_count} | {revenue_str} | {offline} |")
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-pull", action="store_true")
    args = parser.parse_args()

    keys = load_api_keys()
    hcp_key = keys.get("HCP_API_KEY")
    meta_token = keys.get("META_ADS_ACCESS_TOKEN")
    meta_account = keys.get("META_AD_ACCOUNT_ID")

    if not hcp_key:
        print("Error: HCP_API_KEY not found", file=sys.stderr)
        sys.exit(1)

    if not args.no_pull:
        pull_latest_log()

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)

    # Load offline audit entries (for cross-reference)
    audit_entries = load_audit_log(args.days)
    audit_by_phone, audit_by_email = build_audit_index(audit_entries)
    print(f"Loaded {len(audit_entries)} audit entries in last {args.days} days", file=sys.stderr)

    # Pull HCP customers and jobs
    print(f"Fetching HCP customers (last {args.days}d)...", file=sys.stderr)
    customers = hcp_get_all_customers(hcp_key, since_date=cutoff)
    print(f"Loaded {len(customers)} customers", file=sys.stderr)

    print(f"Fetching HCP jobs (last {args.days}d)...", file=sys.stderr)
    jobs = hcp_get_all_jobs(hcp_key, since_date=cutoff)
    print(f"Loaded {len(jobs)} jobs", file=sys.stderr)

    jobs_by_customer = index_jobs_by_customer(jobs)

    # Pull Meta spend per campaign
    meta_spend = {}
    if meta_token and meta_account:
        print(f"Fetching Meta ad spend per campaign...", file=sys.stderr)
        meta_spend = pull_meta_spend_by_campaign(meta_token, meta_account, args.days)
        print(f"Loaded spend for {len(meta_spend)} campaigns", file=sys.stderr)

    # Build per-campaign data
    campaign_data = {}
    for tag, campaign_name in CAMPAIGN_TAG_MAP.items():
        campaign_data[tag] = {
            "campaign_name": campaign_name,
            "customers": [],
            "leads": [],
            "jobs_scheduled": 0,
            "revenue": 0.0,
            "ad_spend": meta_spend.get(campaign_name, 0.0),
        }

    # Classify customers and attach jobs
    for customer in customers:
        tag = classify_customer(customer)
        if not tag or tag not in campaign_data:
            continue

        cid = customer.get("id")
        customer_jobs = jobs_by_customer.get(cid, [])

        # Only count "scheduled or completed" jobs (not just estimates)
        real_jobs = [
            j for j in customer_jobs
            if (j.get("schedule") or {}).get("scheduled_start") is not None
        ]

        revenue = sum((float(j.get("total_amount", 0) or 0) / 100) for j in real_jobs)

        # Cross-reference with offline audit log
        phone = normalize_phone(customer.get("mobile_number") or customer.get("home_number") or "")
        email = normalize_email(customer.get("email"))
        in_offline_audit = phone in audit_by_phone or email in audit_by_email

        lead_entry = {
            "customer_id": cid,
            "first_name": customer.get("first_name", ""),
            "last_name": customer.get("last_name", ""),
            "phone": customer.get("mobile_number") or customer.get("home_number") or "",
            "email": customer.get("email") or "",
            "city": (customer.get("addresses") or [{}])[0].get("city", "") if customer.get("addresses") else "",
            "created_at": customer.get("created_at", ""),
            "jobs": real_jobs,
            "in_offline_audit": in_offline_audit,
        }

        campaign_data[tag]["customers"].append(cid)
        campaign_data[tag]["leads"].append(lead_entry)
        campaign_data[tag]["jobs_scheduled"] += len(real_jobs)
        campaign_data[tag]["revenue"] += revenue

    # Strip empty campaigns
    campaign_data = {k: v for k, v in campaign_data.items() if v["customers"]}

    # Print summary
    print(f"\nResults:", file=sys.stderr)
    for tag, data in campaign_data.items():
        print(f"  {tag}: {len(data['customers'])} leads, {data['jobs_scheduled']} jobs, ${data['revenue']:,.2f} revenue, ${data['ad_spend']:,.2f} spend", file=sys.stderr)

    # Write report
    if not args.dry_run:
        report = build_monthly_report(campaign_data, len(audit_entries), args.days)
        VAULT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = VAULT_DIR / f"revenue-attribution-{datetime.now().strftime('%Y-%m-%d')}.md"
        report_path.write_text(report, encoding="utf-8")
        print(f"\nReport written to {report_path}", file=sys.stderr)
    else:
        print("\n(Dry run — no report written)", file=sys.stderr)


if __name__ == "__main__":
    main()
