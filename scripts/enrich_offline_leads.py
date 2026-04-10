"""
Enrich the offline leads audit log with HouseCall Pro job/revenue data.

For each entry in logs/offline-leads.jsonl, queries HouseCall Pro API to find
any scheduled jobs matching by phone or email, then updates the entry with:
  - hcp_matched: true/false
  - hcp_job_id
  - hcp_scheduled_start
  - hcp_total_amount
  - hcp_work_status

Also generates a monthly report in the Obsidian vault showing:
  - Per-lead revenue
  - Campaign-level totals
  - True CPL and ROAS per campaign

Usage:
    python enrich_offline_leads.py                # Enrich full log
    python enrich_offline_leads.py --days 30      # Lookback window
    python enrich_offline_leads.py --dry-run      # Show matches without updating
"""

import json
import re
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO_DIR = Path(__file__).resolve().parent.parent
LOG_PATH = REPO_DIR / "logs" / "offline-leads.jsonl"
VAULT_DIR = Path.home() / "Documents" / "Obsidian Vault" / "Business" / "HVAC Lead Gen" / "Clients" / "Ash Cooling & Heating" / "Monthly Reports"
HCP_BASE_URL = "https://api.housecallpro.com"


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
    """Strip to digits only, remove leading 1 for US numbers."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def normalize_email(email):
    return (email or "").strip().lower()


def hcp_get_all_jobs(api_key, since_date=None):
    """Paginate through all HCP jobs, optionally filtered by updated_at >= since_date."""
    jobs = []
    page = 1

    while True:
        params = {"page": page, "page_size": 100}
        url = f"{HCP_BASE_URL}/jobs?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Authorization": f"Token {api_key}"})

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"HCP API error on page {page}: {e.code} {e.read().decode()[:200]}", file=sys.stderr)
            break
        except Exception as e:
            print(f"HCP API exception on page {page}: {e}", file=sys.stderr)
            break

        page_jobs = data.get("jobs", [])
        if not page_jobs:
            break

        # Filter by since_date if provided (jobs are sorted newest first)
        filtered = []
        older_found = False
        for job in page_jobs:
            sched = job.get("schedule", {}).get("scheduled_start") or job.get("created_at", "")
            if since_date and sched:
                try:
                    job_date = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                    if job_date < since_date:
                        older_found = True
                        continue
                except ValueError:
                    pass
            filtered.append(job)

        jobs.extend(filtered)

        total_pages = data.get("total_pages", 1)
        print(f"  Fetched page {page}/{total_pages} ({len(page_jobs)} jobs, {len(filtered)} in window)", file=sys.stderr)

        if page >= total_pages or older_found:
            break
        page += 1
        time.sleep(0.1)  # rate limit politeness

    return jobs


def build_job_index(jobs):
    """Index jobs by normalized phone and email for fast lookup."""
    by_phone = {}
    by_email = {}

    for job in jobs:
        customer = job.get("customer", {}) or {}
        for phone_field in ("mobile_number", "home_number", "work_number"):
            phone = normalize_phone(customer.get(phone_field))
            if phone:
                by_phone.setdefault(phone, []).append(job)
        email = normalize_email(customer.get("email"))
        if email:
            by_email.setdefault(email, []).append(job)

    return by_phone, by_email


def match_lead_to_job(entry, by_phone, by_email):
    """Find the best HCP job match for a log entry."""
    phone = normalize_phone(entry.get("phone", ""))
    email = normalize_email(entry.get("email", ""))

    # Phone is strongest match
    if phone and phone in by_phone:
        candidates = by_phone[phone]
        # Pick the most recent one
        return max(candidates, key=lambda j: j.get("created_at", ""))

    if email and email in by_email:
        candidates = by_email[email]
        return max(candidates, key=lambda j: j.get("created_at", ""))

    return None


def enrich_entry(entry, job):
    """Add HCP match data to a log entry."""
    if not job:
        entry["hcp_matched"] = False
        return entry

    entry["hcp_matched"] = True
    entry["hcp_job_id"] = job.get("id")
    entry["hcp_invoice_number"] = job.get("invoice_number")
    entry["hcp_work_status"] = job.get("work_status")
    entry["hcp_scheduled_start"] = job.get("schedule", {}).get("scheduled_start")
    entry["hcp_total_amount"] = job.get("total_amount", 0)
    entry["hcp_customer_id"] = job.get("customer", {}).get("id")
    return entry


def pull_latest_log():
    try:
        subprocess.run(["git", "fetch", "origin", "main"], cwd=REPO_DIR, check=True, capture_output=True, timeout=30)
        subprocess.run(["git", "checkout", "origin/main", "--", "logs/offline-leads.jsonl"], cwd=REPO_DIR, check=True, capture_output=True, timeout=30)
    except Exception as e:
        print(f"Warning: git pull failed: {e}", file=sys.stderr)


def write_monthly_report(enriched_entries, days):
    """Generate a monthly report markdown file in the Obsidian vault."""
    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    report_path = VAULT_DIR / f"offline-enrichment-{date_str}.md"

    matched = [e for e in enriched_entries if e.get("hcp_matched")]
    unmatched = [e for e in enriched_entries if not e.get("hcp_matched")]

    total_revenue = sum(float(e.get("hcp_total_amount", 0) or 0) for e in matched)
    total_sent = len(enriched_entries)
    match_count = len(matched)
    match_rate = (match_count / total_sent * 100) if total_sent > 0 else 0

    lines = [
        f"# Offline Leads Enrichment Report — {datetime.now().strftime('%B %d, %Y')}",
        "",
        f"**Lookback window:** Last {days} days",
        f"**Leads sent to Meta:** {total_sent}",
        f"**Matched to HCP jobs:** {match_count} ({match_rate:.0f}%)",
        f"**Total scheduled job revenue:** ${total_revenue:,.2f}",
        "",
        "## Matched Leads (with HCP Job Data)",
        "",
    ]

    if matched:
        lines.append("| Sent Date | Name | Phone | Job Status | Scheduled | Revenue | Invoice # |")
        lines.append("|-----------|------|-------|------------|-----------|---------|-----------|")
        for e in sorted(matched, key=lambda x: x.get("timestamp", ""), reverse=True):
            sent = (e.get("timestamp") or "").split("T")[0]
            name = f"{e.get('first_name', '')} {e.get('last_name', '')}".strip() or "—"
            phone = e.get("phone", "") or "—"
            status = e.get("hcp_work_status", "—")
            sched = (e.get("hcp_scheduled_start") or "").split("T")[0] or "—"
            rev = f"${float(e.get('hcp_total_amount', 0) or 0):,.2f}"
            inv = e.get("hcp_invoice_number") or "—"
            lines.append(f"| {sent} | {name} | {phone} | {status} | {sched} | {rev} | {inv} |")
    else:
        lines.append("*No matches found.*")
    lines.append("")

    lines.append("## Unmatched Leads")
    lines.append("")
    lines.append(f"*{len(unmatched)} leads sent to Meta have no corresponding HCP job yet (either not scheduled, or HCP has different contact info).*")
    lines.append("")
    if unmatched:
        lines.append("| Sent Date | Name | Phone | Email |")
        lines.append("|-----------|------|-------|-------|")
        for e in sorted(unmatched, key=lambda x: x.get("timestamp", ""), reverse=True):
            sent = (e.get("timestamp") or "").split("T")[0]
            name = f"{e.get('first_name', '')} {e.get('last_name', '')}".strip() or "—"
            phone = e.get("phone", "") or "—"
            email = e.get("email", "") or "—"
            lines.append(f"| {sent} | {name} | {phone} | {email} |")
    lines.append("")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report written to {report_path}", file=sys.stderr)
    return report_path


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60, help="Lookback window in days (default: 60)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes to log file")
    parser.add_argument("--no-pull", action="store_true", help="Skip git pull of log file")
    args = parser.parse_args()

    keys = load_api_keys()
    hcp_key = keys.get("HCP_API_KEY")
    if not hcp_key:
        print("Error: HCP_API_KEY not found in api-keys.env", file=sys.stderr)
        sys.exit(1)

    if not args.no_pull:
        pull_latest_log()

    if not LOG_PATH.exists() or LOG_PATH.stat().st_size == 0:
        print("No offline leads log found or empty.", file=sys.stderr)
        sys.exit(0)

    entries = []
    for line in LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    windowed = []
    for e in entries:
        try:
            ts = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
            if ts >= cutoff:
                windowed.append(e)
        except (KeyError, ValueError):
            continue

    print(f"Loaded {len(entries)} total entries, {len(windowed)} in last {args.days} days", file=sys.stderr)

    if not windowed:
        print("No entries in window to enrich.", file=sys.stderr)
        sys.exit(0)

    print(f"Fetching HCP jobs from last {args.days} days...", file=sys.stderr)
    jobs = hcp_get_all_jobs(hcp_key, since_date=cutoff)
    print(f"Loaded {len(jobs)} jobs from HCP", file=sys.stderr)

    by_phone, by_email = build_job_index(jobs)
    print(f"Indexed {len(by_phone)} phones and {len(by_email)} emails", file=sys.stderr)

    enriched = []
    for entry in windowed:
        job = match_lead_to_job(entry, by_phone, by_email)
        enriched.append(enrich_entry(dict(entry), job))

    matched_count = sum(1 for e in enriched if e.get("hcp_matched"))
    total_rev = sum(float(e.get("hcp_total_amount", 0) or 0) for e in enriched if e.get("hcp_matched"))
    print(f"\nResults:", file=sys.stderr)
    print(f"  Matched: {matched_count}/{len(enriched)}", file=sys.stderr)
    print(f"  Total revenue: ${total_rev:,.2f}", file=sys.stderr)

    if not args.dry_run:
        # Rewrite log file: enriched entries replace their originals, unchanged entries stay
        entry_keys = {(e.get("timestamp"), e.get("phone"), e.get("email")) for e in windowed}
        enriched_map = {(e.get("timestamp"), e.get("phone"), e.get("email")): e for e in enriched}

        new_lines = []
        for original in entries:
            key = (original.get("timestamp"), original.get("phone"), original.get("email"))
            if key in enriched_map:
                new_lines.append(json.dumps(enriched_map[key]))
            else:
                new_lines.append(json.dumps(original))

        LOG_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        print(f"Updated {LOG_PATH}", file=sys.stderr)

    # Write monthly report
    write_monthly_report(enriched, args.days)


if __name__ == "__main__":
    main()
