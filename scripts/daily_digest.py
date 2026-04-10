"""
Generate daily ASH Ads performance digest from Meta Ads API.
Pulls data, analyzes against benchmarks, writes markdown to Obsidian vault.

Usage:
    python daily_digest.py              # Run for today
    python daily_digest.py --days 14    # Custom lookback window
"""

import json
import os
import subprocess
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path


# --- Config ---
VAULT_DIR = Path.home() / "Documents" / "Obsidian Vault" / "Business" / "HVAC Lead Gen" / "Clients" / "Ash Cooling & Heating" / "Daily Digests"
REPO_DIR = Path(__file__).resolve().parent.parent
OFFLINE_LOG_PATH = REPO_DIR / "logs" / "offline-leads.jsonl"
CPL_TARGET = 177
CPL_GOOD = 150
CPL_ACCEPTABLE = 200
CPL_CONCERNING = 300
KILL_SPEND_ZERO_LEADS = 200
FATIGUE_FREQUENCY = 3.5
MIN_SPEND_FOR_VERDICT = 50
MIN_LEADS_FOR_VERDICT = 2
SCALE_CPL = 150
SCALE_LEADS = 3


def load_api_keys():
    env_path = Path.home() / ".claude" / "config" / "api-keys.env"
    keys = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            keys[k.strip()] = v.strip()
    return keys.get("META_ADS_ACCESS_TOKEN"), keys.get("META_AD_ACCOUNT_ID")


def api_get(endpoint, params, token):
    params["access_token"] = token
    url = f"https://graph.facebook.com/v21.0/{endpoint}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url)) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"API Error {e.code}: {e.read().decode()}", file=sys.stderr)
        return {"data": []}


def pull_insights(token, account_id, days, level):
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    fields = "campaign_name,campaign_id,spend,impressions,reach,frequency,clicks,ctr,cpc,actions,cost_per_action_type"
    if level == "ad":
        fields = "ad_name,ad_id,adset_name," + fields
    params = {
        "fields": fields,
        "time_range": json.dumps({"since": since, "until": until}),
        "level": level,
        "limit": 500,
    }
    return api_get(f"{account_id}/insights", params, token)


def pull_daily(token, account_id, days):
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "campaign_name,spend,impressions,clicks,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "level": "campaign",
        "limit": 500,
    }
    return api_get(f"{account_id}/insights", params, token)


def extract_leads_cpl(row):
    """Meta returns both 'lead' (rollup) and 'offsite_conversion.fb_pixel_lead'
    for the same conversion. Use only 'lead' to avoid double-counting."""
    leads, cpl = 0, 0.0
    for a in row.get("actions", []):
        if a.get("action_type") == "lead":
            leads = int(a.get("value", 0))
            break
    for c in row.get("cost_per_action_type", []):
        if c.get("action_type") == "lead":
            cpl = float(c.get("value", 0))
            break
    return leads, cpl


def pull_latest_log():
    """Git pull so we have the latest offline-leads.jsonl from the webhook."""
    try:
        subprocess.run(
            ["git", "fetch", "origin", "main"],
            cwd=REPO_DIR,
            check=True,
            capture_output=True,
            timeout=30,
        )
        # Pull just the logs file to avoid conflicts with unstaged work
        subprocess.run(
            ["git", "checkout", "origin/main", "--", "logs/offline-leads.jsonl"],
            cwd=REPO_DIR,
            check=True,
            capture_output=True,
            timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"Warning: could not pull latest log ({e})", file=sys.stderr)


def load_offline_log(days):
    """Read logs/offline-leads.jsonl and return entries from the last N days."""
    if not OFFLINE_LOG_PATH.exists():
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    entries = []

    for line in OFFLINE_LOG_PATH.read_text(encoding="utf-8").splitlines():
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


def verdict(spend, leads, cpl, frequency):
    if spend < MIN_SPEND_FOR_VERDICT and leads < MIN_LEADS_FOR_VERDICT:
        return "NEW"
    if leads >= SCALE_LEADS and cpl > 0 and cpl < SCALE_CPL:
        return "SCALE"
    if (leads >= 5 and cpl > CPL_CONCERNING) or (spend >= KILL_SPEND_ZERO_LEADS and leads == 0):
        return "KILL"
    if leads >= MIN_LEADS_FOR_VERDICT and cpl > 0 and cpl <= CPL_ACCEPTABLE:
        return "WATCH"
    if spend >= MIN_SPEND_FOR_VERDICT and leads == 0:
        return "KILL"
    if leads > 0 and cpl > CPL_ACCEPTABLE:
        return "KILL"
    return "WATCH"


def generate_alerts(ads):
    alerts = []
    for a in ads:
        name, spend, leads, cpl, freq = a["name"], a["spend"], a["leads"], a["cpl"], a["frequency"]
        if spend >= KILL_SPEND_ZERO_LEADS and leads == 0:
            alerts.append(f"BUDGET WASTE: {name} — ${spend:.0f} spent, 0 leads. Kill it.")
        elif leads > 0 and cpl > CPL_CONCERNING:
            alerts.append(f"CPL SPIKE: {name} — ${cpl:.0f} CPL on {leads} leads. Way above ${CPL_TARGET} target.")
        elif leads > 0 and cpl > CPL_ACCEPTABLE:
            alerts.append(f"CPL WARNING: {name} — ${cpl:.0f} CPL. Above ${CPL_ACCEPTABLE} acceptable range.")
        if freq > FATIGUE_FREQUENCY:
            alerts.append(f"CREATIVE FATIGUE: {name} — frequency at {freq:.1f} (threshold: {FATIGUE_FREQUENCY}).")
        if leads >= SCALE_LEADS and cpl > 0 and cpl < SCALE_CPL:
            alerts.append(f"WINNER: {name} — ${cpl:.0f} CPL on {leads} leads. Consider scaling.")
    return alerts


def generate_recommendations(campaigns, ads):
    recs = []
    kills = [a for a in ads if a["verdict"] == "KILL"]
    winners = [a for a in ads if a["verdict"] == "SCALE"]
    total_kill_spend = sum(a["spend"] for a in kills)
    total_kill_leads = sum(a["leads"] for a in kills)

    if kills:
        kill_names = ", ".join(a["name"] for a in kills[:5])
        recs.append(f"Kill {len(kills)} underperformers ({kill_names}). Combined ${total_kill_spend:.0f} spend, {total_kill_leads} leads. Redirect budget to winners.")
    if winners:
        for w in winners[:2]:
            recs.append(f"Scale {w['name']} — ${w['cpl']:.0f} CPL on {w['leads']} leads. Room to grow.")

    for c in campaigns:
        if c["frequency"] > 3.0 and c["cpl"] > CPL_ACCEPTABLE:
            recs.append(f"Creative refresh needed on {c['name']} — frequency at {c['frequency']:.1f}, CPL at ${c['cpl']:.0f}.")
    if not recs:
        recs.append("No immediate actions. Monitor for another 24-48 hours.")
    return recs[:4]


def build_digest(campaigns, ads, daily, offline_entries, days):
    today = datetime.now().strftime("%B %d, %Y")
    total_spend = sum(c["spend"] for c in campaigns)
    total_leads = sum(c["leads"] for c in campaigns)
    total_offline = len(offline_entries)
    blended_cpl = total_spend / total_leads if total_leads > 0 else 0

    lines = [f"# ASH Ads Daily Digest — {today}", ""]

    # Campaign snapshot — Offline = total HCP leads sent to Meta (global, not per-campaign)
    lines.append(f"## Campaign Snapshot (Last {days} Days)")
    lines.append("")
    lines.append("| Campaign | Spend | Leads | Offline | CPL | Frequency | CTR | Status |")
    lines.append("|----------|-------|-------|---------|-----|-----------|-----|--------|")
    for i, c in enumerate(campaigns):
        cpl_str = f"${c['cpl']:.0f}" if c['cpl'] > 0 else "—"
        freq_str = f"{c['frequency']:.2f}" if c['frequency'] > 0 else "—"
        ctr_str = f"{c['ctr']:.2f}%" if c['ctr'] > 0 else "—"
        status = "ACTIVE" if c["spend"] > 10 else "NEW"
        # Show offline only once in the first row as a global total
        offline_str = f"{total_offline}" if i == 0 else "—"
        lines.append(f"| {c['name']} | ${c['spend']:.2f} | {c['leads']} | {offline_str} | {cpl_str} | {freq_str} | {ctr_str} | {status} |")
    lines.append("")
    lines.append(f"*Offline = total HCP leads sent to Meta Offline Conversions dataset in the period. Not split per-campaign — see Meta Ads Manager > Offline Contacts column for attribution breakdown.*")
    lines.append("")

    # Ad-level
    lines.append("## Ad-Level Performance")
    lines.append("")
    lines.append("| Ad Name | Campaign | Spend | Leads | CPL | CTR | Verdict |")
    lines.append("|---------|----------|-------|-------|-----|-----|---------|")
    sorted_ads = sorted(ads, key=lambda a: (a["cpl"] if a["cpl"] > 0 else 99999))
    for a in sorted_ads:
        cpl_str = f"${a['cpl']:.0f}" if a["cpl"] > 0 else "—"
        ctr_str = f"{a['ctr']:.2f}%" if a["ctr"] > 0 else "—"
        v = a["verdict"]
        v_fmt = f"**{v}**" if v in ("SCALE", "KILL") else v
        lines.append(f"| {a['name']} | {a['campaign']} | ${a['spend']:.2f} | {a['leads']} | {cpl_str} | {ctr_str} | {v_fmt} |")
    lines.append("")

    # Alerts
    alerts = generate_alerts(ads)
    lines.append("## Alerts")
    lines.append("")
    if alerts:
        for alert in alerts:
            lines.append(f"- {alert}")
    else:
        lines.append("- No alerts. All metrics within acceptable ranges.")
    lines.append("")

    # Trends
    lines.append("## Trends")
    lines.append("")
    if len(daily) >= 2:
        first_half = daily[:len(daily)//2]
        second_half = daily[len(daily)//2:]
        first_cpl = sum(d.get("spend", 0) for d in first_half) / max(sum(d.get("leads", 0) for d in first_half), 1)
        second_cpl = sum(d.get("spend", 0) for d in second_half) / max(sum(d.get("leads", 0) for d in second_half), 1)
        if second_cpl < first_cpl * 0.9:
            lines.append("- CPL trend: **IMPROVING**")
        elif second_cpl > first_cpl * 1.1:
            lines.append("- CPL trend: **WORSENING**")
        else:
            lines.append("- CPL trend: STABLE")
    daily_spends = [(d.get("date", ""), d.get("spend", 0)) for d in daily]
    avg_daily = total_spend / max(days, 1)
    lines.append(f"- Average daily spend: ${avg_daily:.2f}")
    lines.append(f"- Total leads over period: {total_leads}")
    lines.append("")

    # Recommendations
    recs = generate_recommendations(campaigns, ads)
    lines.append("## Recommendations")
    lines.append("")
    for i, rec in enumerate(recs, 1):
        lines.append(f"{i}. {rec}")
    lines.append("")

    # Offline Leads Table
    lines.append(f"## Offline Leads Sent to Meta ({days}d)")
    lines.append("")
    if offline_entries:
        # Sort newest first
        sorted_entries = sorted(offline_entries, key=lambda e: e.get("timestamp", ""), reverse=True)
        lines.append("| Date | Name | Phone | Email | City | Fields Matched | Meta Received |")
        lines.append("|------|------|-------|-------|------|----------------|---------------|")
        for e in sorted_entries:
            ts = e.get("timestamp", "")
            date_str = ts.split("T")[0] if ts else "—"
            name = f"{e.get('first_name', '')} {e.get('last_name', '')}".strip() or "—"
            phone = e.get("phone", "") or "—"
            email = e.get("email", "") or "—"
            city = e.get("city", "") or "—"
            fields = ", ".join(e.get("matched_fields", []))
            received = "✓" if e.get("meta_events_received", 0) > 0 else "✗"
            lines.append(f"| {date_str} | {name} | {phone} | {email} | {city} | {fields} | {received} |")
    else:
        lines.append("*No offline leads logged in this period.*")
    lines.append("")

    # Raw numbers
    lines.append("## Raw Numbers")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total spend ({days}d) | ${total_spend:.2f} |")
    lines.append(f"| Total website leads ({days}d) | {total_leads} |")
    lines.append(f"| Total offline leads sent ({days}d) | {total_offline} |")
    lines.append(f"| Blended CPL (website only) | ${blended_cpl:.2f} |" if total_leads > 0 else f"| Blended CPL (website only) | — |")
    lines.append(f"| Active ads | {len([a for a in ads if a['spend'] > 0])} |")
    lines.append("")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    token, account_id = load_api_keys()
    if not token or not account_id:
        print("Missing API keys", file=sys.stderr)
        sys.exit(1)

    print("Pulling campaign data...", file=sys.stderr)
    camp_raw = pull_insights(token, account_id, args.days, "campaign")
    print("Pulling ad data...", file=sys.stderr)
    ad_raw = pull_insights(token, account_id, args.days, "ad")
    print("Pulling daily trend...", file=sys.stderr)
    daily_raw = pull_daily(token, account_id, args.days)

    campaigns = []
    for row in camp_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        campaigns.append({
            "name": row.get("campaign_name", "Unknown"),
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

    ads = []
    for row in ad_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        spend = float(row.get("spend", 0))
        freq = float(row.get("frequency", 0))
        v = verdict(spend, leads, cpl, freq)
        ads.append({
            "name": row.get("ad_name", "Unknown"),
            "adset": row.get("adset_name", ""),
            "campaign": row.get("campaign_name", ""),
            "spend": spend,
            "impressions": int(row.get("impressions", 0)),
            "reach": int(row.get("reach", 0)),
            "frequency": freq,
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "cpc": float(row.get("cpc", 0)),
            "leads": leads,
            "cpl": cpl,
            "verdict": v,
        })

    daily = []
    for row in daily_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        daily.append({
            "date": row.get("date_start", ""),
            "campaign": row.get("campaign_name", ""),
            "spend": float(row.get("spend", 0)),
            "leads": leads,
            "cpl": cpl,
        })

    # Pull latest audit log from git and read offline entries
    print("Pulling latest offline leads log...", file=sys.stderr)
    pull_latest_log()
    offline_entries = load_offline_log(args.days)
    print(f"Found {len(offline_entries)} offline entries in last {args.days} days", file=sys.stderr)

    digest = build_digest(campaigns, ads, daily, offline_entries, args.days)

    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = VAULT_DIR / f"{date_str}.md"
    out_path.write_text(digest, encoding="utf-8")
    print(f"Digest written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
