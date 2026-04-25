"""
ASH Ads Daily Digest (lean version — 2026-04-22 methodology).

Per-ad test, applied at age >= 7 days:
  1. CPL at or below target in last 7d
  2. Frequency under 3.5
  3. CTR holding up vs week 1 (>= 70% of week-1 CTR)

Ads under 7 days old = LEARNING. Ads failing any test at 7d+ = KILL.

Keeps only the signals needed for a daily glance:
  - Refresh status (one line)
  - Monthly pacing (three-line table)
  - Per-ad decision table
  - Actionable alerts

Deeper analysis (best-week CPL, age buckets, trends, full offline audit) lives
in weekly_digest.py.

Usage:
    python daily_digest.py              # Default: 7-day lookback
    python daily_digest.py --days 14    # Custom lookback
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
AD_TRACKING_PATH = REPO_DIR / "logs" / "ad-tracking.json"

# Only campaigns AND ad sets in these lists are included in the report.
# Empty list = no filter at that level. Update when launching/sunsetting.
# Match is exact on the name string.
ACTIVE_CAMPAIGNS = [
    "Beat A Quote - New - 3/26",
]
ACTIVE_ADSETS = [
    "April - Creative Batch",
]


def is_active_campaign(name):
    return not ACTIVE_CAMPAIGNS or name in ACTIVE_CAMPAIGNS


def is_active_adset(name):
    return not ACTIVE_ADSETS or name in ACTIVE_ADSETS


def is_active_row(row):
    """Match insights row by both campaign AND adset filters."""
    return is_active_campaign(row.get("campaign_name", "")) and is_active_adset(row.get("adset_name", ""))

# The three-test thresholds
CPL_TARGET = 177
FREQUENCY_CAP = 3.5
CTR_HOLD_RATIO = 0.70            # CTR >= 70% of week-1 CTR = "holding up"
AGE_LEARNING_MAX = 7             # 0-6 days = LEARNING (no verdict)
MIN_SPEND_FOR_KILL = 50          # Don't kill an ad with barely any spend

# Refresh cadence
REFRESH_CYCLE_DAYS = 14
REFRESH_DUE_DAYS = 10

# Pacing window — anchored to the test/batch launch, not calendar month.
# Update PACING_LAUNCH_DATE whenever you launch a new test. The window is rolling
# 30 days from that date, with $8k spend / 45 leads as the targets across the window.
PACING_LAUNCH_DATE = "2026-04-21"
PACING_WINDOW_DAYS = 30
MONTHLY_LEAD_TARGET = 45      # 45 leads target across PACING_WINDOW_DAYS
MONTHLY_SPEND_TARGET = 8000   # $8k spend target across PACING_WINDOW_DAYS

# CBO bias watch (shown in weekly; flagged in daily alerts if egregious)
CBO_BIAS_SPEND_SHARE = 0.60
CBO_BIAS_MIN_ADS = 3


# ============================================================
# DATA PULLS (shared with weekly_digest.py — do not break signatures)
# ============================================================

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
    fields = "campaign_name,campaign_id,spend,impressions,reach,frequency,clicks,ctr,cpc,actions,cost_per_action_type,conversions"
    if level == "ad":
        fields = "ad_name,ad_id,adset_name," + fields
    elif level == "adset":
        fields = "adset_name,adset_id," + fields
    params = {
        "fields": fields,
        "time_range": json.dumps({"since": since, "until": until}),
        "level": level,
        "limit": 500,
        "use_unified_attribution_setting": "true",
    }
    return api_get(f"{account_id}/insights", params, token)


def pull_daily(token, account_id, days):
    """Daily breakdown at ad-set level so it can be filtered by ACTIVE_ADSETS."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "campaign_name,adset_name,spend,impressions,clicks,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "level": "adset",
        "limit": 1000,
    }
    return api_get(f"{account_id}/insights", params, token)


def pull_ad_daily(token, account_id, days=30):
    """Per-ad daily breakdown. Needed for both first-seen backfill and week-1 CTR."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "ad_id,ad_name,spend,impressions,clicks,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "level": "ad",
        "limit": 2000,
    }
    return api_get(f"{account_id}/insights", params, token)


def pull_month_to_date(token, account_id):
    """Per-adset spend since PACING_LAUNCH_DATE through today.

    Function name is legacy ("month-to-date") — math is now launch-anchored.
    """
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "campaign_name,adset_name,spend,actions,cost_per_action_type,conversions",
        "time_range": json.dumps({"since": PACING_LAUNCH_DATE, "until": until}),
        "level": "adset",
        "limit": 500,
        "use_unified_attribution_setting": "true",
    }
    return api_get(f"{account_id}/insights", params, token)


def extract_leads_cpl(row):
    """Website lead count + CPL. 'lead' action only (avoid double-count)."""
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


def extract_offline_contacts(row):
    for c in row.get("conversions", []):
        if c.get("action_type") == "contact_offline":
            return int(c.get("value", 0))
    return 0


def pull_latest_log():
    try:
        subprocess.run(
            ["git", "fetch", "origin", "main"],
            cwd=REPO_DIR, check=True, capture_output=True, timeout=30,
        )
        subprocess.run(
            ["git", "checkout", "origin/main", "--", "logs/offline-leads.jsonl"],
            cwd=REPO_DIR, check=True, capture_output=True, timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        print(f"Warning: could not pull latest log ({e})", file=sys.stderr)


def load_offline_log(days):
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


# ============================================================
# AD TRACKING (schema v2: {ad_id: {first_seen, week1_ctr}})
# ============================================================

def load_ad_tracking():
    """Load ad tracking. Migrates v1 schema (string date) -> v2 (dict) on read."""
    if not AD_TRACKING_PATH.exists():
        return {}
    try:
        raw = json.loads(AD_TRACKING_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    migrated = {}
    for ad_id, val in raw.items():
        if isinstance(val, str):
            migrated[ad_id] = {"first_seen": val, "week1_ctr": None}
        elif isinstance(val, dict):
            migrated[ad_id] = {
                "first_seen": val.get("first_seen"),
                "week1_ctr": val.get("week1_ctr"),
            }
    return migrated


def save_ad_tracking(tracking):
    AD_TRACKING_PATH.parent.mkdir(parents=True, exist_ok=True)
    AD_TRACKING_PATH.write_text(
        json.dumps(tracking, indent=2, sort_keys=True), encoding="utf-8"
    )


def build_ad_daily_index(ad_daily_data):
    """Group daily rows by ad_id, sorted ascending by date. Includes clicks/impressions."""
    by_ad = {}
    for row in ad_daily_data.get("data", []):
        ad_id = row.get("ad_id")
        if not ad_id:
            continue
        leads, cpl = extract_leads_cpl(row)
        by_ad.setdefault(ad_id, []).append({
            "date": row.get("date_start", ""),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "leads": leads,
            "cpl": cpl,
        })
    for ad_id in by_ad:
        by_ad[ad_id].sort(key=lambda d: d["date"])
    return by_ad


def update_ad_tracking(ads, tracking, ad_daily_index):
    """Backfill first_seen from history; compute week1_ctr once we have 7d of data."""
    today = datetime.now().strftime("%Y-%m-%d")
    updated = False

    for ad in ads:
        ad_id = ad.get("id")
        if not ad_id or ad.get("spend", 0) <= 0:
            continue

        rec = tracking.get(ad_id)

        # New ad — backfill first_seen
        if rec is None:
            first_seen = today
            daily = ad_daily_index.get(ad_id, [])
            for d in daily:
                if d["spend"] > 0 and d["date"]:
                    first_seen = d["date"]
                    break
            tracking[ad_id] = {"first_seen": first_seen, "week1_ctr": None}
            rec = tracking[ad_id]
            updated = True

        # Backfill week1_ctr if unset and we have >= 7 days of data since first_seen
        if rec.get("week1_ctr") is None:
            w1 = calculate_week1_ctr(ad_id, ad_daily_index)
            if w1 is not None:
                rec["week1_ctr"] = w1
                updated = True

    return updated


def calculate_ad_age(ad_id, tracking):
    rec = tracking.get(ad_id)
    if not rec:
        return None
    first_seen = rec.get("first_seen") if isinstance(rec, dict) else rec
    if not first_seen:
        return None
    try:
        first = datetime.strptime(first_seen, "%Y-%m-%d")
        return (datetime.now() - first).days
    except ValueError:
        return None


def get_week1_ctr(ad_id, tracking):
    rec = tracking.get(ad_id)
    if not rec or not isinstance(rec, dict):
        return None
    return rec.get("week1_ctr")


def calculate_week1_ctr(ad_id, ad_daily_index):
    """CTR for the first 7 days with spend. Returns None if insufficient data."""
    daily = ad_daily_index.get(ad_id, [])
    if not daily:
        return None
    # First day with spend
    start_idx = next((i for i, d in enumerate(daily) if d["spend"] > 0), None)
    if start_idx is None:
        return None
    window = daily[start_idx:start_idx + 7]
    if len(window) < 4:  # need at least 4 days in the first week for a stable baseline
        return None
    clicks = sum(d.get("clicks", 0) for d in window)
    impressions = sum(d.get("impressions", 0) for d in window)
    if impressions == 0:
        return None
    return (clicks / impressions) * 100


# ============================================================
# VERDICT (3-test methodology)
# ============================================================

def three_test_verdict(spend, leads, cpl, frequency, ctr, age_days, week1_ctr):
    """Return (verdict, test_results) where test_results is (cpl_pass, freq_pass, ctr_pass).

    LEARNING: age unknown or < 7d.
    KILL: any of the three tests failed at age 7+.
    KEEP: all three pass.
    """
    if age_days is None or age_days < AGE_LEARNING_MAX:
        return "LEARNING", (None, None, None)

    # Test 1: CPL at or below target
    if spend < MIN_SPEND_FOR_KILL and leads == 0:
        cpl_pass = None  # not enough signal yet
    elif leads == 0:
        cpl_pass = False  # meaningful spend, zero leads
    else:
        cpl_pass = cpl > 0 and cpl <= CPL_TARGET

    # Test 2: Frequency under cap
    freq_pass = frequency < FREQUENCY_CAP if frequency > 0 else None

    # Test 3: CTR holding up vs week 1 (skip if no baseline)
    if week1_ctr and week1_ctr > 0 and ctr > 0:
        ctr_pass = ctr >= week1_ctr * CTR_HOLD_RATIO
    else:
        ctr_pass = None

    # KILL if any test explicitly fails (False). Unknown tests (None) don't fail.
    if cpl_pass is False or freq_pass is False or ctr_pass is False:
        return "KILL", (cpl_pass, freq_pass, ctr_pass)
    return "KEEP", (cpl_pass, freq_pass, ctr_pass)


# ============================================================
# REFRESH STATUS + CBO BIAS
# ============================================================

def get_latest_batch_info(ads, tracking):
    """(days_since_latest_batch, latest_batch_date, batch_count).

    A batch = ads first_seen within 3 days of each other.
    """
    active = []
    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        rec = tracking.get(a.get("id"))
        if not rec:
            continue
        first_seen = rec.get("first_seen") if isinstance(rec, dict) else rec
        if not first_seen:
            continue
        try:
            active.append(datetime.strptime(first_seen, "%Y-%m-%d"))
        except ValueError:
            continue

    if not active:
        return None, None, 0

    latest = max(active)
    days_since = (datetime.now() - latest).days
    batch_cutoff = latest - timedelta(days=3)
    batch_count = sum(1 for d in active if d >= batch_cutoff)
    return days_since, latest.strftime("%Y-%m-%d"), batch_count


def detect_cbo_bias(ads):
    """Return list of (adset_name, biased_ad_name, share_pct, spend, total) tuples."""
    alerts = []
    by_adset = {}
    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        by_adset.setdefault(a.get("adset") or "Unknown", []).append(a)

    for adset_name, adset_ads in by_adset.items():
        if len(adset_ads) < CBO_BIAS_MIN_ADS:
            continue
        total = sum(a["spend"] for a in adset_ads)
        if total <= 0:
            continue
        for a in adset_ads:
            share = a["spend"] / total
            if share >= CBO_BIAS_SPEND_SHARE:
                alerts.append((adset_name, a["name"], share * 100, a["spend"], total))
    return alerts


# ============================================================
# MONTHLY PACING
# ============================================================

def calculate_monthly_pacing(mtd_raw):
    """Pacing math anchored to PACING_LAUNCH_DATE (not calendar month)."""
    data = mtd_raw.get("data", [])
    now = datetime.now()
    launch = datetime.strptime(PACING_LAUNCH_DATE, "%Y-%m-%d")
    days_elapsed = max(0, (now - launch).days)
    days_remaining = max(0, PACING_WINDOW_DAYS - days_elapsed)
    pct_through = min(100.0, (days_elapsed / PACING_WINDOW_DAYS) * 100) if PACING_WINDOW_DAYS else 0

    rows = [r for r in data if is_active_row(r)]

    if not rows:
        return {
            "spend": 0, "leads": 0, "offline_attributed": 0, "total_attributed": 0,
            "day_of_month": days_elapsed, "days_in_month": PACING_WINDOW_DAYS,
            "pct_through_month": pct_through,
            "pct_to_lead_target": 0, "pct_to_spend_target": 0,
            "leads_needed": MONTHLY_LEAD_TARGET,
            "days_remaining": days_remaining,
            "launch_date": PACING_LAUNCH_DATE,
        }

    spend = sum(float(r.get("spend", 0)) for r in rows)
    leads = sum(extract_leads_cpl(r)[0] for r in rows)
    offline = sum(extract_offline_contacts(r) for r in rows)
    total = leads + offline

    return {
        "spend": spend, "leads": leads, "offline_attributed": offline,
        "total_attributed": total,
        "day_of_month": days_elapsed, "days_in_month": PACING_WINDOW_DAYS,
        "pct_through_month": pct_through,
        "pct_to_lead_target": (total / MONTHLY_LEAD_TARGET) * 100 if MONTHLY_LEAD_TARGET else 0,
        "pct_to_spend_target": (spend / MONTHLY_SPEND_TARGET) * 100 if MONTHLY_SPEND_TARGET else 0,
        "leads_needed": max(0, MONTHLY_LEAD_TARGET - total),
        "days_remaining": days_remaining,
        "launch_date": PACING_LAUNCH_DATE,
    }


# ============================================================
# DIGEST BUILDER (lean)
# ============================================================

def fmt_test(result):
    if result is None:
        return "—"
    return "✓" if result else "✗"


def build_digest(adsets, ads, offline_entries, days, tracking, refresh_info, pacing, cbo_alerts):
    today = datetime.now().strftime("%B %d, %Y")
    lines = [f"# ASH Ads Daily — {today}", ""]

    # --- Top-of-page status ---
    days_since, batch_date, batch_count = refresh_info
    if days_since is None:
        refresh_line = "**Refresh:** no tracked batches yet"
    elif days_since >= REFRESH_CYCLE_DAYS:
        refresh_line = f"**Refresh:** 🚨 OVERDUE — last batch {days_since}d ago ({batch_date}, {batch_count} ads)"
    elif days_since >= REFRESH_DUE_DAYS:
        refresh_line = f"**Refresh:** ⚠️ due soon — last batch {days_since}d ago ({batch_date}, {batch_count} ads). Next cycle at {REFRESH_CYCLE_DAYS}d."
    else:
        refresh_line = f"**Refresh:** ✅ {days_since}d since last batch ({batch_date}, {batch_count} ads). Next due in {REFRESH_CYCLE_DAYS - days_since}d."
    lines.append(refresh_line)

    pct_diff = pacing["pct_to_lead_target"] - pacing["pct_through_month"]
    if pct_diff >= 5:
        pacing_emoji = "✅ AHEAD"
    elif pct_diff >= -15:
        pacing_emoji = "➖ ON TRACK"
    else:
        pacing_emoji = "🚨 BEHIND"
    lines.append(
        f"**Pacing:** {pacing_emoji} — {pacing['total_attributed']}/{MONTHLY_LEAD_TARGET} leads "
        f"({pacing['pct_to_lead_target']:.0f}%), need {pacing['leads_needed']} more in "
        f"{pacing['days_remaining']}d. ${pacing['spend']:.0f} / ${MONTHLY_SPEND_TARGET:,} spent "
        f"({pacing['pct_to_spend_target']:.0f}%). "
        f"*Day {pacing['day_of_month']}/{pacing['days_in_month']} since launch ({pacing['launch_date']}).*"
    )
    lines.append("")

    # --- Ad Set Snapshot ---
    lines.append(f"## Ad Set Snapshot (Last {days} Days)")
    lines.append("")
    lines.append("| Ad Set | Spend | Leads | Offline | CPL | Effective CPL | Frequency | CTR |")
    lines.append("|--------|-------|-------|---------|-----|---------------|-----------|-----|")
    for a in adsets:
        cpl_str = f"${a['cpl']:.0f}" if a["cpl"] > 0 else "—"
        offline = a.get("offline_attributed", 0)
        total_attr = a["leads"] + offline
        eff_cpl = a["spend"] / total_attr if total_attr > 0 else 0
        eff_cpl_str = f"${eff_cpl:.0f}" if eff_cpl > 0 else "—"
        freq_str = f"{a['frequency']:.2f}" if a["frequency"] > 0 else "—"
        ctr_str = f"{a['ctr']:.2f}%" if a["ctr"] > 0 else "—"
        lines.append(
            f"| {a['name']} | ${a['spend']:.2f} | {a['leads']} | {offline} | "
            f"{cpl_str} | {eff_cpl_str} | {freq_str} | {ctr_str} |"
        )
    lines.append("")
    lines.append("*CPL = website leads only. Effective CPL = includes Meta-attributed offline contacts.*")
    lines.append("")

    # --- Ad decision table ---
    lines.append(f"## Ad Decisions (last {days}d)")
    lines.append("")
    lines.append("| Ad | Age | Spend | Leads | CPL ≤$177 | Freq <3.5 | CTR vs W1 | Verdict |")
    lines.append("|----|-----|-------|-------|-----------|-----------|-----------|---------|")

    # Sort: KILL first (most urgent), then KEEP, then LEARNING
    order = {"KILL": 0, "KEEP": 1, "LEARNING": 2}
    active_ads = [a for a in ads if a.get("spend", 0) > 0]
    active_ads.sort(key=lambda a: (order.get(a["verdict"], 3), -(a["spend"])))

    for a in active_ads:
        age = calculate_ad_age(a.get("id"), tracking)
        age_str = f"{age}d" if age is not None else "?"
        cpl_pass, freq_pass, ctr_pass = a["test_results"]

        # CPL cell: show value + ✓/✗
        if a["leads"] > 0 and a["cpl"] > 0:
            cpl_cell = f"${a['cpl']:.0f} {fmt_test(cpl_pass)}"
        elif a["spend"] >= MIN_SPEND_FOR_KILL and a["leads"] == 0:
            cpl_cell = f"0 leads {fmt_test(cpl_pass)}"
        else:
            cpl_cell = "—"

        freq_cell = f"{a['frequency']:.1f} {fmt_test(freq_pass)}" if a["frequency"] > 0 else "—"

        # CTR vs W1 cell
        w1 = get_week1_ctr(a.get("id"), tracking)
        if w1 and w1 > 0 and a["ctr"] > 0:
            ratio = a["ctr"] / w1 * 100
            ctr_cell = f"{ratio:.0f}% {fmt_test(ctr_pass)}"
        elif a["ctr"] > 0:
            ctr_cell = f"{a['ctr']:.2f}% (no W1)"
        else:
            ctr_cell = "—"

        v = a["verdict"]
        v_fmt = f"**{v}**" if v == "KILL" else v

        lines.append(
            f"| {a['name']} | {age_str} | ${a['spend']:.0f} | {a['leads']} | "
            f"{cpl_cell} | {freq_cell} | {ctr_cell} | {v_fmt} |"
        )
    lines.append("")
    lines.append("*Tests apply at age 7+. LEARNING = too new to judge. KILL = any test failed.*")
    lines.append("")

    # --- Alerts (actionable only) ---
    alerts = []

    # Kill list summary
    kills = [a for a in active_ads if a["verdict"] == "KILL"]
    if kills:
        kill_spend = sum(a["spend"] for a in kills)
        kill_leads = sum(a["leads"] for a in kills)
        names = ", ".join(a["name"] for a in kills)
        alerts.append(
            f"☠️ **KILL {len(kills)} ad{'s' if len(kills) != 1 else ''}:** {names}. "
            f"Combined ${kill_spend:.0f} spend, {kill_leads} leads."
        )

    # Refresh
    if days_since is not None and days_since >= REFRESH_DUE_DAYS:
        severity = "OVERDUE" if days_since >= REFRESH_CYCLE_DAYS else "due soon"
        alerts.append(f"🔄 Refresh {severity} — ship 2-3 new creatives.")

    # Pacing
    if pct_diff < -15 and pacing["day_of_month"] >= 7:
        alerts.append(
            f"🚨 Pacing behind — need {pacing['leads_needed']} leads in "
            f"{pacing['days_remaining']}d to hit {MONTHLY_LEAD_TARGET}."
        )

    # CBO bias (surface only the worst offender in daily)
    if cbo_alerts:
        worst = max(cbo_alerts, key=lambda x: x[2])
        _, ad_name, share, spend, total = worst
        alerts.append(
            f"⚖️ CBO bias: {ad_name} taking {share:.0f}% of ad set spend "
            f"(${spend:.0f} of ${total:.0f}). Check if others are starving."
        )

    lines.append("## Alerts")
    lines.append("")
    if alerts:
        for a in alerts:
            lines.append(f"- {a}")
    else:
        lines.append("- None. Ads holding, pacing on track, batch is fresh.")
    lines.append("")

    # --- One-line offline summary ---
    total_offline_sent = len(offline_entries)
    if total_offline_sent > 0:
        received = sum(1 for e in offline_entries if e.get("meta_events_received", 0) > 0)
        lines.append(
            f"*Offline leads sent ({days}d): {total_offline_sent}. Meta received {received}. "
            f"Full audit in weekly report.*"
        )
        lines.append("")

    return "\n".join(lines)


# ============================================================
# MAIN
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    token, account_id = load_api_keys()
    if not token or not account_id:
        print("Missing API keys", file=sys.stderr)
        sys.exit(1)

    print("Pulling ad-set-level insights...", file=sys.stderr)
    adset_raw = pull_insights(token, account_id, args.days, "adset")

    print("Pulling ad-level insights...", file=sys.stderr)
    ad_raw = pull_insights(token, account_id, args.days, "ad")

    print("Pulling per-ad daily history (30d) for age + week-1 CTR...", file=sys.stderr)
    ad_daily_raw = pull_ad_daily(token, account_id, 30)
    ad_daily_index = build_ad_daily_index(ad_daily_raw)

    print("Pulling month-to-date totals for pacing...", file=sys.stderr)
    mtd_raw = pull_month_to_date(token, account_id)
    pacing = calculate_monthly_pacing(mtd_raw)

    # Build ad-set snapshot list (filtered to ACTIVE_CAMPAIGNS + ACTIVE_ADSETS)
    adsets = []
    for row in adset_raw.get("data", []):
        if not is_active_row(row):
            continue
        leads, cpl = extract_leads_cpl(row)
        offline_attributed = extract_offline_contacts(row)
        adsets.append({
            "name": row.get("adset_name", "Unknown"),
            "campaign": row.get("campaign_name", ""),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "frequency": float(row.get("frequency", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "leads": leads,
            "cpl": cpl,
            "offline_attributed": offline_attributed,
        })

    # Build ad list (filtered to ACTIVE_CAMPAIGNS + ACTIVE_ADSETS)
    ads = []
    for row in ad_raw.get("data", []):
        if not is_active_row(row):
            continue
        leads, cpl = extract_leads_cpl(row)
        offline_attributed = extract_offline_contacts(row)
        ads.append({
            "id": row.get("ad_id"),
            "name": row.get("ad_name", "Unknown"),
            "adset": row.get("adset_name", ""),
            "campaign": row.get("campaign_name", ""),
            "spend": float(row.get("spend", 0)),
            "impressions": int(row.get("impressions", 0)),
            "frequency": float(row.get("frequency", 0)),
            "clicks": int(row.get("clicks", 0)),
            "ctr": float(row.get("ctr", 0)),
            "leads": leads,
            "cpl": cpl,
            "offline_attributed": offline_attributed,
        })

    # Tracking + verdicts
    tracking = load_ad_tracking()
    if update_ad_tracking(ads, tracking, ad_daily_index):
        save_ad_tracking(tracking)

    for a in ads:
        age = calculate_ad_age(a.get("id"), tracking)
        w1 = get_week1_ctr(a.get("id"), tracking)
        v, tests = three_test_verdict(
            a["spend"], a["leads"], a["cpl"], a["frequency"], a["ctr"], age, w1,
        )
        a["age_days"] = age
        a["week1_ctr"] = w1
        a["verdict"] = v
        a["test_results"] = tests

    refresh_info = get_latest_batch_info(ads, tracking)
    cbo_alerts = detect_cbo_bias(ads)

    # Offline log
    print("Pulling latest offline leads log...", file=sys.stderr)
    pull_latest_log()
    offline_entries = load_offline_log(args.days)

    digest = build_digest(
        adsets, ads, offline_entries, args.days,
        tracking, refresh_info, pacing, cbo_alerts,
    )

    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = VAULT_DIR / f"{date_str}.md"
    out_path.write_text(digest, encoding="utf-8")
    print(f"Digest written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
