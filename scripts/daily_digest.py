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
AD_TRACKING_PATH = REPO_DIR / "logs" / "ad-tracking.json"

# Performance benchmarks
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

# Ad lifecycle + refresh cadence (locked in with Brennan 2026-04-20)
REFRESH_CYCLE_DAYS = 14          # Full creative refresh cycle
REFRESH_DUE_DAYS = 10            # Warn when latest batch is 10+ days old
AD_AGE_LEARNING_MAX = 3          # Days 1-3 = "LEARNING" verdict
AD_AGE_EARLY_MAX = 7             # Days 4-7 = "EARLY SIGNAL" verdict
AD_AGE_FATIGUE_CHECK = 14        # Day 14+ fatigue logic kicks in
BEST_WEEK_FATIGUE_THRESHOLD = 0.30  # CPL 30% worse than best = fatigue

# CBO bias detection
CBO_BIAS_SPEND_SHARE = 0.60      # One ad >60% of ad set spend = potential bias
CBO_BIAS_MIN_ADS = 3             # Only check ad sets with 3+ ads

# Monthly targets ($8k/mo @ $177 CPL = 45 leads)
MONTHLY_LEAD_TARGET = 45
MONTHLY_SPEND_TARGET = 8000


# ============================================================
# DATA PULLS — UNTOUCHED from the working version
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
    params = {
        "fields": fields,
        "time_range": json.dumps({"since": since, "until": until}),
        "level": level,
        "limit": 500,
        "use_unified_attribution_setting": "true",
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
    """Extract website lead count and CPL.

    Note: Meta returns 'lead', 'onsite_web_lead', and 'offsite_conversion.fb_pixel_lead'
    as the same website conversion. Use only 'lead' to avoid double-counting.
    """
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
    """Extract Meta-attributed offline Contact events from the 'conversions' field."""
    for c in row.get("conversions", []):
        if c.get("action_type") == "contact_offline":
            return int(c.get("value", 0))
    return 0


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


# ============================================================
# NEW: ADDITIVE PULLS — do not modify anything above
# ============================================================

def pull_ad_daily(token, account_id, days=30):
    """Per-ad daily breakdown for best-week CPL calculation.

    ADDITIVE — does not touch pull_daily() which is campaign-level.
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")
    params = {
        "fields": "ad_id,ad_name,spend,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "time_increment": 1,
        "level": "ad",
        "limit": 2000,
    }
    return api_get(f"{account_id}/insights", params, token)


def pull_month_to_date(token, account_id):
    """Pull current calendar month data for pacing calculations."""
    now = datetime.now()
    since = now.replace(day=1).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    params = {
        "fields": "spend,actions,cost_per_action_type,conversions",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "account",
        "use_unified_attribution_setting": "true",
    }
    return api_get(f"{account_id}/insights", params, token)


# ============================================================
# NEW: AD TRACKING (persistent first-seen dates)
# ============================================================

def load_ad_tracking():
    """Load ad_id -> first_seen_date dict from disk."""
    if not AD_TRACKING_PATH.exists():
        return {}
    try:
        return json.loads(AD_TRACKING_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_ad_tracking(tracking):
    """Persist ad tracking dict."""
    AD_TRACKING_PATH.parent.mkdir(parents=True, exist_ok=True)
    AD_TRACKING_PATH.write_text(json.dumps(tracking, indent=2, sort_keys=True), encoding="utf-8")


def update_ad_tracking(ads, tracking, ad_daily_index=None):
    """Store first_seen date for each ad.

    For brand-new ads, uses today. For ads with historical daily data (from the
    30-day per-ad pull), infers first_seen from the earliest day with spend > 0.
    This lets us backfill accurate ages on first run instead of showing every
    pre-existing ad as 0 days old.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    updated = False
    for ad in ads:
        ad_id = ad.get("id")
        if not ad_id:
            continue
        if ad.get("spend", 0) <= 0:
            continue
        if ad_id in tracking:
            continue

        first_seen = today
        # Try to infer from historical daily data
        if ad_daily_index:
            daily = ad_daily_index.get(ad_id, [])
            for d in daily:  # already sorted ascending by date
                if d.get("spend", 0) > 0 and d.get("date"):
                    first_seen = d["date"]
                    break

        tracking[ad_id] = first_seen
        updated = True
    return updated


def calculate_ad_age(ad_id, tracking):
    """Days since we first saw this ad with spend. Returns None if unknown."""
    first_seen = tracking.get(ad_id)
    if not first_seen:
        return None
    try:
        first_date = datetime.strptime(first_seen, "%Y-%m-%d")
        return (datetime.now() - first_date).days
    except ValueError:
        return None


# ============================================================
# NEW: BEST-WEEK CPL CALCULATION
# ============================================================

def build_ad_daily_index(ad_daily_data):
    """Group daily rows by ad_id for rolling-window calculations."""
    by_ad = {}
    for row in ad_daily_data.get("data", []):
        ad_id = row.get("ad_id")
        if not ad_id:
            continue
        leads, cpl = extract_leads_cpl(row)
        spend = float(row.get("spend", 0))
        by_ad.setdefault(ad_id, []).append({
            "date": row.get("date_start", ""),
            "spend": spend,
            "leads": leads,
            "cpl": cpl,
        })
    # Sort each ad's daily data by date
    for ad_id in by_ad:
        by_ad[ad_id].sort(key=lambda d: d["date"])
    return by_ad


def calculate_best_7day_cpl(ad_id, ad_daily_index):
    """Find the best 7-day rolling window CPL for this ad.

    Returns (best_cpl, best_window_end_date) or (None, None) if insufficient data.
    """
    daily = ad_daily_index.get(ad_id, [])
    if len(daily) < 3:  # Need at least a few days
        return None, None

    best_cpl = None
    best_window = None

    # Rolling 7-day windows
    for i in range(len(daily)):
        window = daily[i:i + 7]
        if len(window) < 3:  # Need at least 3 days in window
            break
        window_spend = sum(d["spend"] for d in window)
        window_leads = sum(d["leads"] for d in window)
        if window_leads == 0 or window_spend < 50:
            continue
        window_cpl = window_spend / window_leads
        if best_cpl is None or window_cpl < best_cpl:
            best_cpl = window_cpl
            best_window = window[-1]["date"]

    return best_cpl, best_window


# ============================================================
# UPDATED: age-aware verdict
# ============================================================

def verdict(spend, leads, cpl, frequency, age_days=None):
    """Age-aware verdict.

    Age gates:
      None or 0-3 days  -> LEARNING (protect new ads from premature kill)
      4-7 days          -> EARLY SIGNAL (watchlist, not kill yet)
      8-13 days         -> Full verdict logic
      14+ days          -> Full verdict + fatigue-age weighting
    """
    # Newborn protection
    if age_days is not None and age_days <= AD_AGE_LEARNING_MAX:
        return "LEARNING"

    # Early-signal window (4-7 days): only flag egregious waste
    if age_days is not None and age_days <= AD_AGE_EARLY_MAX:
        if spend >= KILL_SPEND_ZERO_LEADS * 2 and leads == 0:
            return "KILL"
        if leads >= MIN_LEADS_FOR_VERDICT and cpl > 0 and cpl < CPL_GOOD:
            return "WATCH"  # early winner, let it develop
        return "EARLY"

    # Original verdict logic (days 8+)
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


# ============================================================
# NEW: CBO BIAS + REFRESH STATUS
# ============================================================

def detect_cbo_bias(ads):
    """Return list of CBO bias alerts (one ad hogging ad set budget)."""
    alerts = []
    # Group ads by ad set
    by_adset = {}
    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        by_adset.setdefault(a.get("adset") or "Unknown", []).append(a)

    for adset_name, adset_ads in by_adset.items():
        if len(adset_ads) < CBO_BIAS_MIN_ADS:
            continue
        total_spend = sum(a["spend"] for a in adset_ads)
        if total_spend <= 0:
            continue
        for a in adset_ads:
            share = a["spend"] / total_spend
            if share >= CBO_BIAS_SPEND_SHARE:
                alerts.append(
                    f"CBO BIAS: {a['name']} has {share*100:.0f}% of ad set spend "
                    f"(${a['spend']:.0f} of ${total_spend:.0f}). Others may be starving."
                )
    return alerts


def get_latest_batch_info(ads, tracking):
    """Return (days_since_latest_batch, latest_batch_date, ad_count_in_batch).

    A "batch" = ads that first_seen within 3 days of each other.
    The "latest batch" is defined by the most recent first_seen date among active ads.
    """
    today = datetime.now()
    active_first_seens = []

    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        ad_id = a.get("id")
        first_seen = tracking.get(ad_id)
        if not first_seen:
            continue
        try:
            fs_date = datetime.strptime(first_seen, "%Y-%m-%d")
            active_first_seens.append((fs_date, ad_id))
        except ValueError:
            continue

    if not active_first_seens:
        return None, None, 0

    latest_date = max(fs for fs, _ in active_first_seens)
    days_since = (today - latest_date).days

    # Count ads in the "latest batch" (within 3 days of the latest date)
    batch_cutoff = latest_date - timedelta(days=3)
    batch_count = sum(1 for fs, _ in active_first_seens if fs >= batch_cutoff)

    return days_since, latest_date.strftime("%Y-%m-%d"), batch_count


def build_age_buckets(ads, tracking):
    """Categorize active ads by lifecycle stage."""
    buckets = {
        "learning": [],    # 0-3 days
        "early": [],       # 4-7 days
        "peak": [],        # 8-14 days
        "fatigue_watch": [],  # 15-21 days
        "dead": [],        # 22+ days
        "untracked": [],   # no first_seen
    }
    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        age = calculate_ad_age(a.get("id"), tracking)
        if age is None:
            buckets["untracked"].append(a)
        elif age <= AD_AGE_LEARNING_MAX:
            buckets["learning"].append(a)
        elif age <= AD_AGE_EARLY_MAX:
            buckets["early"].append(a)
        elif age <= AD_AGE_FATIGUE_CHECK:
            buckets["peak"].append(a)
        elif age <= 21:
            buckets["fatigue_watch"].append(a)
        else:
            buckets["dead"].append(a)
    return buckets


# ============================================================
# NEW: MONTHLY PACING
# ============================================================

def calculate_monthly_pacing(mtd_raw):
    """Extract month-to-date spend, leads, offline attributed, and pacing vs target."""
    data = mtd_raw.get("data", [])
    if not data:
        return {
            "spend": 0,
            "leads": 0,
            "offline_attributed": 0,
            "total_attributed": 0,
            "day_of_month": datetime.now().day,
            "days_in_month": 30,
            "pct_through_month": 0,
            "pct_to_lead_target": 0,
            "pct_to_spend_target": 0,
            "leads_needed": MONTHLY_LEAD_TARGET,
            "days_remaining": 30,
        }

    row = data[0]
    spend = float(row.get("spend", 0))
    leads, _ = extract_leads_cpl(row)
    offline = extract_offline_contacts(row)
    total_attr = leads + offline

    now = datetime.now()
    # Last day of current month
    if now.month == 12:
        next_month = now.replace(year=now.year + 1, month=1, day=1)
    else:
        next_month = now.replace(month=now.month + 1, day=1)
    last_day = (next_month - timedelta(days=1)).day
    days_remaining = last_day - now.day

    return {
        "spend": spend,
        "leads": leads,
        "offline_attributed": offline,
        "total_attributed": total_attr,
        "day_of_month": now.day,
        "days_in_month": last_day,
        "pct_through_month": (now.day / last_day) * 100,
        "pct_to_lead_target": (total_attr / MONTHLY_LEAD_TARGET) * 100 if MONTHLY_LEAD_TARGET > 0 else 0,
        "pct_to_spend_target": (spend / MONTHLY_SPEND_TARGET) * 100 if MONTHLY_SPEND_TARGET > 0 else 0,
        "leads_needed": max(0, MONTHLY_LEAD_TARGET - total_attr),
        "days_remaining": days_remaining,
    }


# ============================================================
# ALERTS (enhanced)
# ============================================================

def generate_alerts(ads, tracking, refresh_info):
    alerts = []

    # 1. Refresh due
    days_since_batch, batch_date, batch_count = refresh_info
    if days_since_batch is not None and days_since_batch >= REFRESH_DUE_DAYS:
        severity = "OVERDUE" if days_since_batch >= REFRESH_CYCLE_DAYS else "DUE SOON"
        alerts.append(
            f"REFRESH {severity}: Last batch launched {days_since_batch} days ago ({batch_date}, {batch_count} ads). "
            f"Target cycle is every {REFRESH_CYCLE_DAYS} days."
        )

    # 2. Per-ad alerts (age-aware)
    for a in ads:
        name = a["name"]
        spend = a["spend"]
        leads = a["leads"]
        cpl = a["cpl"]
        freq = a["frequency"]
        age = calculate_ad_age(a.get("id"), tracking)
        best_cpl = a.get("best_7day_cpl")

        # Skip egregious alerts for brand-new ads
        age_known = age is not None
        is_newborn = age_known and age <= AD_AGE_LEARNING_MAX
        is_early = age_known and age <= AD_AGE_EARLY_MAX

        if is_newborn:
            continue  # let newborns cook

        # Budget waste (more lenient on early ads)
        waste_threshold = KILL_SPEND_ZERO_LEADS * (2 if is_early else 1)
        if spend >= waste_threshold and leads == 0:
            age_note = f" (age {age}d)" if age_known else ""
            alerts.append(f"BUDGET WASTE: {name}{age_note} — ${spend:.0f} spent, 0 leads. Kill it.")
        elif leads > 0 and cpl > CPL_CONCERNING:
            alerts.append(f"CPL SPIKE: {name} — ${cpl:.0f} CPL on {leads} leads. Way above ${CPL_TARGET} target.")
        elif leads > 0 and cpl > CPL_ACCEPTABLE and not is_early:
            alerts.append(f"CPL WARNING: {name} — ${cpl:.0f} CPL. Above ${CPL_ACCEPTABLE} acceptable range.")

        # Frequency fatigue
        if freq > FATIGUE_FREQUENCY:
            alerts.append(f"CREATIVE FATIGUE: {name} — frequency at {freq:.1f} (threshold: {FATIGUE_FREQUENCY}).")

        # Best-week vs current-week fatigue (CPL regression)
        if best_cpl and cpl > 0 and leads >= MIN_LEADS_FOR_VERDICT:
            pct_worse = (cpl - best_cpl) / best_cpl
            if pct_worse >= BEST_WEEK_FATIGUE_THRESHOLD:
                alerts.append(
                    f"FATIGUE WATCH: {name} — current CPL ${cpl:.0f} is {pct_worse*100:.0f}% worse than best ${best_cpl:.0f}."
                )

        # Winner signal
        if leads >= SCALE_LEADS and cpl > 0 and cpl < SCALE_CPL:
            alerts.append(f"WINNER: {name} — ${cpl:.0f} CPL on {leads} leads. Consider scaling.")

    # 3. CBO bias
    alerts.extend(detect_cbo_bias(ads))

    return alerts


def generate_recommendations(campaigns, ads, refresh_info, pacing):
    recs = []
    kills = [a for a in ads if a.get("verdict") == "KILL"]
    winners = [a for a in ads if a.get("verdict") == "SCALE"]
    total_kill_spend = sum(a["spend"] for a in kills)
    total_kill_leads = sum(a["leads"] for a in kills)

    # Refresh reminder first (highest priority)
    days_since_batch, _, _ = refresh_info
    if days_since_batch is not None and days_since_batch >= REFRESH_DUE_DAYS:
        recs.append(
            f"Ship a new creative batch ({days_since_batch} days since last). "
            f"Target: 2-3 new ads, pause 2 worst from current batch."
        )

    if kills:
        kill_names = ", ".join(a["name"] for a in kills[:5])
        recs.append(
            f"Kill {len(kills)} underperformers ({kill_names}). "
            f"Combined ${total_kill_spend:.0f} spend, {total_kill_leads} leads. Redirect budget to winners."
        )

    if winners:
        for w in winners[:2]:
            recs.append(f"Scale {w['name']} — ${w['cpl']:.0f} CPL on {w['leads']} leads. Room to grow.")

    # Pacing rec
    if pacing["day_of_month"] >= 10 and pacing["pct_to_lead_target"] < pacing["pct_through_month"] - 15:
        recs.append(
            f"Monthly pacing BEHIND: {pacing['pct_to_lead_target']:.0f}% of lead target at "
            f"{pacing['pct_through_month']:.0f}% through month. Need {pacing['leads_needed']} more leads in "
            f"{pacing['days_remaining']} days."
        )

    for c in campaigns:
        if c["frequency"] > 3.0 and c["cpl"] > CPL_ACCEPTABLE:
            recs.append(f"Creative refresh needed on {c['name']} — frequency at {c['frequency']:.1f}, CPL at ${c['cpl']:.0f}.")

    if not recs:
        recs.append("No immediate actions. Monitor for another 24-48 hours.")
    return recs[:5]


# ============================================================
# DIGEST BUILDER (enhanced)
# ============================================================

def build_digest(campaigns, ads, daily, offline_entries, days, tracking, refresh_info, pacing):
    today = datetime.now().strftime("%B %d, %Y")
    total_spend = sum(c["spend"] for c in campaigns)
    total_leads = sum(c["leads"] for c in campaigns)
    total_offline_attributed = sum(c.get("offline_attributed", 0) for c in campaigns)
    total_offline_sent = len(offline_entries)
    blended_cpl = total_spend / total_leads if total_leads > 0 else 0

    lines = [f"# ASH Ads Daily Digest — {today}", ""]

    # ----- NEW: Creative Refresh Status (top banner) -----
    days_since_batch, batch_date, batch_count = refresh_info
    lines.append("## Creative Refresh Status")
    lines.append("")
    if days_since_batch is None:
        lines.append("*No tracked batches yet. Will begin tracking from this run forward.*")
    else:
        if days_since_batch >= REFRESH_CYCLE_DAYS:
            banner = f"🚨 **REFRESH OVERDUE** — last batch was {days_since_batch} days ago ({batch_date})"
        elif days_since_batch >= REFRESH_DUE_DAYS:
            banner = f"⚠️ **REFRESH DUE SOON** — last batch was {days_since_batch} days ago ({batch_date})"
        else:
            banner = f"✅ Last batch launched {days_since_batch} days ago ({batch_date}, {batch_count} ads)"
        lines.append(banner)
        lines.append("")

    buckets = build_age_buckets(ads, tracking)
    lines.append("| Lifecycle Stage | Ads | Notes |")
    lines.append("|-----------------|-----|-------|")
    lines.append(f"| Learning (0-{AD_AGE_LEARNING_MAX}d) | {len(buckets['learning'])} | Too new to judge |")
    lines.append(f"| Early signal ({AD_AGE_LEARNING_MAX+1}-{AD_AGE_EARLY_MAX}d) | {len(buckets['early'])} | Watchlist, not kill |")
    lines.append(f"| Peak ({AD_AGE_EARLY_MAX+1}-{AD_AGE_FATIGUE_CHECK}d) | {len(buckets['peak'])} | Full verdict applies |")
    lines.append(f"| Fatigue watch ({AD_AGE_FATIGUE_CHECK+1}-21d) | {len(buckets['fatigue_watch'])} | Check frequency + best-vs-current |")
    lines.append(f"| Dead zone (22d+) | {len(buckets['dead'])} | Kill and replace |")
    if buckets["untracked"]:
        lines.append(f"| Untracked | {len(buckets['untracked'])} | Pre-existing ads, no age data |")
    lines.append("")

    # ----- NEW: Monthly Pacing -----
    lines.append("## Monthly Pacing")
    lines.append("")
    lines.append("| Metric | Progress | Target |")
    lines.append("|--------|----------|--------|")
    lines.append(
        f"| Leads (website + offline attributed) | {pacing['total_attributed']} "
        f"({pacing['pct_to_lead_target']:.0f}%) | {MONTHLY_LEAD_TARGET} |"
    )
    lines.append(
        f"| Spend | ${pacing['spend']:.0f} "
        f"({pacing['pct_to_spend_target']:.0f}%) | ${MONTHLY_SPEND_TARGET:,} |"
    )
    lines.append(
        f"| Days elapsed | {pacing['day_of_month']} / {pacing['days_in_month']} "
        f"({pacing['pct_through_month']:.0f}%) | — |"
    )
    lines.append("")
    # Pacing verdict
    pct_diff = pacing["pct_to_lead_target"] - pacing["pct_through_month"]
    if pct_diff >= 5:
        lines.append(f"✅ **AHEAD** — need {pacing['leads_needed']} more leads in {pacing['days_remaining']} days.")
    elif pct_diff >= -15:
        lines.append(f"➖ **ON TRACK** — need {pacing['leads_needed']} more leads in {pacing['days_remaining']} days.")
    else:
        lines.append(f"🚨 **BEHIND** — need {pacing['leads_needed']} more leads in {pacing['days_remaining']} days.")
    lines.append("")

    # Campaign snapshot (unchanged)
    lines.append(f"## Campaign Snapshot (Last {days} Days)")
    lines.append("")
    lines.append("| Campaign | Spend | Leads | Offline | CPL | Effective CPL | Frequency | CTR | Status |")
    lines.append("|----------|-------|-------|---------|-----|---------------|-----------|-----|--------|")
    for c in campaigns:
        cpl_str = f"${c['cpl']:.0f}" if c['cpl'] > 0 else "—"
        offline = c.get('offline_attributed', 0)
        total_attr = c['leads'] + offline
        eff_cpl = c['spend'] / total_attr if total_attr > 0 else 0
        eff_cpl_str = f"${eff_cpl:.0f}" if eff_cpl > 0 else "—"
        freq_str = f"{c['frequency']:.2f}" if c['frequency'] > 0 else "—"
        ctr_str = f"{c['ctr']:.2f}%" if c['ctr'] > 0 else "—"
        status = "ACTIVE" if c["spend"] > 10 else "NEW"
        lines.append(f"| {c['name']} | ${c['spend']:.2f} | {c['leads']} | {offline} | {cpl_str} | {eff_cpl_str} | {freq_str} | {ctr_str} | {status} |")
    lines.append("")
    lines.append("*CPL = website leads only. Effective CPL = includes Meta-attributed offline contacts.*")
    if total_offline_sent > 0:
        lines.append(f"*Offline sent to Meta this period: {total_offline_sent}. Match rate: {(total_offline_attributed / total_offline_sent * 100):.0f}%*")
    lines.append("")

    # Ad-level (NEW: Age, Best CPL, vs Best columns)
    lines.append("## Ad-Level Performance")
    lines.append("")
    lines.append("| Ad Name | Campaign | Age | Spend | Leads | Offline | CPL | Best CPL | vs Best | Eff CPL | CTR | Verdict |")
    lines.append("|---------|----------|-----|-------|-------|---------|-----|----------|---------|---------|-----|---------|")
    # Sort: learning first, then by CPL ascending
    def sort_key(a):
        age = calculate_ad_age(a.get("id"), tracking)
        age_sort = age if age is not None else 999
        cpl_sort = a["cpl"] if a["cpl"] > 0 else 99999
        return (age_sort, cpl_sort)
    sorted_ads = sorted(ads, key=sort_key)
    for a in sorted_ads:
        age = calculate_ad_age(a.get("id"), tracking)
        age_str = f"{age}d" if age is not None else "?"
        cpl_str = f"${a['cpl']:.0f}" if a["cpl"] > 0 else "—"
        best_cpl = a.get("best_7day_cpl")
        best_str = f"${best_cpl:.0f}" if best_cpl else "—"
        if best_cpl and a["cpl"] > 0:
            pct_diff = (a["cpl"] - best_cpl) / best_cpl * 100
            vs_best_str = f"+{pct_diff:.0f}%" if pct_diff > 0 else f"{pct_diff:.0f}%"
        else:
            vs_best_str = "—"
        offline = a.get('offline_attributed', 0)
        total_attr = a['leads'] + offline
        eff_cpl = a['spend'] / total_attr if total_attr > 0 else 0
        eff_cpl_str = f"${eff_cpl:.0f}" if eff_cpl > 0 else "—"
        ctr_str = f"{a['ctr']:.2f}%" if a["ctr"] > 0 else "—"
        v = a["verdict"]
        v_fmt = f"**{v}**" if v in ("SCALE", "KILL") else v
        lines.append(
            f"| {a['name']} | {a['campaign']} | {age_str} | ${a['spend']:.2f} | {a['leads']} | {offline} | "
            f"{cpl_str} | {best_str} | {vs_best_str} | {eff_cpl_str} | {ctr_str} | {v_fmt} |"
        )
    lines.append("")

    # Alerts (enhanced)
    alerts = generate_alerts(ads, tracking, refresh_info)
    lines.append("## Alerts")
    lines.append("")
    if alerts:
        for alert in alerts:
            lines.append(f"- {alert}")
    else:
        lines.append("- No alerts. All metrics within acceptable ranges.")
    lines.append("")

    # Trends (unchanged)
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
    avg_daily = total_spend / max(days, 1)
    lines.append(f"- Average daily spend: ${avg_daily:.2f}")
    lines.append(f"- Total leads over period: {total_leads}")
    lines.append("")

    # Recommendations (enhanced)
    recs = generate_recommendations(campaigns, ads, refresh_info, pacing)
    lines.append("## Recommendations")
    lines.append("")
    for i, rec in enumerate(recs, 1):
        lines.append(f"{i}. {rec}")
    lines.append("")

    # Offline Leads Audit Trail (unchanged)
    lines.append(f"## Offline Leads Audit Trail ({days}d)")
    lines.append("")
    lines.append(f"*Full list of HCP leads sent to Meta's offline dataset. Meta attributed {total_offline_attributed} of {total_offline_sent} to ad exposure.*")
    lines.append("")
    if offline_entries:
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

    # Raw numbers (unchanged)
    lines.append("## Raw Numbers")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total spend ({days}d) | ${total_spend:.2f} |")
    lines.append(f"| Total website leads ({days}d) | {total_leads} |")
    lines.append(f"| Total offline leads sent to Meta ({days}d) | {total_offline_sent} |")
    lines.append(f"| Total offline leads Meta attributed ({days}d) | {total_offline_attributed} |")
    if total_offline_sent > 0:
        lines.append(f"| Meta offline match rate | {(total_offline_attributed / total_offline_sent * 100):.0f}% |")
    total_attributed = total_leads + total_offline_attributed
    lines.append(f"| Total attributed conversions | {total_attributed} |")
    lines.append(f"| Blended CPL (website only) | ${blended_cpl:.2f} |" if total_leads > 0 else f"| Blended CPL (website only) | — |")
    if total_attributed > 0:
        lines.append(f"| Blended CPL (website + offline) | ${total_spend / total_attributed:.2f} |")
    lines.append(f"| Active ads | {len([a for a in ads if a['spend'] > 0])} |")
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

    # --- UNCHANGED pulls ---
    print("Pulling campaign data...", file=sys.stderr)
    camp_raw = pull_insights(token, account_id, args.days, "campaign")
    print("Pulling ad data...", file=sys.stderr)
    ad_raw = pull_insights(token, account_id, args.days, "ad")
    print("Pulling daily trend...", file=sys.stderr)
    daily_raw = pull_daily(token, account_id, args.days)

    # --- NEW additive pulls ---
    print("Pulling per-ad daily breakdown (30d) for best-window CPL...", file=sys.stderr)
    ad_daily_raw = pull_ad_daily(token, account_id, 30)
    ad_daily_index = build_ad_daily_index(ad_daily_raw)

    print("Pulling month-to-date totals for pacing...", file=sys.stderr)
    mtd_raw = pull_month_to_date(token, account_id)
    pacing = calculate_monthly_pacing(mtd_raw)

    # --- UNCHANGED parsing ---
    campaigns = []
    for row in camp_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        offline_attributed = extract_offline_contacts(row)
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
            "offline_attributed": offline_attributed,
        })

    # Build ad list (with new fields added)
    ads = []
    for row in ad_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        offline_attributed = extract_offline_contacts(row)
        spend = float(row.get("spend", 0))
        freq = float(row.get("frequency", 0))
        ad_id = row.get("ad_id")
        ads.append({
            "id": ad_id,
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
            "offline_attributed": offline_attributed,
        })

    # --- NEW: age tracking + best-week CPL + age-aware verdicts ---
    tracking = load_ad_tracking()
    updated = update_ad_tracking(ads, tracking, ad_daily_index)
    if updated:
        save_ad_tracking(tracking)
        print(f"Tracked {len([a for a in ads if a.get('spend', 0) > 0 and a.get('id') in tracking])} active ads.", file=sys.stderr)

    for a in ads:
        age = calculate_ad_age(a.get("id"), tracking)
        best_cpl, best_window = calculate_best_7day_cpl(a.get("id"), ad_daily_index)
        a["age_days"] = age
        a["best_7day_cpl"] = best_cpl
        a["best_window_end"] = best_window
        a["verdict"] = verdict(a["spend"], a["leads"], a["cpl"], a["frequency"], age)

    refresh_info = get_latest_batch_info(ads, tracking)

    # --- UNCHANGED daily parsing ---
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

    # --- UNCHANGED offline log pull ---
    print("Pulling latest offline leads log...", file=sys.stderr)
    pull_latest_log()
    offline_entries = load_offline_log(args.days)
    print(f"Found {len(offline_entries)} offline entries in last {args.days} days", file=sys.stderr)

    # Build digest with all the new inputs
    digest = build_digest(
        campaigns, ads, daily, offline_entries, args.days,
        tracking, refresh_info, pacing,
    )

    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = VAULT_DIR / f"{date_str}.md"
    out_path.write_text(digest, encoding="utf-8")
    print(f"Digest written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
