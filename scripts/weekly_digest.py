"""
ASH Ads Weekly Digest — deep analysis (2026-04-22 methodology).

Complements the lean daily digest with the analysis that's too noisy to look at
every day but valuable once a week:

  - Week-over-week rollup (spend, leads, blended CPL)
  - Ad lifecycle audit (age bucket distribution, best-week CPL, vs-best %)
  - Trend: first half vs second half of the period
  - CBO spend-share table (full, not just worst offender)
  - Offline match rate + full audit trail
  - Deeper recommendations

Shares data-pull functions with daily_digest.py so API calls stay consistent.

Usage:
    python weekly_digest.py              # Default: last 7 days
    python weekly_digest.py --days 14    # Rolling 2-week review
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from daily_digest import (
    # config
    REPO_DIR, CPL_TARGET, FREQUENCY_CAP, AGE_LEARNING_MAX,
    REFRESH_CYCLE_DAYS, REFRESH_DUE_DAYS, MIN_SPEND_FOR_KILL,
    MONTHLY_LEAD_TARGET, MONTHLY_SPEND_TARGET,
    CBO_BIAS_SPEND_SHARE, CBO_BIAS_MIN_ADS,
    # pulls
    load_api_keys, pull_insights, pull_daily, pull_ad_daily,
    pull_month_to_date, pull_latest_log, load_offline_log,
    extract_leads_cpl, extract_offline_contacts,
    # tracking
    load_ad_tracking, save_ad_tracking, update_ad_tracking,
    calculate_ad_age, get_week1_ctr, build_ad_daily_index,
    calculate_week1_ctr,
    # verdict + analysis
    three_test_verdict, detect_cbo_bias, get_latest_batch_info,
    calculate_monthly_pacing,
)


VAULT_DIR = Path.home() / "Documents" / "Obsidian Vault" / "Business" / "HVAC Lead Gen" / "Clients" / "Ash Cooling & Heating" / "Weekly Reports"


# ============================================================
# WEEKLY-ONLY ANALYSIS
# ============================================================

def calculate_best_7day_cpl(ad_id, ad_daily_index):
    """Best rolling 7-day CPL window in the ad's history. (best_cpl, end_date) or (None, None)."""
    daily = ad_daily_index.get(ad_id, [])
    if len(daily) < 3:
        return None, None
    best_cpl = None
    best_window_end = None
    for i in range(len(daily)):
        window = daily[i:i + 7]
        if len(window) < 3:
            break
        w_spend = sum(d["spend"] for d in window)
        w_leads = sum(d["leads"] for d in window)
        if w_leads == 0 or w_spend < 50:
            continue
        w_cpl = w_spend / w_leads
        if best_cpl is None or w_cpl < best_cpl:
            best_cpl = w_cpl
            best_window_end = window[-1]["date"]
    return best_cpl, best_window_end


def build_age_buckets(ads, tracking):
    """Learning / peak / fatigue-watch / dead-zone."""
    buckets = {
        "learning": [],      # 0-6 days
        "peak": [],          # 7-14 days
        "fatigue_watch": [], # 15-21 days
        "dead_zone": [],     # 22+ days
        "untracked": [],
    }
    for a in ads:
        if a.get("spend", 0) <= 0:
            continue
        age = calculate_ad_age(a.get("id"), tracking)
        if age is None:
            buckets["untracked"].append(a)
        elif age < AGE_LEARNING_MAX:
            buckets["learning"].append(a)
        elif age <= 14:
            buckets["peak"].append(a)
        elif age <= 21:
            buckets["fatigue_watch"].append(a)
        else:
            buckets["dead_zone"].append(a)
    return buckets


def period_trend(daily_rows):
    """First half vs second half CPL comparison of a list of daily campaign rows."""
    if len(daily_rows) < 2:
        return None
    mid = len(daily_rows) // 2
    first = daily_rows[:mid]
    second = daily_rows[mid:]
    def cpl(rows):
        s = sum(float(r.get("spend", 0)) for r in rows)
        l = sum(extract_leads_cpl(r)[0] for r in rows)
        return (s / l) if l > 0 else 0
    f_cpl = cpl(first)
    s_cpl = cpl(second)
    if f_cpl <= 0 or s_cpl <= 0:
        return None
    return {"first_cpl": f_cpl, "second_cpl": s_cpl}


def generate_recommendations(ads, buckets, refresh_info, pacing, cbo_alerts, trend):
    recs = []
    days_since, _, _ = refresh_info

    # Refresh
    if days_since is not None and days_since >= REFRESH_DUE_DAYS:
        recs.append(
            f"Ship a new creative batch ({days_since}d since last). "
            f"Target: 2-3 new concepts with unique ad text per concept, then pause the 2 worst."
        )

    # Kills
    kills = [a for a in ads if a.get("verdict") == "KILL"]
    if kills:
        names = ", ".join(a["name"] for a in kills)
        kill_spend = sum(a["spend"] for a in kills)
        recs.append(
            f"Kill {len(kills)} ads that failed the 3-test: {names}. "
            f"Combined ${kill_spend:.0f} spent this week."
        )

    # Dead zone replacements
    if buckets["dead_zone"]:
        recs.append(
            f"{len(buckets['dead_zone'])} ads in dead zone (22+ days). Replace regardless of current CPL — "
            f"useful life is 3-5 weeks."
        )

    # Pacing
    if pacing["day_of_month"] >= 7 and pacing["pct_to_lead_target"] < pacing["pct_through_month"] - 15:
        recs.append(
            f"Monthly pacing behind: {pacing['pct_to_lead_target']:.0f}% of lead target at "
            f"{pacing['pct_through_month']:.0f}% through month. Need {pacing['leads_needed']} more leads "
            f"in {pacing['days_remaining']}d."
        )

    # CBO bias — if >1 ad set takes this much spend share, it's a pattern
    if cbo_alerts:
        recs.append(
            f"Test ABO: {len(cbo_alerts)} CBO bias event{'s' if len(cbo_alerts) != 1 else ''} this week. "
            f"Forcing equal spend may unearth viable creatives the algo is starving."
        )

    # Trend
    if trend and trend["second_cpl"] > trend["first_cpl"] * 1.15:
        recs.append(
            f"CPL is worsening within the period (${trend['first_cpl']:.0f} → ${trend['second_cpl']:.0f}). "
            f"Likely batch fatigue — ship the next batch on schedule."
        )

    if not recs:
        recs.append("No immediate actions. Ads performing within targets.")
    return recs[:6]


# ============================================================
# BUILDER
# ============================================================

def build_weekly(ads, daily, offline_entries, days, tracking, refresh_info,
                 pacing, cbo_alerts, ad_daily_index, trend):
    today = datetime.now().strftime("%B %d, %Y")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%B %d")
    end_date = datetime.now().strftime("%B %d, %Y")

    total_spend = sum(a["spend"] for a in ads)
    total_leads = sum(a["leads"] for a in ads)
    total_offline_attr = sum(a.get("offline_attributed", 0) for a in ads)
    total_attributed = total_leads + total_offline_attr
    blended_cpl = total_spend / total_leads if total_leads > 0 else 0
    effective_cpl = total_spend / total_attributed if total_attributed > 0 else 0

    lines = [f"# ASH Ads Weekly — {start_date} – {end_date}", ""]
    lines.append(f"*Generated {today}. Lookback: {days} days.*")
    lines.append("")

    # --- Summary ---
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total spend | ${total_spend:.2f} |")
    lines.append(f"| Website leads | {total_leads} |")
    lines.append(f"| Offline attributed | {total_offline_attr} |")
    lines.append(f"| Total attributed | {total_attributed} |")
    if total_leads > 0:
        lines.append(f"| Blended CPL (website) | ${blended_cpl:.2f} |")
    if total_attributed > 0:
        lines.append(f"| Effective CPL (web + offline) | ${effective_cpl:.2f} |")
    lines.append(f"| Monthly pacing | {pacing['total_attributed']}/{MONTHLY_LEAD_TARGET} leads ({pacing['pct_to_lead_target']:.0f}%) at {pacing['pct_through_month']:.0f}% through month |")
    lines.append("")

    # --- Ad lifecycle audit ---
    buckets = build_age_buckets(ads, tracking)
    lines.append("## Ad Lifecycle Audit")
    lines.append("")
    lines.append("| Stage | Count | Notes |")
    lines.append("|-------|-------|-------|")
    lines.append(f"| Learning (0-{AGE_LEARNING_MAX-1}d) | {len(buckets['learning'])} | Too new to judge |")
    lines.append(f"| Peak (7-14d) | {len(buckets['peak'])} | Prime performance window |")
    lines.append(f"| Fatigue watch (15-21d) | {len(buckets['fatigue_watch'])} | Monitor CTR/freq |")
    lines.append(f"| Dead zone (22d+) | {len(buckets['dead_zone'])} | Replace regardless of current CPL |")
    if buckets["untracked"]:
        lines.append(f"| Untracked | {len(buckets['untracked'])} | Pre-existing, no age data |")
    lines.append("")

    # --- Per-ad deep table ---
    lines.append("### Per-Ad Performance")
    lines.append("")
    lines.append("| Ad | Age | Spend | Leads | CPL | Best 7d CPL | vs Best | Freq | CTR | W1 CTR | Verdict |")
    lines.append("|----|-----|-------|-------|-----|-------------|---------|------|-----|--------|---------|")

    order = {"KILL": 0, "KEEP": 1, "LEARNING": 2}
    active = [a for a in ads if a.get("spend", 0) > 0]
    active.sort(key=lambda a: (order.get(a["verdict"], 3), -a["spend"]))

    for a in active:
        age = calculate_ad_age(a.get("id"), tracking)
        age_str = f"{age}d" if age is not None else "?"
        cpl_str = f"${a['cpl']:.0f}" if a["cpl"] > 0 else "—"

        best_cpl, _ = calculate_best_7day_cpl(a.get("id"), ad_daily_index)
        best_str = f"${best_cpl:.0f}" if best_cpl else "—"
        if best_cpl and a["cpl"] > 0:
            delta = (a["cpl"] - best_cpl) / best_cpl * 100
            vs_best = f"+{delta:.0f}%" if delta >= 0 else f"{delta:.0f}%"
        else:
            vs_best = "—"

        freq = f"{a['frequency']:.1f}" if a["frequency"] > 0 else "—"
        ctr = f"{a['ctr']:.2f}%" if a["ctr"] > 0 else "—"
        w1 = get_week1_ctr(a.get("id"), tracking)
        w1_str = f"{w1:.2f}%" if w1 else "—"
        v = a["verdict"]
        v_fmt = f"**{v}**" if v == "KILL" else v

        lines.append(
            f"| {a['name']} | {age_str} | ${a['spend']:.0f} | {a['leads']} | {cpl_str} | "
            f"{best_str} | {vs_best} | {freq} | {ctr} | {w1_str} | {v_fmt} |"
        )
    lines.append("")

    # --- Trend ---
    lines.append("## Period Trend")
    lines.append("")
    if trend:
        direction = "IMPROVING" if trend["second_cpl"] < trend["first_cpl"] * 0.9 else \
                    "WORSENING" if trend["second_cpl"] > trend["first_cpl"] * 1.1 else \
                    "STABLE"
        lines.append(f"- First half CPL: ${trend['first_cpl']:.2f}")
        lines.append(f"- Second half CPL: ${trend['second_cpl']:.2f}")
        lines.append(f"- Direction: **{direction}**")
    else:
        lines.append("- Insufficient daily data for trend comparison.")
    lines.append("")

    # --- CBO spend share ---
    lines.append("## CBO Spend Share")
    lines.append("")
    by_adset = {}
    for a in active:
        by_adset.setdefault(a.get("adset") or "Unknown", []).append(a)
    any_cbo = False
    for adset_name, adset_ads in by_adset.items():
        if len(adset_ads) < 2:
            continue
        total = sum(a["spend"] for a in adset_ads)
        if total <= 0:
            continue
        any_cbo = True
        lines.append(f"### {adset_name}")
        lines.append("")
        lines.append("| Ad | Spend | Share |")
        lines.append("|----|-------|-------|")
        for a in sorted(adset_ads, key=lambda x: -x["spend"]):
            share = a["spend"] / total * 100
            flag = " 🚩" if share >= CBO_BIAS_SPEND_SHARE * 100 else ""
            lines.append(f"| {a['name']} | ${a['spend']:.0f} | {share:.0f}%{flag} |")
        lines.append("")
    if not any_cbo:
        lines.append("*Only one ad set with >1 ad — CBO bias check not applicable.*")
        lines.append("")
    lines.append(
        f"*Flag at {CBO_BIAS_SPEND_SHARE*100:.0f}%+ share. "
        f"If bias persists, test ABO to force equal spend across creatives.*"
    )
    lines.append("")

    # --- Offline match rate ---
    total_offline_sent = len(offline_entries)
    received = sum(1 for e in offline_entries if e.get("meta_events_received", 0) > 0)
    match_rate = (received / total_offline_sent * 100) if total_offline_sent > 0 else 0

    lines.append("## Offline Leads")
    lines.append("")
    lines.append(f"- Sent to Meta: **{total_offline_sent}**")
    lines.append(f"- Received by Meta: **{received}**")
    lines.append(f"- Meta-attributed to ad exposure: **{total_offline_attr}**")
    if total_offline_sent > 0:
        lines.append(f"- Receive rate: {match_rate:.0f}%")
        if total_offline_attr > 0:
            lines.append(f"- Attribution rate (of received): {(total_offline_attr / received * 100) if received else 0:.0f}%")
    lines.append("")

    if offline_entries:
        lines.append("### Audit Trail")
        lines.append("")
        lines.append("| Date | Name | Phone | Email | City | Fields | Received |")
        lines.append("|------|------|-------|-------|------|--------|----------|")
        for e in sorted(offline_entries, key=lambda x: x.get("timestamp", ""), reverse=True):
            ts = e.get("timestamp", "")
            date_str = ts.split("T")[0] if ts else "—"
            name = f"{e.get('first_name', '')} {e.get('last_name', '')}".strip() or "—"
            phone = e.get("phone", "") or "—"
            email = e.get("email", "") or "—"
            city = e.get("city", "") or "—"
            fields = ", ".join(e.get("matched_fields", []))
            rec = "✓" if e.get("meta_events_received", 0) > 0 else "✗"
            lines.append(f"| {date_str} | {name} | {phone} | {email} | {city} | {fields} | {rec} |")
        lines.append("")

    # --- Recommendations ---
    recs = generate_recommendations(ads, buckets, refresh_info, pacing, cbo_alerts, trend)
    lines.append("## Recommendations")
    lines.append("")
    for i, r in enumerate(recs, 1):
        lines.append(f"{i}. {r}")
    lines.append("")

    return "\n".join(lines)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    args = parser.parse_args()

    token, account_id = load_api_keys()
    if not token or not account_id:
        print("Missing API keys", file=sys.stderr)
        sys.exit(1)

    print("Pulling ad-level insights...", file=sys.stderr)
    ad_raw = pull_insights(token, account_id, args.days, "ad")

    print("Pulling daily campaign trend...", file=sys.stderr)
    daily_raw = pull_daily(token, account_id, args.days)

    print("Pulling per-ad 30d history for best-window CPL + week-1 CTR...", file=sys.stderr)
    ad_daily_raw = pull_ad_daily(token, account_id, 30)
    ad_daily_index = build_ad_daily_index(ad_daily_raw)

    print("Pulling month-to-date for pacing...", file=sys.stderr)
    mtd_raw = pull_month_to_date(token, account_id)
    pacing = calculate_monthly_pacing(mtd_raw)

    # Build ads
    ads = []
    for row in ad_raw.get("data", []):
        leads, cpl = extract_leads_cpl(row)
        offline = extract_offline_contacts(row)
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
            "offline_attributed": offline,
        })

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

    # Daily campaign trend for first-vs-second-half comparison
    daily_rows = daily_raw.get("data", [])
    trend = period_trend(daily_rows)

    # Offline log
    print("Pulling latest offline leads log...", file=sys.stderr)
    pull_latest_log()
    offline_entries = load_offline_log(args.days)

    report = build_weekly(
        ads, daily_rows, offline_entries, args.days,
        tracking, refresh_info, pacing, cbo_alerts,
        ad_daily_index, trend,
    )

    VAULT_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    out_path = VAULT_DIR / f"{date_str}.md"
    out_path.write_text(report, encoding="utf-8")
    print(f"Weekly report written to {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
