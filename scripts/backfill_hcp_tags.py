"""
One-time backfill: sync GHL campaign tags into HCP customers.

For every GHL contact that has a campaign tag (beat-a-quote, blowout-sale,
summer-sale-2026, valentines-sale), find the corresponding HCP customer by
phone, and add the matching HCP tag if it's not already there.

This is the reverse direction of the webhook (which goes HCP → GHL) and
catches all the historical leads that came in before the webhook was set up.

Usage:
    python backfill_hcp_tags.py              # Live run
    python backfill_hcp_tags.py --dry-run    # Show what would change
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path


HCP_BASE = "https://api.housecallpro.com"
GHL_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"

# Map GHL tag → HCP tag (same as the webhook)
TAG_MAP = {
    "beat-a-quote": "beat-a-quote-lp",
    "blowout-sale": "blowout-pack-unit-lp",
    "summer-sale-2026": "summer-sale-2026",
    "valentines-sale": "valentines-sale",
}


def load_keys():
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


def ghl_search_contacts(token, location_id, page_after=None, limit=100):
    body = {"locationId": location_id, "pageLimit": limit}
    if page_after:
        body["searchAfter"] = page_after
    req = urllib.request.Request(
        f"{GHL_BASE}/contacts/search",
        method="POST",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Version": GHL_API_VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "ash-backfill/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"GHL search error: {e.code} {e.read().decode()[:200]}", file=sys.stderr)
        return {}


def ghl_get_all_tagged_contacts(token, location_id):
    """Paginate through ALL GHL contacts and return only those with campaign tags."""
    all_tagged = []
    after = None
    page = 1

    while True:
        data = ghl_search_contacts(token, location_id, page_after=after, limit=100)
        contacts = data.get("contacts", [])
        if not contacts:
            break

        for c in contacts:
            tags = c.get("tags") or []
            campaign_tags = [t for t in tags if t in TAG_MAP]
            if campaign_tags:
                all_tagged.append({
                    "id": c.get("id"),
                    "name": f"{c.get('firstName') or ''} {c.get('lastName') or ''}".strip(),
                    "phone": c.get("phone"),
                    "email": c.get("email"),
                    "ghl_tags": tags,
                    "campaign_tags": campaign_tags,
                })

        print(f"  GHL page {page}: {len(contacts)} contacts ({sum(1 for c in contacts if any(t in TAG_MAP for t in (c.get('tags') or [])))} with campaign tags)", file=sys.stderr)

        # Pagination via searchAfter
        if len(contacts) < 100:
            break
        after = contacts[-1].get("searchAfter")
        if not after:
            break
        page += 1
        time.sleep(0.1)

    return all_tagged


def hcp_find_customer_by_phone(phone, hcp_key, customer_index):
    """Look up an HCP customer by normalized phone using prebuilt index."""
    return customer_index.get(normalize_phone(phone))


def hcp_get_all_customers(hcp_key):
    """Pull all HCP customers and index by normalized phone."""
    customers = []
    index = {}
    page = 1
    while True:
        req = urllib.request.Request(
            f"{HCP_BASE}/customers?page={page}&page_size=100",
            headers={"Authorization": f"Token {hcp_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            print(f"HCP error: {e}", file=sys.stderr)
            break

        page_customers = data.get("customers", [])
        if not page_customers:
            break

        for c in page_customers:
            customers.append(c)
            for phone_field in ("mobile_number", "home_number", "work_number"):
                p = normalize_phone(c.get(phone_field))
                if p:
                    index[p] = c

        total_pages = data.get("total_pages", 1)
        print(f"  HCP page {page}/{total_pages}: {len(page_customers)} customers", file=sys.stderr)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.1)

    return customers, index


def hcp_patch_customer_tags(customer_id, tags, hcp_key):
    req = urllib.request.Request(
        f"{HCP_BASE}/customers/{customer_id}",
        method="PATCH",
        data=json.dumps({"tags": tags}).encode(),
        headers={
            "Authorization": f"Token {hcp_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Don't actually PATCH HCP")
    args = parser.parse_args()

    keys = load_keys()
    hcp_key = keys.get("HCP_API_KEY")
    ghl_key = keys.get("GHL_API_KEY")
    ghl_location = keys.get("GHL_LOCATION_ID")

    if not all([hcp_key, ghl_key, ghl_location]):
        print("Missing API keys", file=sys.stderr)
        sys.exit(1)

    print("=== Step 1: Fetching GHL contacts with campaign tags ===", file=sys.stderr)
    tagged = ghl_get_all_tagged_contacts(ghl_key, ghl_location)
    print(f"Found {len(tagged)} GHL contacts with campaign tags\n", file=sys.stderr)

    if not tagged:
        print("No tagged contacts found in GHL.", file=sys.stderr)
        return

    print("=== Step 2: Indexing all HCP customers ===", file=sys.stderr)
    customers, hcp_index = hcp_get_all_customers(hcp_key)
    print(f"Indexed {len(hcp_index)} HCP customers by phone\n", file=sys.stderr)

    print("=== Step 3: Matching and updating ===", file=sys.stderr)
    stats = {
        "total_ghl_tagged": len(tagged),
        "no_phone": 0,
        "no_hcp_match": 0,
        "already_tagged": 0,
        "would_update": 0,
        "updated": 0,
        "failed": 0,
    }

    actions = []

    for ghl_contact in tagged:
        if not ghl_contact["phone"]:
            stats["no_phone"] += 1
            continue

        hcp_customer = hcp_find_customer_by_phone(ghl_contact["phone"], hcp_key, hcp_index)
        if not hcp_customer:
            stats["no_hcp_match"] += 1
            continue

        # Map GHL campaign tags to HCP tags
        hcp_tags_to_add = sorted({TAG_MAP[t] for t in ghl_contact["campaign_tags"] if t in TAG_MAP})
        current_hcp_tags = hcp_customer.get("tags") or []
        missing = [t for t in hcp_tags_to_add if t not in current_hcp_tags]

        if not missing:
            stats["already_tagged"] += 1
            continue

        merged = sorted(set(current_hcp_tags + hcp_tags_to_add))
        action = {
            "ghl_name": ghl_contact["name"],
            "phone": ghl_contact["phone"],
            "hcp_id": hcp_customer["id"],
            "hcp_name": f"{hcp_customer.get('first_name', '')} {hcp_customer.get('last_name', '')}".strip(),
            "current_tags": current_hcp_tags,
            "tags_to_add": missing,
            "final_tags": merged,
        }

        if args.dry_run:
            stats["would_update"] += 1
            actions.append(action)
            print(f"  WOULD UPDATE {action['hcp_name']} ({action['phone']}): add {missing}", file=sys.stderr)
        else:
            status, body = hcp_patch_customer_tags(hcp_customer["id"], merged, hcp_key)
            if 200 <= status < 300:
                stats["updated"] += 1
                actions.append(action)
                print(f"  UPDATED {action['hcp_name']} ({action['phone']}): added {missing}", file=sys.stderr)
            else:
                stats["failed"] += 1
                action["error"] = f"{status}: {body[:150]}"
                actions.append(action)
                print(f"  FAILED {action['hcp_name']}: {status} {body[:150]}", file=sys.stderr)
            time.sleep(0.1)  # rate limit politeness

    print("\n=== Summary ===", file=sys.stderr)
    for k, v in stats.items():
        print(f"  {k}: {v}", file=sys.stderr)

    # Print results table
    if actions:
        print("\n=== Actions ===", file=sys.stderr)
        for a in actions:
            err = f" [ERROR: {a['error']}]" if "error" in a else ""
            print(f"  {a['hcp_name']:<30} {a['phone']:<15} +{','.join(a['tags_to_add'])}{err}")


if __name__ == "__main__":
    main()
