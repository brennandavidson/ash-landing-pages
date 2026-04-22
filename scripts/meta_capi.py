"""
Send conversion events to Meta Conversions API (CAPI).

Hashes PII (phone, email, name) with SHA256 before sending.
Supports test mode via --test-code flag for validation in Events Manager.

Usage:
    # Test mode (shows up in Events Manager > Test Events)
    python meta_capi.py --phone 6233433189 --test-code TEST12345

    # Live mode
    python meta_capi.py --phone 6233433189 --email john@example.com --first-name John --last-name Doe --source-url "https://go.ashcooling.com/summer-sale"

    # From GHL webhook payload (JSON stdin)
    echo '{"phone":"+16233433189","full_name":"John Doe"}' | python meta_capi.py --stdin
"""

import hashlib
import json
import re
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path


PIXEL_ID = "1787874271848321"  # Website events (Lead from forms)
OFFLINE_DATASET_ID = "1438722024117263"  # Offline events (HouseCall Pro sales)
API_VERSION = "v21.0"


def load_token():
    env_path = Path.home() / ".claude" / "config" / "api-keys.env"
    for line in env_path.read_text().splitlines():
        if line.startswith("META_ADS_ACCESS_TOKEN="):
            return line.split("=", 1)[1].strip()
    print("Error: META_ADS_ACCESS_TOKEN not found", file=sys.stderr)
    sys.exit(1)


def sha256_hash(value):
    """Hash a value with SHA256 per Meta's requirements."""
    if not value:
        return None
    # Lowercase, strip whitespace
    cleaned = str(value).lower().strip()
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()


def normalize_phone(phone):
    """Strip to digits only, ensure no country code prefix issues."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    # Meta expects digits only, no country code prefix for US
    # But if it starts with 1 and is 11 digits, strip the 1
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits


def build_event(phone=None, email=None, first_name=None, last_name=None,
                city=None, state=None, zip_code=None, country="us",
                source_url=None, event_name="Lead", offline=False,
                value=None, currency="usd"):
    """Build a CAPI event payload."""
    user_data = {}

    if phone:
        normalized = normalize_phone(phone)
        if normalized:
            user_data["ph"] = [sha256_hash(normalized)]

    if email:
        user_data["em"] = [sha256_hash(email)]

    if first_name:
        user_data["fn"] = [sha256_hash(first_name)]

    if last_name:
        user_data["ln"] = [sha256_hash(last_name)]

    if city:
        user_data["ct"] = [sha256_hash(city)]

    if state:
        user_data["st"] = [sha256_hash(state)]

    if zip_code:
        user_data["zp"] = [sha256_hash(zip_code)]

    if country:
        user_data["country"] = [sha256_hash(country)]

    if not user_data:
        print("Error: At least phone or email required for matching", file=sys.stderr)
        sys.exit(1)

    event = {
        "event_name": event_name,
        "event_time": int(time.time()),
        "action_source": "physical_store" if offline else "website",
        "user_data": user_data,
    }

    if source_url and not offline:
        event["event_source_url"] = source_url

    if value is not None:
        event["custom_data"] = {
            "value": float(value),
            "currency": currency,
        }

    return event


def send_event(event, token, test_event_code=None, offline=False):
    """Send event to Meta CAPI."""
    dataset_id = OFFLINE_DATASET_ID if offline else PIXEL_ID
    url = f"https://graph.facebook.com/{API_VERSION}/{dataset_id}/events"

    payload = {
        "data": json.dumps([event]),
        "access_token": token,
    }

    if test_event_code:
        payload["test_event_code"] = test_event_code

    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"CAPI Error {e.code}: {error_body}", file=sys.stderr)
        return None


def parse_ghl_payload(payload):
    """Extract lead data from a GHL webhook payload."""
    phone = payload.get("phone", "")
    email = payload.get("email", "")
    full_name = payload.get("full_name", "")
    first_name = payload.get("first_name", "") or payload.get("firstName", "")
    last_name = payload.get("last_name", "") or payload.get("lastName", "")

    # Split full_name if first/last not provided
    if full_name and not first_name:
        parts = full_name.strip().split(" ", 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

    address = payload.get("address", "")
    city = payload.get("city", "")
    state = payload.get("state", "")
    zip_code = payload.get("zip", "") or payload.get("postalCode", "")

    return {
        "phone": phone,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Send events to Meta Conversions API")
    parser.add_argument("--phone", type=str, help="Phone number")
    parser.add_argument("--email", type=str, help="Email address")
    parser.add_argument("--first-name", type=str, help="First name")
    parser.add_argument("--last-name", type=str, help="Last name")
    parser.add_argument("--city", type=str, help="City")
    parser.add_argument("--state", type=str, default="az", help="State (default: az)")
    parser.add_argument("--zip", type=str, help="Zip code")
    parser.add_argument("--source-url", type=str, default="https://go.ashcooling.com/summer-sale", help="Event source URL")
    parser.add_argument("--event-name", type=str, default="Lead", help="Event name (default: Lead)")
    parser.add_argument("--test-code", type=str, help="Test event code from Events Manager (test mode)")
    parser.add_argument("--offline", action="store_true", default=True, help="Send to offline dataset (default: true)")
    parser.add_argument("--website", action="store_true", help="Send to website pixel instead of offline dataset")
    parser.add_argument("--value", type=float, help="Conversion value in dollars")
    parser.add_argument("--stdin", action="store_true", help="Read GHL webhook JSON from stdin")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print event details")
    args = parser.parse_args()

    token = load_token()

    is_offline = not args.website

    if args.stdin:
        payload = json.loads(sys.stdin.read())
        data = parse_ghl_payload(payload)
        event = build_event(
            phone=data["phone"],
            email=data["email"],
            first_name=data["first_name"],
            last_name=data["last_name"],
            city=data["city"],
            state=data["state"],
            zip_code=data["zip_code"],
            source_url=args.source_url,
            event_name=args.event_name,
            offline=is_offline,
            value=args.value,
        )
    else:
        event = build_event(
            phone=args.phone,
            email=args.email,
            first_name=args.first_name,
            last_name=args.last_name,
            city=args.city,
            state=args.state,
            zip_code=args.zip,
            source_url=args.source_url,
            event_name=args.event_name,
            offline=is_offline,
            value=args.value,
        )

    if args.verbose:
        print("Event payload:", file=sys.stderr)
        print(json.dumps(event, indent=2), file=sys.stderr)
        if args.test_code:
            print(f"Test event code: {args.test_code}", file=sys.stderr)

    result = send_event(event, token, args.test_code, offline=is_offline)

    if result:
        events_received = result.get("events_received", 0)
        messages = result.get("messages", [])
        print(f"Success: {events_received} event(s) received by Meta", file=sys.stderr)
        if messages:
            for msg in messages:
                print(f"  Message: {msg}", file=sys.stderr)
        print(json.dumps(result))
    else:
        print("Failed to send event", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
