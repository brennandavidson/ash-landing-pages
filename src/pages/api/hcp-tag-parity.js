// HCP → GHL tag parity webhook
// Fires when HouseCall Pro creates a customer.
// Looks up the customer's phone number in GHL, and if GHL has any campaign
// tags for that contact, adds the matching HCP tags to the HCP customer.
//
// This closes the attribution gap for phone-call leads that hit GHL tracking
// numbers but never come through the LP form flow.

export const prerender = false;

const HCP_BASE = 'https://api.housecallpro.com';
const GHL_BASE = 'https://services.leadconnectorhq.com';
const GHL_API_VERSION = '2021-07-28';

// Map GHL tag name → HCP tag name. When a GHL contact has the key, apply
// the value to HCP. Extend this as new campaigns launch.
const TAG_MAP = {
  'beat-a-quote': 'beat-a-quote-lp',
  'blowout-sale': 'blowout-pack-unit-lp',
  'summer-sale-2026': 'summer-sale-2026',
  'valentines-sale': 'valentines-sale',
};

function getEnv(key) {
  return globalThis.process?.env?.[key];
}

function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return digits;
}

async function searchGhlByPhone(phone, token, locationId) {
  const body = {
    locationId,
    query: phone,
    pageLimit: 5,
  };
  const res = await fetch(`${GHL_BASE}/contacts/search`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      Version: GHL_API_VERSION,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GHL search failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

function findMatchingContact(contacts, normalizedPhone) {
  if (!contacts || contacts.length === 0) return null;
  // Match on normalized phone (GHL stores +1XXXXXXXXXX, we normalize to XXXXXXXXXX)
  for (const c of contacts) {
    const ghlPhone = normalizePhone(c.phone);
    if (ghlPhone === normalizedPhone) return c;
  }
  // Fallback: first result if phone normalization fails
  return contacts[0] || null;
}

function mapGhlTagsToHcp(ghlTags) {
  const hcpTags = new Set();
  for (const tag of ghlTags || []) {
    const mapped = TAG_MAP[tag];
    if (mapped) hcpTags.add(mapped);
  }
  return [...hcpTags];
}

async function updateHcpCustomerTags(customerId, newTags, hcpKey) {
  // HCP uses PATCH for partial updates
  const res = await fetch(`${HCP_BASE}/customers/${customerId}`, {
    method: 'PATCH',
    headers: {
      Authorization: `Token ${hcpKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ tags: newTags }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HCP PATCH failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

async function getHcpCustomer(customerId, hcpKey) {
  const res = await fetch(`${HCP_BASE}/customers/${customerId}`, {
    headers: { Authorization: `Token ${hcpKey}` },
  });
  if (!res.ok) {
    throw new Error(`HCP GET failed: ${res.status}`);
  }
  return res.json();
}

export async function POST({ request }) {
  const HCP_KEY = getEnv('HCP_API_KEY');
  const GHL_KEY = getEnv('GHL_API_KEY');
  const GHL_LOCATION = getEnv('GHL_LOCATION_ID');

  if (!HCP_KEY || !GHL_KEY || !GHL_LOCATION) {
    return new Response(
      JSON.stringify({ error: 'Server configuration error' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }

  try {
    const body = await request.json();
    // HCP customer.created webhook payload shape — customer data at top level
    // or under a "customer" key depending on the event type
    const customer = body.customer || body;
    const customerId = customer.id;
    const phone = customer.mobile_number || customer.home_number || customer.phone || '';

    if (!customerId) {
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'No customer ID in payload' }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) {
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'No phone number' }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Search GHL for the contact
    const ghlResult = await searchGhlByPhone(normalizedPhone, GHL_KEY, GHL_LOCATION);
    const ghlContact = findMatchingContact(ghlResult.contacts, normalizedPhone);

    if (!ghlContact) {
      return new Response(
        JSON.stringify({
          status: 'no_match',
          reason: 'No GHL contact found for phone',
          customer_id: customerId,
          phone: normalizedPhone,
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    const campaignTagsToAdd = mapGhlTagsToHcp(ghlContact.tags);

    if (campaignTagsToAdd.length === 0) {
      return new Response(
        JSON.stringify({
          status: 'no_campaign_tags',
          reason: 'GHL contact found but has no recognized campaign tags',
          ghl_contact_id: ghlContact.id,
          ghl_tags: ghlContact.tags || [],
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Pull current HCP customer tags and merge
    const currentCustomer = await getHcpCustomer(customerId, HCP_KEY);
    const currentTags = currentCustomer.tags || [];
    const merged = [...new Set([...currentTags, ...campaignTagsToAdd])];

    // Skip the PATCH if nothing would change (prevents infinite loop with customer.updated webhooks)
    const addedTags = campaignTagsToAdd.filter(t => !currentTags.includes(t));
    if (addedTags.length === 0) {
      return new Response(
        JSON.stringify({
          status: 'already_tagged',
          customer_id: customerId,
          existing_tags: currentTags,
        }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
    }

    await updateHcpCustomerTags(customerId, merged, HCP_KEY);

    return new Response(
      JSON.stringify({
        status: 'updated',
        customer_id: customerId,
        ghl_contact_id: ghlContact.id,
        added_tags: addedTags,
        final_tags: merged,
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  } catch (err) {
    console.error('hcp-tag-parity error:', err);
    return new Response(
      JSON.stringify({ error: 'Processing failed', message: err.message }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}
