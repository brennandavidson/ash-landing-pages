// HCP → GHL tag parity webhook (COMPLETELY SEPARATE from Meta CAPI webhook)
// Fires on HCP customer.created. Looks up the phone in GHL. If the GHL
// contact has campaign tags, applies matching HCP tags to the HCP customer.
//
// DOES NOT TOUCH META IN ANY WAY. This is purely HCP <-> GHL.

const HCP_BASE = 'https://api.housecallpro.com';
const GHL_BASE = 'https://services.leadconnectorhq.com';
const GHL_API_VERSION = '2021-07-28';

// Map GHL tag name → HCP tag name
const TAG_MAP = {
  'beat-a-quote': 'beat-a-quote-lp',
  'blowout-sale': 'blowout-pack-unit-lp',
  'summer-sale-2026': 'summer-sale-2026',
  'valentines-sale': 'valentines-sale',
};

function normalizePhone(phone) {
  if (!phone) return null;
  const digits = String(phone).replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return digits;
}

async function searchGhlByPhone(phone, token, locationId) {
  const res = await fetch(`${GHL_BASE}/contacts/search`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      Version: GHL_API_VERSION,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ locationId, query: phone, pageLimit: 5 }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GHL search failed: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

function findMatchingContact(contacts, normalizedPhone) {
  if (!contacts || contacts.length === 0) return null;
  for (const c of contacts) {
    const ghlPhone = normalizePhone(c.phone);
    if (ghlPhone === normalizedPhone) return c;
  }
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

async function getHcpCustomer(customerId, hcpKey) {
  const res = await fetch(`${HCP_BASE}/customers/${customerId}`, {
    headers: { Authorization: `Token ${hcpKey}` },
  });
  if (!res.ok) throw new Error(`HCP GET failed: ${res.status}`);
  return res.json();
}

async function updateHcpCustomerTags(customerId, newTags, hcpKey) {
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

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const HCP_KEY = process.env.HCP_API_KEY;
  const GHL_KEY = process.env.GHL_API_KEY;
  const GHL_LOCATION = process.env.GHL_LOCATION_ID;

  if (!HCP_KEY || !GHL_KEY || !GHL_LOCATION) {
    return res.status(500).json({
      error: 'Server configuration error',
      missing: {
        HCP_API_KEY: !HCP_KEY,
        GHL_API_KEY: !GHL_KEY,
        GHL_LOCATION_ID: !GHL_LOCATION,
      },
    });
  }

  try {
    const body = req.body || {};
    const customer = body.customer || body;
    const customerId = customer.id;
    const phone =
      customer.mobile_number || customer.home_number || customer.phone || '';

    if (!customerId) {
      return res.status(200).json({ status: 'skipped', reason: 'No customer ID' });
    }

    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) {
      return res.status(200).json({ status: 'skipped', reason: 'No phone number', customer_id: customerId });
    }

    // Search GHL
    const ghlResult = await searchGhlByPhone(normalizedPhone, GHL_KEY, GHL_LOCATION);
    const ghlContact = findMatchingContact(ghlResult.contacts, normalizedPhone);

    if (!ghlContact) {
      return res.status(200).json({
        status: 'no_ghl_match',
        customer_id: customerId,
        phone: normalizedPhone,
      });
    }

    const campaignTagsToAdd = mapGhlTagsToHcp(ghlContact.tags);

    if (campaignTagsToAdd.length === 0) {
      return res.status(200).json({
        status: 'no_campaign_tags',
        ghl_contact_id: ghlContact.id,
        ghl_tags: ghlContact.tags || [],
      });
    }

    // Pull current HCP tags, merge with new ones, PATCH only if changed
    const currentCustomer = await getHcpCustomer(customerId, HCP_KEY);
    const currentTags = currentCustomer.tags || [];
    const addedTags = campaignTagsToAdd.filter(t => !currentTags.includes(t));

    if (addedTags.length === 0) {
      return res.status(200).json({
        status: 'already_tagged',
        customer_id: customerId,
        existing_tags: currentTags,
      });
    }

    const merged = [...new Set([...currentTags, ...campaignTagsToAdd])];
    await updateHcpCustomerTags(customerId, merged, HCP_KEY);

    return res.status(200).json({
      status: 'updated',
      customer_id: customerId,
      ghl_contact_id: ghlContact.id,
      added_tags: addedTags,
      final_tags: merged,
    });
  } catch (err) {
    console.error('hcp-tag-parity error:', err);
    return res.status(500).json({ error: 'Processing failed', message: err.message });
  }
}
