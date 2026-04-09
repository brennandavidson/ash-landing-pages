import crypto from 'node:crypto';

const OFFLINE_DATASET_ID = '1438722024117263';

function sha256(value) {
  if (!value) return null;
  return crypto.createHash('sha256').update(String(value).toLowerCase().trim()).digest('hex');
}

function normalizePhone(phone) {
  if (!phone) return null;
  const digits = phone.replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('1')) return digits.slice(1);
  return digits;
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const META_ACCESS_TOKEN = process.env.META_ADS_ACCESS_TOKEN;

  if (!META_ACCESS_TOKEN) {
    return res.status(500).json({
      error: 'Server configuration error',
      debug_keys: Object.keys(process.env).filter(k => k.startsWith('META')),
    });
  }

  try {
    const body = req.body;
    const customer = body.customer || body;

    const phone = customer.phone || customer.mobile_number || customer.home_number || '';
    const email = customer.email || '';
    const firstName = customer.first_name || '';
    const lastName = customer.last_name || '';
    const city = customer.city || '';
    const state = customer.state || customer.region || '';
    const zip = customer.zip || customer.postal_code || customer.zip_code || '';

    if (!phone && !email) {
      return res.status(200).json({ status: 'skipped', reason: 'No phone or email' });
    }

    const userData = {};
    if (phone) {
      const normalized = normalizePhone(phone);
      if (normalized) userData.ph = [sha256(normalized)];
    }
    if (email) userData.em = [sha256(email)];
    if (firstName) userData.fn = [sha256(firstName)];
    if (lastName) userData.ln = [sha256(lastName)];
    if (city) userData.ct = [sha256(city)];
    if (state) userData.st = [sha256(state)];
    if (zip) userData.zp = [sha256(zip)];
    userData.country = [sha256('us')];

    const event = {
      event_name: 'Contact',
      event_time: Math.floor(Date.now() / 1000),
      action_source: 'physical_store',
      user_data: userData,
    };

    const params = new URLSearchParams({
      data: JSON.stringify([event]),
      access_token: META_ACCESS_TOKEN,
    });

    const metaRes = await fetch(
      `https://graph.facebook.com/v21.0/${OFFLINE_DATASET_ID}/events`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: params.toString(),
      }
    );

    const result = await metaRes.json();

    return res.status(200).json({
      status: 'sent',
      events_received: result.events_received || 0,
      matched_fields: Object.keys(userData),
    });

  } catch (error) {
    console.error('Webhook error:', error);
    return res.status(500).json({ error: 'Processing failed' });
  }
}
