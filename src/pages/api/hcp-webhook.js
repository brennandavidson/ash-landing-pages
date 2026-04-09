import crypto from 'node:crypto';

export const prerender = false; // This route must be server-rendered

// Access env at runtime - avoid static analysis by bundler
function getEnv(key) {
  const e = globalThis.process?.env;
  return e ? e[key] : undefined;
}

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

async function sendToMeta(event, token) {
  const params = new URLSearchParams({
    data: JSON.stringify([event]),
    access_token: token,
  });

  const response = await fetch(
    `https://graph.facebook.com/v21.0/${OFFLINE_DATASET_ID}/events`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: params.toString(),
    }
  );

  return response.json();
}

export async function POST({ request }) {
  const META_ACCESS_TOKEN = getEnv('META_ADS_ACCESS_TOKEN');
  const WEBHOOK_SECRET = getEnv('HCP_WEBHOOK_SECRET');

  if (!META_ACCESS_TOKEN) {
    return new Response(
      JSON.stringify({ error: 'Server configuration error' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }

  if (WEBHOOK_SECRET) {
    const authHeader = request.headers.get('x-webhook-secret') || request.headers.get('authorization');
    if (authHeader !== WEBHOOK_SECRET && authHeader !== `Bearer ${WEBHOOK_SECRET}`) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }
  }

  try {
    const body = await request.json();
    const customer = body.customer || body;

    const phone = customer.phone || customer.mobile_number || customer.home_number || '';
    const email = customer.email || '';
    const firstName = customer.first_name || '';
    const lastName = customer.last_name || '';
    const city = customer.city || '';
    const state = customer.state || customer.region || '';
    const zip = customer.zip || customer.postal_code || customer.zip_code || '';

    if (!phone && !email) {
      return new Response(
        JSON.stringify({ status: 'skipped', reason: 'No phone or email — cannot match' }),
        { status: 200, headers: { 'Content-Type': 'application/json' } }
      );
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

    const result = await sendToMeta(event, META_ACCESS_TOKEN);

    return new Response(
      JSON.stringify({
        status: 'sent',
        events_received: result.events_received || 0,
        matched_fields: Object.keys(userData),
      }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );

  } catch (error) {
    console.error('Webhook processing error:', error);
    return new Response(
      JSON.stringify({ error: 'Processing failed' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}
