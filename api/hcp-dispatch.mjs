// HCP webhook dispatcher.
//
// HouseCall Pro allows only one webhook URL per account. This endpoint
// receives that one webhook and fans out to two completely separate
// downstream endpoints via HTTP:
//
//   1. /api/hcp-webhook    — Meta CAPI (offline attribution)
//   2. /api/hcp-tag-parity — GHL → HCP tag sync
//
// Both downstream endpoints are independent serverless functions.
// The dispatcher cannot modify their behavior in any way — it only
// forwards the raw HCP payload. If the tag parity path fails, it
// cannot affect the Meta CAPI path and vice versa.

// Always use the production domain — VERCEL_URL resolves to the
// preview deployment which requires auth on Pro accounts.
const SELF_BASE = 'https://go.ashcooling.com';

async function forward(path, body) {
  try {
    const res = await fetch(`${SELF_BASE}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let parsed;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = { raw: text.slice(0, 300) };
    }
    return { ok: res.ok, status: res.status, body: parsed };
  } catch (err) {
    return { ok: false, status: 0, error: err.message };
  }
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const body = req.body || {};

  // Fire both endpoints in parallel. They're independent serverless
  // functions — neither one can affect the other.
  const [metaResult, tagResult] = await Promise.all([
    forward('/api/hcp-webhook', body),
    forward('/api/hcp-tag-parity', body),
  ]);

  return res.status(200).json({
    status: 'dispatched',
    meta_capi: metaResult,
    tag_parity: tagResult,
  });
}
