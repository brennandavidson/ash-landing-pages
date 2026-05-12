import crypto from 'node:crypto';

const OFFLINE_DATASET_ID = '1438722024117263';
const GITHUB_OWNER = 'brennandavidson';
const GITHUB_REPO = 'ash-landing-pages';
const LOG_FILE_PATH = 'logs/offline-leads.jsonl';
const WEBSITE_LEADS_PATH = 'logs/website-leads.jsonl';
// Window for matching an HCP customer against a recent website lead.
// GHL → HCP creation usually takes 30-120 seconds. 15 min is conservative
// to absorb Zap lag, retries, or workflow delays without false matches.
const WEBSITE_LEAD_MATCH_WINDOW_MIN = 15;

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

/**
 * Fetch the last N entries from logs/website-leads.jsonl to check
 * whether this HCP customer was just submitted via the website form.
 * Returns an array of {timestamp, phone_hash, source_url} entries.
 */
async function fetchRecentWebsiteLeads(githubToken) {
  if (!githubToken) return [];

  try {
    const apiUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${WEBSITE_LEADS_PATH}`;
    const res = await fetch(apiUrl, {
      headers: {
        'Authorization': `Bearer ${githubToken}`,
        'Accept': 'application/vnd.github+json',
        'User-Agent': 'ash-lp-webhook',
      },
    });
    if (!res.ok) return [];

    const fileData = await res.json();
    const content = Buffer.from(fileData.content, 'base64').toString('utf-8');
    const lines = content.trim().split('\n').filter(Boolean);

    // Last 100 entries is plenty for a 15-minute window check
    return lines.slice(-100).map((line) => {
      try { return JSON.parse(line); } catch { return null; }
    }).filter(Boolean);
  } catch (e) {
    console.warn('Could not fetch website-leads log:', e.message);
    return [];
  }
}

function findWebsiteLeadMatch(phoneHash, websiteLeads, windowMinutes) {
  if (!phoneHash) return null;
  const cutoff = Date.now() - windowMinutes * 60 * 1000;
  for (const lead of websiteLeads) {
    const t = new Date(lead.timestamp).getTime();
    if (Number.isNaN(t) || t < cutoff) continue;
    if (lead.phone_hash === phoneHash) return lead;
  }
  return null;
}

/**
 * Append a log entry to logs/offline-leads.jsonl in the GitHub repo.
 * Uses GitHub Contents API with SHA-based concurrency.
 */
async function appendToAuditLog(entry, githubToken) {
  if (!githubToken) {
    console.warn('GITHUB_TOKEN not set — skipping audit log');
    return { skipped: true };
  }

  const headers = {
    'Authorization': `Bearer ${githubToken}`,
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'User-Agent': 'ash-lp-webhook',
  };

  const apiUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${LOG_FILE_PATH}`;

  // Retry on SHA conflicts (race condition if multiple webhooks fire simultaneously)
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      // Get current file contents + SHA
      const getRes = await fetch(apiUrl, { headers });
      if (!getRes.ok && getRes.status !== 404) {
        return { error: `GitHub GET failed: ${getRes.status}` };
      }

      let existingContent = '';
      let sha = null;
      if (getRes.ok) {
        const fileData = await getRes.json();
        existingContent = Buffer.from(fileData.content, 'base64').toString('utf-8');
        sha = fileData.sha;
      }

      const newLine = JSON.stringify(entry) + '\n';
      const newContent = existingContent + newLine;
      const encodedContent = Buffer.from(newContent, 'utf-8').toString('base64');

      const putBody = {
        message: `log: offline lead ${entry.first_name || ''} ${entry.last_name || ''}`.trim(),
        content: encodedContent,
        committer: {
          name: 'ASH Webhook',
          email: 'webhook@ashcooling.com',
        },
      };
      if (sha) putBody.sha = sha;

      const putRes = await fetch(apiUrl, {
        method: 'PUT',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(putBody),
      });

      if (putRes.ok) {
        return { success: true };
      }

      // 409 = SHA conflict, retry
      if (putRes.status === 409 || putRes.status === 422) {
        await new Promise(r => setTimeout(r, 500 * (attempt + 1)));
        continue;
      }

      const errBody = await putRes.text();
      return { error: `GitHub PUT failed: ${putRes.status} - ${errBody.slice(0, 200)}` };
    } catch (e) {
      return { error: `GitHub exception: ${e.message}` };
    }
  }

  return { error: 'Max retries exceeded' };
}

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const META_ACCESS_TOKEN = process.env.META_ADS_ACCESS_TOKEN;
  const GITHUB_TOKEN = process.env.GITHUB_AUDIT_TOKEN;

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
    const hcpId = customer.id || body.id || '';

    if (!phone && !email) {
      return res.status(200).json({ status: 'skipped', reason: 'No phone or email' });
    }

    // ── Dedup against website leads ──
    // If this HCP customer matches a recent website-form submission by phone,
    // skip the CAPI fire — the website Pixel already captured the Lead.
    // Audit log entry is still written with skip reason for traceability.
    const phoneNormalized = phone ? normalizePhone(phone) : null;
    const phoneHash = phoneNormalized ? sha256(phoneNormalized) : null;
    const websiteLeads = await fetchRecentWebsiteLeads(GITHUB_TOKEN);
    const websiteMatch = findWebsiteLeadMatch(phoneHash, websiteLeads, WEBSITE_LEAD_MATCH_WINDOW_MIN);

    if (websiteMatch) {
      const skipEntry = {
        timestamp: new Date().toISOString(),
        skipped: true,
        skip_reason: 'website-lead-match',
        matched_website_lead_at: websiteMatch.timestamp,
        matched_source_url: websiteMatch.source_url || null,
        hcp_id: hcpId || null,
        first_name: firstName,
        last_name: lastName,
        phone: phone,
        email: email,
      };
      const auditResult = await appendToAuditLog(skipEntry, GITHUB_TOKEN);
      return res.status(200).json({
        status: 'skipped',
        reason: 'website-lead-match',
        matched_at: websiteMatch.timestamp,
        audit_log: auditResult,
      });
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

    // Stable event_id so Meta dedupes repeat HCP webhooks for the same customer
    // within the 7-day window (HCP commonly fires multiple updates per record).
    // Prefer HCP customer id; fall back to a hash of phone+email if missing.
    const eventName = 'Contact';
    const idSource = hcpId || sha256(`${normalizePhone(phone) || ''}|${(email || '').toLowerCase().trim()}`)?.slice(0, 24) || `noid-${Date.now()}`;
    const eventId = `hcp-${eventName.toLowerCase()}-${idSource}`;

    const event = {
      event_id: eventId,
      event_name: eventName,
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

    const metaResult = await metaRes.json();

    // Build audit log entry (unhashed for internal reference only)
    const logEntry = {
      timestamp: new Date().toISOString(),
      event_id: eventId,
      event_name: eventName,
      hcp_id: hcpId || null,
      first_name: firstName,
      last_name: lastName,
      phone: phone,
      email: email,
      city: city,
      state: state,
      zip: zip,
      matched_fields: Object.keys(userData),
      meta_events_received: metaResult.events_received || 0,
      meta_fbtrace_id: metaResult.fbtrace_id || null,
      meta_error: metaResult.error || null,
    };

    // Fire-and-forget the audit log (don't block the webhook response)
    const auditResult = await appendToAuditLog(logEntry, GITHUB_TOKEN);

    return res.status(200).json({
      status: 'sent',
      event_id: eventId,
      events_received: metaResult.events_received || 0,
      matched_fields: Object.keys(userData),
      audit_log: auditResult,
    });

  } catch (error) {
    console.error('Webhook error:', error);
    return res.status(500).json({ error: 'Processing failed', message: error.message });
  }
}
