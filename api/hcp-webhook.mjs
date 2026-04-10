import crypto from 'node:crypto';

const OFFLINE_DATASET_ID = '1438722024117263';
const GITHUB_OWNER = 'brennandavidson';
const GITHUB_REPO = 'ash-landing-pages';
const LOG_FILE_PATH = 'logs/offline-leads.jsonl';

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

    const metaResult = await metaRes.json();

    // Build audit log entry (unhashed for internal reference only)
    const logEntry = {
      timestamp: new Date().toISOString(),
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
      events_received: metaResult.events_received || 0,
      matched_fields: Object.keys(userData),
      audit_log: auditResult,
    });

  } catch (error) {
    console.error('Webhook error:', error);
    return res.status(500).json({ error: 'Processing failed', message: error.message });
  }
}
