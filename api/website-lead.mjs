import crypto from 'node:crypto';

const GITHUB_OWNER = 'brennandavidson';
const GITHUB_REPO = 'ash-landing-pages';
const LOG_FILE_PATH = 'logs/website-leads.jsonl';

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
 * Append a website-lead marker to logs/website-leads.jsonl in the GitHub repo.
 * Used by hcp-webhook.mjs to skip duplicate CAPI fires for HCP customers
 * that were already captured by the website Pixel.
 */
async function appendToLeadLog(entry, githubToken) {
  if (!githubToken) {
    console.warn('GITHUB_AUDIT_TOKEN not set — skipping website-lead log');
    return { skipped: true };
  }

  const headers = {
    'Authorization': `Bearer ${githubToken}`,
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'User-Agent': 'ash-lp-website-lead',
  };

  const apiUrl = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${LOG_FILE_PATH}`;

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
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
        message: `log: website lead`,
        content: encodedContent,
        committer: {
          name: 'ASH Website',
          email: 'webhook@ashcooling.com',
        },
      };
      if (sha) putBody.sha = sha;

      const putRes = await fetch(apiUrl, {
        method: 'PUT',
        headers: { ...headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(putBody),
      });

      if (putRes.ok) return { success: true };
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
  // Permissive CORS — same domain in prod but harmless to allow
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const GITHUB_TOKEN = process.env.GITHUB_AUDIT_TOKEN;

  try {
    const body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    const phone = body?.phone || '';
    const sourceUrl = body?.source_url || '';

    if (!phone) {
      return res.status(200).json({ status: 'skipped', reason: 'No phone' });
    }

    const normalized = normalizePhone(phone);
    if (!normalized) {
      return res.status(200).json({ status: 'skipped', reason: 'Invalid phone' });
    }

    const phoneHash = sha256(normalized);

    const entry = {
      timestamp: new Date().toISOString(),
      phone_hash: phoneHash,
      source_url: sourceUrl,
    };

    const logResult = await appendToLeadLog(entry, GITHUB_TOKEN);

    return res.status(200).json({
      status: 'logged',
      phone_hash: phoneHash,
      log: logResult,
    });
  } catch (error) {
    console.error('Website lead error:', error);
    return res.status(500).json({ error: 'Processing failed', message: error.message });
  }
}
