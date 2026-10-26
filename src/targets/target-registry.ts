import { promises as dns } from 'node:dns';
import { isIP } from 'node:net';
import { RunJobParams } from '../jobs/run-job';

export interface JobTarget {
  name: string;
  url: string;
  kind: 'demo' | 'remote';
  oracle: boolean;
  allowPrivate: boolean;
}

const DEMO_TARGET_NAME = 'demo-receiver';

function normalize(value: string): string {
  return value.replace(/\/+$/, '');
}

/** Treat loopback spellings as equivalent and ignore trailing slashes. */
function canonicalTarget(value: string): string {
  const url = new URL(value);
  if (['localhost', '127.0.0.1', '::1', '[::1]'].includes(url.hostname)) {
    url.hostname = '127.0.0.1';
  }
  url.pathname = url.pathname.replace(/\/+$/, '');
  url.search = '';
  url.hash = '';
  return url.toString().replace(/\/+$/, '');
}

/**
 * Literal private/loopback/link-local host detection. Handles both bracketed
 * (`[::1]`, from `URL.hostname`) and bare (`::1`, from `dns.lookup`) IPv6
 * spellings, plus IPv4-mapped and IPv4-compatible IPv6 addresses so an
 * `::ffff:127.0.0.1` cannot smuggle a private IPv4 past the check.
 */
export function isPrivateHost(hostname: string): boolean {
  const host = hostname.toLowerCase().trim();
  if (host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local')) {
    return true;
  }
  const bare = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;
  const version = isIP(bare);
  if (version === 4) {
    return isPrivateIpv4(bare);
  }
  if (version === 6) {
    return isPrivateIpv6(bare);
  }
  return false;
}

function isPrivateIpv4(ip: string): boolean {
  const parts = ip.split('.');
  const a = Number(parts[0]);
  const b = Number(parts[1]);
  return (
    a === 0 ||
    a === 10 ||
    a === 127 ||
    (a === 192 && b === 168) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 169 && b === 254)
  );
}

function isPrivateIpv6(ip: string): boolean {
  const lower = ip.toLowerCase();
  if (lower === '::' || lower === '::1') {
    return true;
  }
  // IPv4-mapped (`::ffff:a.b.c.d`) and IPv4-compatible (`::a.b.c.d`).
  const mapped = /^(?:::ffff:|::)(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(lower);
  if (mapped) {
    return isPrivateIpv4(`${mapped[1]}.${mapped[2]}.${mapped[3]}.${mapped[4]}`);
  }
  // Unique local fc00::/7 and link-local fe80::/10.
  return /^f[cd]/.test(lower) || /^fe[89ab]/.test(lower);
}

/**
 * Fail closed: a DNS failure counts as "not allowed". Re-checks the literal
 * address family at resolution time. Note: the subsequent `fetch` re-resolves
 * the hostname, so a hostile DNS server could rebind between this check and the
 * connect (TOCTOU). This residual risk is mitigated by the operator-controlled
 * allowlist and `redirect: 'error'` on delivery, not by connection-time IP
 * pinning.
 */
async function resolvesToPrivate(hostname: string): Promise<boolean> {
  try {
    const addresses = await dns.lookup(hostname, { all: true });
    return addresses.some((address) => isPrivateHost(address.address));
  } catch {
    return true;
  }
}

function configuredTargets(): { name: string; url: string; allowPrivate: boolean; oracle: boolean }[] {
  const raw = process.env.TARGETS_JSON;
  if (!raw) {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) {
    return [];
  }
  const targets: { name: string; url: string; allowPrivate: boolean; oracle: boolean }[] = [];
  for (const entry of parsed.slice(0, 20)) {
    if (typeof entry !== 'object' || entry === null) {
      continue;
    }
    const candidate = entry as Record<string, unknown>;
    const name = typeof candidate.name === 'string' ? candidate.name : '';
    const urlValue = typeof candidate.url === 'string' ? candidate.url : '';
    if (!/^[a-z0-9][a-z0-9-]{0,40}$/i.test(name) || name === DEMO_TARGET_NAME) {
      continue;
    }
    try {
      const url = new URL(urlValue);
      if (url.protocol !== 'http:' && url.protocol !== 'https:') {
        continue;
      }
      const allowPrivate = candidate.allowPrivate === true;
      if (!allowPrivate && isPrivateHost(url.hostname)) {
        continue;
      }
      targets.push({ name, url: normalize(urlValue), allowPrivate, oracle: candidate.oracle === true });
    } catch {
      continue;
    }
  }
  return targets;
}

/**
 * Resolve the job's target against the runner's own allowlist. The job carries
 * a name and URL for observability, but the runner trusts only its local
 * configuration; mismatches and private hosts fail closed.
 */
export async function resolveJobTarget(params: RunJobParams): Promise<JobTarget> {
  const demoUrl = normalize(process.env.DEMO_RECEIVER_URL ?? 'http://127.0.0.1:3100');

  if (params.targetName === DEMO_TARGET_NAME) {
    if (canonicalTarget(params.targetUrl) !== canonicalTarget(demoUrl)) {
      throw new Error('Job target URL does not match the configured demo receiver');
    }
    return {
      name: DEMO_TARGET_NAME,
      url: demoUrl,
      kind: 'demo',
      oracle: true,
      allowPrivate: true,
    };
  }

  const entry = configuredTargets().find((target) => target.name === params.targetName);
  if (!entry) {
    throw new Error(`Unknown or disabled target "${params.targetName}"`);
  }
  if (canonicalTarget(entry.url) !== canonicalTarget(params.targetUrl)) {
    throw new Error('Job target URL does not match the allowlisted target');
  }
  const hostname = new URL(entry.url).hostname;
  if (!entry.allowPrivate && (isPrivateHost(hostname) || (await resolvesToPrivate(hostname)))) {
    throw new Error(`Target "${entry.name}" resolves to a private address and is not allowed`);
  }
  return {
    name: entry.name,
    url: entry.url,
    kind: 'remote',
    oracle: entry.oracle,
    allowPrivate: entry.allowPrivate,
  };
}

