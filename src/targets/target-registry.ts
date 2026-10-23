import { promises as dns } from 'node:dns';
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

/** Literal private/loopback/link-local host detection. */
export function isPrivateHost(hostname: string): boolean {
  const host = hostname.toLowerCase();
  if (host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local')) {
    return true;
  }
  if (host === '0.0.0.0') {
    return true;
  }
  if (host.startsWith('[') && host.endsWith(']')) {
    const inner = host.slice(1, -1);
    return (
      inner === '::1' ||
      /^f[cd][0-9a-f]{2}:/.test(inner) ||
      /^fe[89ab][0-9a-f]:/.test(inner)
    );
  }
  const ipv4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(host);
  if (ipv4) {
    const a = Number(ipv4[1]);
    const b = Number(ipv4[2]);
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 192 && b === 168) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 169 && b === 254)
    );
  }
  return false;
}

/** Fail closed: a DNS failure counts as "not allowed". */
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

