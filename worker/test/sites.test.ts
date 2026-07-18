// ABOUTME: Tests public web hosting: MIME inference, site/index/SPA serving, shares, and routing.
// ABOUTME: Runs AgentFS against in-memory SQLite so serving exercises real filesystem reads.

import Database from 'better-sqlite3';
import { beforeEach, describe, expect, it } from 'vitest';
import { AgentFS } from 'agentfs-sdk/cloudflare';
import { VolumeAccessCoordinator, HttpError } from '../src/files-api';
import { initSchema } from '../src/schema';
import { createTestStorage } from './support/storage';
import {
  contentTypeFor,
  createShare,
  deleteSite,
  parsePublicVolume,
  readSite,
  serveShare,
  serveSite,
  subdomainVolume,
  writeSite,
} from '../src/sites';

describe('contentTypeFor', () => {
  it('infers common web types and defaults to octet-stream', () => {
    expect(contentTypeFor('/index.html')).toBe('text/html; charset=utf-8');
    expect(contentTypeFor('/app.js')).toBe('text/javascript; charset=utf-8');
    expect(contentTypeFor('/style.css')).toBe('text/css; charset=utf-8');
    expect(contentTypeFor('/logo.svg')).toBe('image/svg+xml');
    expect(contentTypeFor('/data.bin')).toBe('application/octet-stream');
    expect(contentTypeFor('/noext')).toBe('application/octet-stream');
    expect(contentTypeFor('/dir.with.dots/file')).toBe('application/octet-stream');
  });
});

describe('public serving', () => {
  let fs: AgentFS;
  let sql: ReturnType<typeof createTestStorage>['sql'];
  const access = new VolumeAccessCoordinator();

  beforeEach(async () => {
    const storage = createTestStorage(new Database(':memory:'));
    sql = storage.sql;
    initSchema(sql);
    fs = AgentFS.create(storage);
    await fs.mkdir('/public');
    await fs.writeFile('/public/index.html', '<!doctype html>root');
    await fs.mkdir('/public/assets');
    await fs.writeFile('/public/assets/app.js', 'console.log(1)');
  });

  function get(path: string): Request {
    return new Request(`http://vol.example${path}`);
  }

  it('serves the index document for the site root with an inferred type', async () => {
    writeSite(sql, { pathPrefix: '/public', indexDocument: 'index.html', spa: false, cacheControl: 'max-age=60' });
    const response = await serveSite(fs, access, readSite(sql)!, '/', get('/s/vol/'));
    expect(response.status).toBe(200);
    expect(response.headers.get('Content-Type')).toBe('text/html; charset=utf-8');
    expect(response.headers.get('Cache-Control')).toBe('max-age=60');
    expect(await response.text()).toBe('<!doctype html>root');
  });

  it('serves nested assets with their own content type', async () => {
    writeSite(sql, { pathPrefix: '/public', indexDocument: 'index.html', spa: false, cacheControl: null });
    const response = await serveSite(fs, access, readSite(sql)!, '/assets/app.js', get('/s/vol/assets/app.js'));
    expect(response.status).toBe(200);
    expect(response.headers.get('Content-Type')).toBe('text/javascript; charset=utf-8');
    expect(await response.text()).toBe('console.log(1)');
  });

  it('falls back to the index document for unmatched SPA routes', async () => {
    writeSite(sql, { pathPrefix: '/public', indexDocument: 'index.html', spa: true, cacheControl: null });
    const response = await serveSite(fs, access, readSite(sql)!, '/deep/route', get('/s/vol/deep/route'));
    expect(response.status).toBe(200);
    expect(await response.text()).toBe('<!doctype html>root');
  });

  it('returns 404 for unmatched non-SPA routes', async () => {
    writeSite(sql, { pathPrefix: '/public', indexDocument: 'index.html', spa: false, cacheControl: null });
    await expect(serveSite(fs, access, readSite(sql)!, '/missing', get('/s/vol/missing')))
      .rejects.toMatchObject({ status: 404 });
  });

  it('publishes and unpublishes the site', () => {
    expect(readSite(sql)).toBeNull();
    writeSite(sql, { pathPrefix: '/public', indexDocument: 'index.html', spa: false, cacheControl: null });
    expect(readSite(sql)?.pathPrefix).toBe('/public');
    expect(deleteSite(sql)).toBe(true);
    expect(readSite(sql)).toBeNull();
  });

  it('serves a share and enforces expiry', async () => {
    const live = createShare(sql, '/public/assets/app.js', null, null);
    const response = await serveShare(fs, access, live, get(`/d/vol/${live.id}`));
    expect(response.status).toBe(200);
    expect(await response.text()).toBe('console.log(1)');

    const expired = createShare(sql, '/public/index.html', Math.floor(Date.now() / 1000) - 1, null);
    await expect(serveShare(fs, access, expired, get(`/d/vol/${expired.id}`)))
      .rejects.toMatchObject({ status: 410 });
  });
});

describe('public routing helpers', () => {
  it('extracts the volume from public paths', () => {
    expect(parsePublicVolume('/s/myvol/index.html')).toBe('myvol');
    expect(parsePublicVolume('/d/myvol/abc123')).toBe('myvol');
    expect(parsePublicVolume('/v1/volumes/myvol/files/a')).toBeNull();
    expect(parsePublicVolume('/s')).toBeNull();
  });

  it('resolves subdomain volumes only under the configured zone', () => {
    expect(subdomainVolume('myvol.sites.example.com', 'sites.example.com')).toBe('myvol');
    expect(subdomainVolume('myvol.sites.example.com:443', 'sites.example.com')).toBe('myvol');
    expect(subdomainVolume('sites.example.com', 'sites.example.com')).toBeNull();
    expect(subdomainVolume('a.b.sites.example.com', 'sites.example.com')).toBeNull();
    expect(subdomainVolume('myvol.other.com', 'sites.example.com')).toBeNull();
    expect(subdomainVolume('myvol.sites.example.com', undefined)).toBeNull();
  });
});
