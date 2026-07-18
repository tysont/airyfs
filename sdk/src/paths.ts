// ABOUTME: Canonicalizes and URL-encodes paths inside a AiryFS volume.
// ABOUTME: Avoids Node dependencies so path helpers work in browsers and Workers.

export function resolveRemotePath(cwd: string, input = '.'): string {
  const source = input.startsWith('/') ? input : `${cwd}/${input || '.'}`;
  const segments: string[] = [];
  for (const segment of source.split('/')) {
    if (segment === '' || segment === '.') continue;
    if (segment === '..') segments.pop();
    else segments.push(segment);
  }
  return `/${segments.join('/')}`;
}

export function encodeRemotePath(path: string): string {
  return resolveRemotePath('/', path).split('/').filter(Boolean).map(encodeURIComponent).join('/');
}

export function remoteBasename(path: string): string {
  const segments = resolveRemotePath('/', path).split('/').filter(Boolean);
  return segments.at(-1) ?? '';
}

export function remoteDirname(path: string): string {
  const segments = resolveRemotePath('/', path).split('/').filter(Boolean);
  segments.pop();
  return `/${segments.join('/')}`;
}
