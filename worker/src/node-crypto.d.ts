// ABOUTME: Minimal ambient types for the node:crypto SHA-256 surface used under nodejs_compat.
// ABOUTME: Avoids pulling all of @types/node (which conflicts with @cloudflare/workers-types globals).

declare module 'node:crypto' {
  interface Hash {
    update(data: Uint8Array | string): Hash;
    digest(encoding: 'hex'): string;
  }
  export function createHash(algorithm: string): Hash;
}
