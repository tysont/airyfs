// ABOUTME: Public entrypoint for the universal AiryFS TypeScript SDK.
// ABOUTME: Re-exports the complete client, DTOs, errors, streams, paths, and workflows.

export { AiryFSClient } from './client.js';
export { AiryFSApiError, AiryFSTransportError, responseError } from './errors.js';
export { NdjsonDecoder, NdjsonError, decodeNdjsonStream } from './ndjson.js';
export { encodeRemotePath, remoteBasename, remoteDirname, resolveRemotePath } from './paths.js';
export {
  RESUMABLE_CHUNK_BYTES,
  drainJobLogs,
  execStreamWithId,
  followJobLogs,
  resumableUploadBlob,
  tailFile,
  waitForJob,
  watchChanges,
} from './helpers.js';
export type * from './types.js';
