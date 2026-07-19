// ABOUTME: Maintains the deployment-wide catalog of named AiryFS volumes.
// ABOUTME: Receives one-time registrations without joining ordinary volume request routing.

import { DurableObject } from 'cloudflare:workers';
import {
  initVolumeRegistry,
  listVolumes,
  registerVolume,
  type VolumePage,
  type VolumeRecord,
} from './volume-registry-storage';

export class VolumeRegistry extends DurableObject<Record<string, never>> {
  constructor(ctx: DurableObjectState, env: Record<string, never>) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => initVolumeRegistry(ctx.storage.sql));
  }

  register(name: string, chunkSize: number): VolumeRecord {
    return registerVolume(this.ctx.storage.sql, name, chunkSize);
  }

  list(after: string, limit: number): VolumePage {
    return listVolumes(this.ctx.storage.sql, after, limit);
  }
}
