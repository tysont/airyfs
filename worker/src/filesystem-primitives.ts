// ABOUTME: Implements small direct filesystem mutations missing from AgentFS's TypeScript API.
// ABOUTME: Keeps inode and dentry updates atomic in the volume's SQLite transaction boundary.

import { normalizePath } from './auth';
import { HttpError } from './files-api';
import type { SqlExec, TransactionSync } from './schema';

const S_IFMT = 0o170000;
const S_IFREG = 0o100000;
const S_IFDIR = 0o040000;
const S_IFLNK = 0o120000;
const DEFAULT_FILE_MODE = S_IFREG | 0o644;

interface InodeRow {
  ino: number;
  mode: number;
}

export interface DiskUsage {
  bytes: number;
  inodes: number;
}

function fsError(code: string, syscall: string, path: string, message: string): Error {
  return Object.assign(new Error(`${code}: ${message}, ${syscall} '${path}'`), { code, path });
}

function splitTimestamp(value: number): { seconds: number; nanoseconds: number } {
  if (!Number.isFinite(value) || value < 0) {
    throw new HttpError(400, 'INVALID_ARGUMENT', 'Timestamps must be non-negative finite numbers');
  }
  const seconds = Math.floor(value);
  return { seconds, nanoseconds: Math.floor((value - seconds) * 1_000_000_000) };
}

function currentTimestamp(): { seconds: number; nanoseconds: number } {
  const milliseconds = Date.now();
  return {
    seconds: Math.floor(milliseconds / 1000),
    nanoseconds: (milliseconds % 1000) * 1_000_000,
  };
}

export class FilesystemPrimitives {
  constructor(
    private readonly sql: SqlExec,
    private readonly transactionSync: TransactionSync,
  ) {}

  touch(rawPath: string, atime?: number, mtime?: number): void {
    const path = this.validatePath(rawPath, 'touch', true);
    const now = currentTimestamp();
    const access = atime === undefined ? now : splitTimestamp(atime);
    const modify = mtime === undefined ? now : splitTimestamp(mtime);

    this.transactionSync(() => {
      const inode = this.resolve(path, 'touch', false);
      if (inode) {
        this.rejectSymlink(inode, path, 'touch');
        this.sql.exec(
          `UPDATE fs_inode
           SET atime = ?, atime_nsec = ?, mtime = ?, mtime_nsec = ?, ctime = ?, ctime_nsec = ?
           WHERE ino = ?`,
          access.seconds, access.nanoseconds,
          modify.seconds, modify.nanoseconds,
          now.seconds, now.nanoseconds,
          inode.ino,
        );
        return;
      }

      const { parent, name } = this.resolveParent(path, 'touch');
      this.sql.exec(
        `INSERT INTO fs_inode
           (mode, nlink, uid, gid, size, atime, atime_nsec, mtime, mtime_nsec, ctime, ctime_nsec)
         VALUES (?, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?)`,
        DEFAULT_FILE_MODE,
        access.seconds, access.nanoseconds,
        modify.seconds, modify.nanoseconds,
        now.seconds, now.nanoseconds,
      );
      const created = this.sql.exec('SELECT last_insert_rowid() AS ino').toArray()[0];
      const ino = Number(created.ino);
      this.sql.exec('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)', name, parent.ino, ino);
      this.sql.exec('UPDATE fs_inode SET nlink = 1 WHERE ino = ?', ino);
      this.updateDirectoryTimes(parent.ino, now);
    });
  }

  chmod(rawPath: string, mode: number): void {
    const path = this.validatePath(rawPath, 'chmod', true);
    if (!Number.isSafeInteger(mode) || mode < 0 || mode > 0o7777) {
      throw new HttpError(400, 'INVALID_ARGUMENT', 'mode must be an integer between 0000 and 7777');
    }
    const now = currentTimestamp();
    this.transactionSync(() => {
      const inode = this.resolve(path, 'chmod', true)!;
      this.rejectSymlink(inode, path, 'chmod');
      this.sql.exec(
        'UPDATE fs_inode SET mode = (mode & ?) | ?, ctime = ?, ctime_nsec = ? WHERE ino = ?',
        S_IFMT, mode, now.seconds, now.nanoseconds, inode.ino,
      );
    });
  }

  link(rawExisting: string, rawPath: string): void {
    const existing = this.validatePath(rawExisting, 'link', true);
    const path = this.validatePath(rawPath, 'link');
    const now = currentTimestamp();

    this.transactionSync(() => {
      const source = this.resolve(existing, 'link', true)!;
      if ((source.mode & S_IFMT) === S_IFDIR) {
        throw fsError('EPERM', 'link', existing, 'hard link not allowed for directory');
      }
      if (this.resolve(path, 'link', false)) {
        throw fsError('EEXIST', 'link', path, 'file already exists');
      }
      const { parent, name } = this.resolveParent(path, 'link');
      this.sql.exec('INSERT INTO fs_dentry (name, parent_ino, ino) VALUES (?, ?, ?)', name, parent.ino, source.ino);
      this.sql.exec(
        'UPDATE fs_inode SET nlink = nlink + 1, ctime = ?, ctime_nsec = ? WHERE ino = ?',
        now.seconds, now.nanoseconds, source.ino,
      );
      this.updateDirectoryTimes(parent.ino, now);
    });
  }

  diskUsage(rawPath: string): DiskUsage {
    const path = this.validatePath(rawPath, 'du', true);
    const root = this.resolve(path, 'du', true)!;
    const row = this.sql.exec(
      `WITH RECURSIVE subtree(ino) AS (
         VALUES (?)
         UNION
         SELECT d.ino FROM fs_dentry d JOIN subtree s ON d.parent_ino = s.ino
       )
       SELECT count(*) AS inodes, coalesce(sum(i.size), 0) AS bytes
       FROM subtree s JOIN fs_inode i ON i.ino = s.ino`,
      root.ino,
    ).toArray()[0];
    return { bytes: Number(row.bytes), inodes: Number(row.inodes) };
  }

  updateCtime(rawPath: string): void {
    const path = this.validatePath(rawPath, 'append', true);
    const now = currentTimestamp();
    this.transactionSync(() => {
      const inode = this.resolve(path, 'append', true)!;
      this.sql.exec('UPDATE fs_inode SET ctime = ?, ctime_nsec = ? WHERE ino = ?', now.seconds, now.nanoseconds, inode.ino);
    });
  }

  private validatePath(rawPath: string, syscall: string, allowRoot = false): string {
    if (typeof rawPath !== 'string' || rawPath.length === 0 || rawPath.includes('\0')) {
      throw new HttpError(400, 'INVALID_ARGUMENT', `Invalid path for ${syscall}`);
    }
    const path = normalizePath(rawPath);
    if (!allowRoot && path === '/') throw fsError('EEXIST', syscall, path, 'volume root already exists');
    return path;
  }

  private resolve(path: string, syscall: string, required: boolean): InodeRow | null {
    let current = this.inode(1);
    if (!current) throw fsError('ENOENT', syscall, path, 'volume root does not exist');
    if (path === '/') return current;

    for (const name of path.split('/').filter(Boolean)) {
      if ((current.mode & S_IFMT) !== S_IFDIR) {
        throw fsError('ENOTDIR', syscall, path, 'path component is not a directory');
      }
      const rows = this.sql.exec(
        `SELECT i.ino, i.mode FROM fs_dentry d
         JOIN fs_inode i ON i.ino = d.ino
         WHERE d.parent_ino = ? AND d.name = ?`,
        current.ino, name,
      ).toArray();
      if (rows.length === 0) {
        if (required) throw fsError('ENOENT', syscall, path, 'no such file or directory');
        return null;
      }
      current = { ino: Number(rows[0].ino), mode: Number(rows[0].mode) };
    }
    return current;
  }

  private resolveParent(path: string, syscall: string): { parent: InodeRow; name: string } {
    const parts = path.split('/').filter(Boolean);
    const name = parts.pop();
    if (!name) throw fsError('EINVAL', syscall, path, 'invalid path');
    const parentPath = parts.length === 0 ? '/' : `/${parts.join('/')}`;
    const parent = this.resolve(parentPath, syscall, true)!;
    if ((parent.mode & S_IFMT) !== S_IFDIR) {
      throw fsError('ENOTDIR', syscall, parentPath, 'not a directory');
    }
    return { parent, name };
  }

  private inode(ino: number): InodeRow | null {
    const rows = this.sql.exec('SELECT ino, mode FROM fs_inode WHERE ino = ?', ino).toArray();
    return rows.length === 0 ? null : { ino: Number(rows[0].ino), mode: Number(rows[0].mode) };
  }

  private rejectSymlink(inode: InodeRow, path: string, syscall: string): void {
    if ((inode.mode & S_IFMT) === S_IFLNK) {
      throw fsError('ELOOP', syscall, path, 'symbolic links are not followed by direct operations');
    }
  }

  private updateDirectoryTimes(ino: number, timestamp: { seconds: number; nanoseconds: number }): void {
    this.sql.exec(
      'UPDATE fs_inode SET mtime = ?, mtime_nsec = ?, ctime = ?, ctime_nsec = ? WHERE ino = ?',
      timestamp.seconds, timestamp.nanoseconds, timestamp.seconds, timestamp.nanoseconds, ino,
    );
  }
}
