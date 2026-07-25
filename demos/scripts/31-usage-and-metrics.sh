# Watch usage, history, and Prometheus metrics.
airy status
airy metrics | grep -E 'filesystem_bytes_used|filesystem_inodes|sqlite_bytes'

# Output:
# ┌────────────────┬────────────────────────────────────────────────┐
# │ Component      │ Status                                         │
# ├────────────────┼────────────────────────────────────────────────┤
# │ Endpoint       │ https://airyfs-int.tyson-s-sandbox.workers.dev │
# │ Volume         │ airy-demo                                      │
# │ Container      │ stopped                                        │
# │ SQLite         │ 388 KiB                                        │
# └────────────────┴────────────────────────────────────────────────┘
# airyfs_filesystem_bytes_used 32
# airyfs_filesystem_inodes 4
# airyfs_sqlite_bytes 397312
