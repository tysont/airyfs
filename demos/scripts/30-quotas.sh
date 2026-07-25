# Set byte and inode quotas per volume.
airy volume quota --bytes 10485760

# Output:
# ┌───────────────┬───────────┐
# │ Resource      │ Limit     │
# ├───────────────┼───────────┤
# │ Logical bytes │ 10 MiB    │
# ├───────────────┼───────────┤
# │ Inodes        │ unlimited │
# └───────────────┴───────────┘
