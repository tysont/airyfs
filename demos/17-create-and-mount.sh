# Create and mount a new volume in one command.
airy mount create /data2 --target airy-demo-bigfs --create
airy mount list

# Output:
# Mounted airy-demo-bigfs:/ at /data2
# ┌────────────┬─────────────────┬─────────┐
# │ Mountpoint │ Target Volume   │ Subpath │
# ├────────────┼─────────────────┼─────────┤
# │ /data      │ airy-demo2      │ /       │
# ├────────────┼─────────────────┼─────────┤
# │ /data2     │ airy-demo-bigfs │ /       │
# └────────────┴─────────────────┴─────────┘
