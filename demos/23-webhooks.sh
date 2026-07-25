# Fire webhooks when files change.
airy webhook create https://example.com/airyfs-hook
airy webhook list

# Output:
# Created webhook 20296916-1f23-487b-bd17-cf4ddb6eb812
# uuxE-CE6N2w5YwnYFGUzzv9AwdqKVIIM9t47QordL54
# The signing secret is shown once. Store it securely.
# ┌──────────────┬─────────────────────────────────┬──────┬─────────────────────────────┐
# │ ID           │ URL                             │ Path │ Events                      │
# ├──────────────┼─────────────────────────────────┼──────┼─────────────────────────────┤
# │ 20296916-... │ https://example.com/airyfs-hook │ /    │ create,modify,remove,rename │
# └──────────────┴─────────────────────────────────┴──────┴─────────────────────────────┘
