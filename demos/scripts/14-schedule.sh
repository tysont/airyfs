# Schedule recurring commands like cron.
airy schedule create nightly '0 3 * * *' echo backup
airy schedule list

# Output:
# Created schedule nightly
# ┌──────────────┬─────────┬────────────┬─────────┬───────────────────────┬─────────────┐
# │ ID           │ Name    │ Cron (UTC) │ Enabled │ Next                  │ Command     │
# ├──────────────┼─────────┼────────────┼─────────┼───────────────────────┼─────────────┤
# │ ea7e3caf-... │ nightly │ 0 3 * * *  │ yes     │ 7/25/2026, 8:00:00 PM │ echo backup │
# └──────────────┴─────────┴────────────┴─────────┴───────────────────────┴─────────────┘
