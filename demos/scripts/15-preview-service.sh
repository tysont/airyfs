# Run long-lived preview services with public URLs.
airy service create web --public -- python3 -m http.server '$PORT'
airy service list

# Output:
# Created preview service web on $PORT=5000 at https://airyfs-int.tyson-s-sandbox.workers.dev/p/airy-demo/web/
# ┌──────┬──────┬─────────┬────────┬───────────┬────────────────────────────────┐
# │ Name │ Port │ Enabled │ Public │ Directory │ Command                        │
# ├──────┼──────┼─────────┼────────┼───────────┼────────────────────────────────┤
# │ web  │ 5000 │ yes     │ yes    │ /         │ python3 -m http.server '$PORT' │
# └──────┴──────┴─────────┴────────┴───────────┴────────────────────────────────┘
