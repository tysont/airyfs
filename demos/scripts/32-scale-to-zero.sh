# Scale compute to zero while direct APIs keep working.
airy destroy --force
airy cat /note.txt          # still served straight from Durable Object SQLite

# Output:
# Destroyed Container for airy-demo; volume data persists
# original
