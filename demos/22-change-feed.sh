# Subscribe to a change feed and long-poll for new events.
airy touch /event.txt
airy watch --once /event.txt          # or: airy watch /  (follows live)

# Output:
# A /event.txt
