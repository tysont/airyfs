# Fork a live volume or clone a snapshot into a new one.
airy volume fork airy-demo-fork
# the fork is an independent copy with the same contents:
airy session create fork -e "$AIRYFS_ENDPOINT" -v airy-demo-fork
airy --session fork cat /note.txt

# Output:
# Forked volume airy-demo to airy-demo-fork
# original
