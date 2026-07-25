# Open a volume in Finder or any WebDAV client.
# macOS: open 'https://airyfs-int.tyson-s-sandbox.workers.dev/dav/airy-demo/'
curl -sS -X OPTIONS -D - -o /dev/null https://airyfs-int.tyson-s-sandbox.workers.dev/dav/airy-demo/ | grep -iE '^(dav|allow):'

# Output:
# allow: OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY, LOCK, UNLOCK
# dav: 1, 2
