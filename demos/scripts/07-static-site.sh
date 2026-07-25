# Host a static website from a volume.
echo '<h1>Hi from AiryFS</h1>' | airy write /index.html
airy site publish

# Output:
# Wrote /index.html
# Published airy-demo at https://airyfs-int.tyson-s-sandbox.workers.dev/s/airy-demo/
