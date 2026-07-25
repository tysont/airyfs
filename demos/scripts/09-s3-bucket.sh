# Expose a volume as an S3-compatible bucket (path-style, AWS SigV4).
# Point any S3 client at the /s3 endpoint with the volume name as the bucket.
# Access key is "airyfs"; the secret is the deployment root token.
aws s3 --endpoint-url https://airyfs-int.tyson-s-sandbox.workers.dev/s3 ls s3://airy-demo/

# Output:
# 2026-07-25 14:17:00         23 index.html
# 2026-07-25 14:17:00         16 style.css
