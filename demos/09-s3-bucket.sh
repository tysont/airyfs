# Expose a volume as an S3-compatible bucket.
# S3 uses the deployment root credential (AWS SigV4); configure it once, then
# point any S3 client at the endpoint with the volume name as the bucket.
aws configure set aws_access_key_id "$AIRYFS_ROOT_TOKEN"
aws configure set aws_secret_access_key "$AIRYFS_ROOT_TOKEN"
aws s3 --endpoint-url https://airyfs-int.tyson-s-sandbox.workers.dev ls s3://airy-demo/

# Output:
# 2026-07-25 08:49:51        24 index.html
# 2026-07-25 08:49:49        12 style.css
