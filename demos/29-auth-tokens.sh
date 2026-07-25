# Lock a volume behind root credentials, passwords, or scoped tokens.
# (Requires the deployment to have AIRYFS_AUTH_SECRET set; the demo endpoint
# runs with auth disabled, so this is the command shape and expected output.)
airy capability create --operation read --path /notes --expires 1h

# Output:
# eyJpZCI6ImNhcC0uLi4iLCJ2b2x1bWUiOiJhaXJ5LWRlbW8iLCJvcGVyYXRpb25zIjpbInJlYWQiXSwicGF0aFByZWZpeGVzIjpbIi9ub3RlcyJdLCJleHBpcmVzIjoxNzg1MH0=...
# scope: read on /notes, expires in 1h
