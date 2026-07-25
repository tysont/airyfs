# Create a real filesystem and run things in the cloud.
airy volume create
echo 'hello from the cloud' | airy write /hello.txt
airy ls /
airy exec wc -w hello.txt

# Output:
# Volume airy-cloud uses 256 KiB chunks
# Wrote /hello.txt
# ┌───────────┬──────┬──────┬───────────────────────┐
# │ Name      │ Type │ Size │ Modified              │
# ├───────────┼──────┼──────┼───────────────────────┤
# │ hello.txt │ file │ 21 B │ 7/25/2026, 9:53:32 AM │
# └───────────┴──────┴──────┴───────────────────────┘
# 4 hello.txt
