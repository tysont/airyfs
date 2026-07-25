# Mount other volumes as subdirectories to grow past one volume's limit.
airy mount create /data --target airy-demo2
echo 'written via the mount' | airy write /data/new.txt
# the write landed in the OTHER volume:
airy --session demo2 cat /new.txt

# Output:
# Mounted airy-demo2:/ at /data
# Wrote /data/new.txt
# written via the mount
