# Import and export whole subtrees as archives.
airy push ./proj /backup           # upload a local tree transactionally
airy tree /backup                  # (airy pull /backup ./restored downloads it)

# Output:
# Pushed ./proj to /backup (2 files, 1 dirs, 0 symlinks, 11 bytes)
# /backup
# a.txt
# src/
#   b.txt
