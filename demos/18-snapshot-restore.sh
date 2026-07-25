# Snapshot a volume instantly and restore just as fast (with a diff in between).
echo original | airy write /note.txt
ID=$(airy snapshot create before | awk '{print $NF}')
echo changed | airy write /note.txt
airy snapshot diff "$ID"
airy snapshot restore "$ID" --force
airy cat /note.txt

# Output:
# Wrote /note.txt
# M /note.txt
# Restored airy-demo from snapshot before
# original
