# Search, glob, and grep across a volume (no Container needed).
airy grep quick /
airy glob '*.txt' /
airy find -n notes /

# Output:
# /notes.txt:1:5:the quick brown fox
# /note.txt
# /notes.txt
# /notes.txt
