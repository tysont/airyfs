# Give Linux programs a real filesystem at /volume.
airy exec ls -la /volume

# Output:
# total 10
# drwxr-xr-x  1 root root 4096 ... .
# drwxr-xr-x 20 root root 4096 ... ..
# -rw-r--r--  1 root root    0 ... event.txt
# -rw-r--r--  1 root root    3 ... log.txt
# -rw-r--r--  1 root root    9 ... note.txt
# -rw-r--r--  1 root root   20 ... notes.txt
