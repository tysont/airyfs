# Stream command output live or run interactively in a PTY.
airy exec sh -c 'for i in 1 2 3; do echo "line $i"; sleep 1; done'
# (interactive shell: airy exec --pty bash)

# Output (arrives one line per second, live):
# line 1
# line 2
# line 3
