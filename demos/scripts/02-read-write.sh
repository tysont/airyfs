# Read and write files over HTTP, an SDK, or a CLI.
airy mkdir -p /notes
echo 'hello from the cloud' | airy write /notes/todo.txt
airy cat /notes/todo.txt

# Output:
# Created /notes
# Wrote /notes/todo.txt
# hello from the cloud
