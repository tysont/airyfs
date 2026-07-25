# Share files between machines.
# Export the session on machine A, import the blob on machine B, and both
# read and write the same volume.
airy session export demo

# Output:
# This blob contains a credential. Share it only over a trusted channel.
# airyfs1:eyJuYW1lIjoiZGVtbyIsImVuZHBvaW50IjoiaHR0cHM6Ly9haXJ5ZnMtaW50Li4uIiwidm9sdW1lIjoiYWlyeS1kZW1vIn0=
