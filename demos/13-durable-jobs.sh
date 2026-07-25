# Queue durable background jobs that survive restarts.
JID=$(airy job submit sh -c 'echo building; sleep 1; echo done' | awk 'END{print $NF}')
airy job logs "$JID"

# Output:
# Submitted job c316b639-f2b8-49f6-85b0-e09f22bfe580
# building
# done
