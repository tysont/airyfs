# Store content-addressed assets by hash.
H=$(airy asset put ./local.txt | awk '{print $NF}')
airy asset get "$H"

# Output:
# Published asset 6e459fed18ddb06d57c8e9f0d000c302c7e01389926db6e89884bfbe91a2a5df
# Downloaded asset 6e459fed18ddb06d57c8e9f0d000c302c7e01389926db6e89884bfbe91a2a5df to 6e459fed...
