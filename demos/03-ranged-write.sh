# Patch huge files in place with random-access ranged writes.
printf 'color: red\n' | airy write /style.css
printf 'blue ' | airy write --offset 7 /style.css
airy cat /style.css

# Output:
# Wrote /style.css
# Wrote 5 bytes to /style.css at offset 7
# color: blue
