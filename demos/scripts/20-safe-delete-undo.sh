# Delete safely with per-volume trash, restore, and undo.
airy rm /style.css
airy undo
airy ls /

# Output:
# Moved /style.css to trash
# Restored /style.css
# ┌───────────┬──────┬──────┬─────────────────────┐
# │ Name      │ Type │ Size │ Modified            │
# ├───────────┼──────┼──────┼─────────────────────┤
# │ style.css │ file │ 12 B │ ...                 │
# └───────────┴──────┴──────┴─────────────────────┘
