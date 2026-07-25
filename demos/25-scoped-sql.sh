# Run scoped SQL against your own tables beside your files.
airy sql 'CREATE TABLE IF NOT EXISTS app_notes (id INTEGER PRIMARY KEY, body TEXT)'
airy sql "INSERT INTO app_notes (body) VALUES ('buy milk')"
airy sql 'SELECT * FROM app_notes'

# Output:
# SQL executed; 2 rows written
# SQL executed; 1 row written
# ┌────┬──────────┐
# │ id │ body     │
# ├────┼──────────┤
# │ 1  │ buy milk │
# └────┴──────────┘
