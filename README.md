# dbtool


## CLI
Help text from the command line interface CLI/dbtool.php:
```
Usage:
php dbtool.php [OPTIONS] SCHEMAFILE [SCHEMAFILE...]
php dbtool.php [OPTIONS] --config=CONFIGFILE

General Options:
  -h, --help                       Displays this help text.

  -cVALUE, --config=VALUE          Loads a config file.
  -dVALUE, --database=VALUE        An execution will use the given database, if a database isn't specified in the schemafile.
  -e, --execute                    Run the generated SQL to align the database with the provided schema.
  -f, --force                      Combined with -e: Run any SQL without asking first.
  --no-alter                       An execution will not include ALTER statements.
  --no-create                      An execution will not include CREATE statements.
  --no-drop                        An execution will not include DROP statements.
  -p[VALUE], --password[=VALUE]    Use given password or if not set, request password before connecting to the database.
  -uVALUE, --user=VALUE            Use the given username when connecting to the database.
  -v, --verbose                    Write extra descriptive output.
  -wKEY=VALUE, --var KEY=VALUE     Define a variable to be inserted in the schema.

  --test                           Run everything as usual, but without executing any SQL.
```
Useful bash alias:
```
alias dbtool='php /path/to/dbtool/cli/dbtool.php'
```
