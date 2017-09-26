# dbtool


## CLI
Help text from the command line interface CLI/dbtool.php:
```
Usage: php dbtool.php [OPTIONS] CONFIGFILE [OPTIONS]

General Options:
  -h, --help                 Displays this help text.

  -a, --action ACTION        Specify the action used, supported actions are 'diff' and 'permission'.
  -e, --execute              Run the generated SQL to align the database with the provided schema.
  -f, --force                Combined with -e: Run any SQL without asking first.
  -p, --password             Request password before connecting to the database.
  -u, --username USERNAME    Use the given username when connecting to the database.
  -v, --verbose              Write extra descriptive output.

  --test                     Run everything as usual, but without executing any SQL.

Diff Specific Options:
  --no-alter                 An executed diff will not include ALTER statements.
  --no-create                An executed diff will not include CREATE statements.
  --no-drop                  An executed diff will not include DROP statements.
```
Useful bash alias:
```
alias dbtool='php /path/to/dbtool/cli/dbtool.php'
```
