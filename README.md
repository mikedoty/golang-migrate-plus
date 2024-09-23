<!-- [![GitHub Workflow Status (branch)](https://img.shields.io/github/actions/workflow/status/golang-migrate/migrate/ci.yaml?branch=master)](https://github.com/golang-migrate/migrate/actions/workflows/ci.yaml?query=branch%3Amaster) -->
<!-- [![GoDoc](https://pkg.go.dev/badge/github.com/golang-migrate/migrate)](https://pkg.go.dev/github.com/golang-migrate/migrate/v4) -->
<!-- [![Coverage Status](https://img.shields.io/coveralls/github/golang-migrate/migrate/master.svg)](https://coveralls.io/github/golang-migrate/migrate?branch=master) -->
<!-- [![packagecloud.io](https://img.shields.io/badge/deb-packagecloud.io-844fec.svg)](https://packagecloud.io/golang-migrate/migrate?filter=debs) -->
<!-- [![Docker Pulls](https://img.shields.io/docker/pulls/migrate/migrate.svg)](https://hub.docker.com/r/migrate/migrate/) -->
![Supported Go Versions](https://img.shields.io/badge/Go-1.21%2C%201.22-lightgrey.svg)
<!-- [![GitHub Release](https://img.shields.io/github/release/golang-migrate/migrate.svg)](https://github.com/golang-migrate/migrate/releases) -->
<!-- [![Go Report Card](https://goreportcard.com/badge/github.com/golang-migrate/migrate/v4)](https://goreportcard.com/report/github.com/golang-migrate/migrate/v4) -->

# golang-migrate-plus

This project is based off of the golang-migrate package.

## Migration History

golang-migrate-plus adds migration history tracking, so that you can identify which migrations you have previously
applied.  By default, this will exist in the `schema_migrations_history` table.

> You can choose a different name for the history table via the `x-migrations-history-table` option.

### x-migrations-history-enabled (default: false)

Migration history is disabled by default.  To enable migration history, provide `x-migrations-history-enabled=true` in
your connection string.

## Sourcing

golang-migrate-plus also adds migration file sourcing support via the `source` and `exec` commands:

### x-migrations-path
To better support sourcing and allow you to store your views, procedures, etc. in a sibling folder,
golang-migrate-plus adds an optional `x-migrations-path` option to the `file://` driver.  (No other
source driver currently supports the migrations path option.)

Use it like this:

```
// Previously:
// file:///app/migrations

// Now:
file:///app?x-migrations-path=migrations
```

> Providing `/app` as your mount point will allow you to include e.g. `/app/views` in your file mount, so that your migration files can reference relative paths such as `../views/some_view_definition.sql`.

### Usage

- Use `source` to import a separate sql file into your migration script.  Example:

  ```
  create table if not exists users(id int, name varchar(80));

  -- Suppose this view selects * from users where len(name) > 10
  source "../views/vw_users_with_long_names.sql";
  ```

  > Before the migration is applied, the contents of `vw_users_with_long_names.sql` will be directly inserted into the migration contents, and the result will be run as a single sql execution.

- Use `exec` to run a separate sql file as an independent sql execution.

  ```
  create table if not exists animals(id int, color varchar(80));

  -- Suppose this inserts 5 rows into the animals table
  exec "../seed_data/add_5_animals.sql";
  ```

  > The `create table` command will be run as its own sql execution; next, the contents of `add_5_animals.sql` will be run as a second sql execution.
  >
  > This is useful for database types which do not support, for example, creating multiple procedures within a single sql execution, and would otherwise require a separate migration script for each procedure.
  >
  > Sourcing allows you to define your views, procedures, etc. in source control, more easily track diffs to the file, and avoid race conditions whereby separate developers make simultaneous edits to a view and overwrite one or the other's changes.

## Database Support

golang-migrate-plus currently only covers a small subset of databases.  Check the table below to see if your database is supported.

Database | Supported? | Notes
--------|------------|--------
**Postgres** | :white_check_mark: | All features supported
**MySQL**    | :white_check_mark: | No TRUNCATE trigger support for manual truncate commands on schema_migrations
**Singlestore** | :white_check_mark: | No triggers support for manual changes to schema_migrations
**Others** | :x: | -

## Forced Transactional Migrations via x-force-transactional-migrations (default: false)

By default, golang-migrate and golang-migrate-plus do not run migrations within a transaction.
Typically individual migration files will explicitly opt in by beginning and ending with a `begin`/`commit`
(or equivalent transaction statements, depending on choice of database).

Enabling `x-force-transactional-migrations` will force each migration to run in a transaction.
Do note that some database types have special cases that supercede transactions; for instance,
a CREATE TABLE statement in MySQL will immediately commit the active transaction.  Consult your
database's documentation for more information.

> Note:  If existing migration files already have e.g. `BEGIN` and `COMMIT` statements, they will not conflict with the `x-force-transactional-migrations` option.  They will simply be ignored, and one single transaction will run.


# migrate (Original base package)

__Database migrations written in Go. Use as [CLI](#cli-usage) or import as [library](#use-in-your-go-project).__

* Migrate reads migrations from [sources](#migration-sources)
   and applies them in correct order to a [database](#databases).
* Drivers are "dumb", migrate glues everything together and makes sure the logic is bulletproof.
   (Keeps the drivers lightweight, too.)
* Database drivers don't assume things or try to correct user input. When in doubt, fail.

Forked from [mattes/migrate](https://github.com/mattes/migrate)

## Databases

Database drivers run migrations. [Add a new database?](database/driver.go)

* [PostgreSQL](database/postgres)
* [PGX v4](database/pgx)
* [PGX v5](database/pgx/v5)
* [Redshift](database/redshift)
* [Ql](database/ql)
* [Cassandra / ScyllaDB](database/cassandra)
* [SQLite](database/sqlite)
* [SQLite3](database/sqlite3) ([todo #165](https://github.com/mattes/migrate/issues/165))
* [SQLCipher](database/sqlcipher)
* [MySQL / MariaDB](database/mysql)
* [Neo4j](database/neo4j)
* [MongoDB](database/mongodb)
* [CrateDB](database/crate) ([todo #170](https://github.com/mattes/migrate/issues/170))
* [Shell](database/shell) ([todo #171](https://github.com/mattes/migrate/issues/171))
* [Google Cloud Spanner](database/spanner)
* [CockroachDB](database/cockroachdb)
* [YugabyteDB](database/yugabytedb)
* [ClickHouse](database/clickhouse)
* [Firebird](database/firebird)
* [MS SQL Server](database/sqlserver)
* [rqlite](database/rqlite)

### Database URLs

Database connection strings are specified via URLs. The URL format is driver dependent but generally has the form: `dbdriver://username:password@host:port/dbname?param1=true&param2=false`

Any [reserved URL characters](https://en.wikipedia.org/wiki/Percent-encoding#Percent-encoding_reserved_characters) need to be escaped. Note, the `%` character also [needs to be escaped](https://en.wikipedia.org/wiki/Percent-encoding#Percent-encoding_the_percent_character)

Explicitly, the following characters need to be escaped:
`!`, `#`, `$`, `%`, `&`, `'`, `(`, `)`, `*`, `+`, `,`, `/`, `:`, `;`, `=`, `?`, `@`, `[`, `]`

It's easiest to always run the URL parts of your DB connection URL (e.g. username, password, etc) through an URL encoder. See the example Python snippets below:

```bash
$ python3 -c 'import urllib.parse; print(urllib.parse.quote(input("String to encode: "), ""))'
String to encode: FAKEpassword!#$%&'()*+,/:;=?@[]
FAKEpassword%21%23%24%25%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D
$ python2 -c 'import urllib; print urllib.quote(raw_input("String to encode: "), "")'
String to encode: FAKEpassword!#$%&'()*+,/:;=?@[]
FAKEpassword%21%23%24%25%26%27%28%29%2A%2B%2C%2F%3A%3B%3D%3F%40%5B%5D
$
```

## Migration Sources

Source drivers read migrations from local or remote sources. [Add a new source?](source/driver.go)

* [Filesystem](source/file) - read from filesystem
* [io/fs](source/iofs) - read from a Go [io/fs](https://pkg.go.dev/io/fs#FS)
* [Go-Bindata](source/go_bindata) - read from embedded binary data ([jteeuwen/go-bindata](https://github.com/jteeuwen/go-bindata))
* [pkger](source/pkger) - read from embedded binary data ([markbates/pkger](https://github.com/markbates/pkger))
* [GitHub](source/github) - read from remote GitHub repositories
* [GitHub Enterprise](source/github_ee) - read from remote GitHub Enterprise repositories
* [Bitbucket](source/bitbucket) - read from remote Bitbucket repositories
* [Gitlab](source/gitlab) - read from remote Gitlab repositories
* [AWS S3](source/aws_s3) - read from Amazon Web Services S3
* [Google Cloud Storage](source/google_cloud_storage) - read from Google Cloud Platform Storage

## CLI usage

* Simple wrapper around this library.
* Handles ctrl+c (SIGINT) gracefully.
* No config search paths, no config files, no magic ENV var injections.

__[CLI Documentation](cmd/migrate)__

### Basic usage

```bash
$ migrate -source file://path/to/migrations -database postgres://localhost:5432/database up 2
```

### Docker usage

```bash
$ docker run -v {{ migration dir }}:/migrations --network host migrate/migrate
    -path=/migrations/ -database postgres://localhost:5432/database up 2
```

## Use in your Go project

* API is stable and frozen for this release (v3 & v4).
* Uses [Go modules](https://golang.org/cmd/go/#hdr-Modules__module_versions__and_more) to manage dependencies.
* To help prevent database corruptions, it supports graceful stops via `GracefulStop chan bool`.
* Bring your own logger.
* Uses `io.Reader` streams internally for low memory overhead.
* Thread-safe and no goroutine leaks.

__[Go Documentation](https://pkg.go.dev/github.com/golang-migrate/migrate/v4)__

```go
import (
    "github.com/golang-migrate/migrate/v4"
    _ "github.com/golang-migrate/migrate/v4/database/postgres"
    _ "github.com/golang-migrate/migrate/v4/source/github"
)

func main() {
    m, err := migrate.New(
        "github://mattes:personal-access-token@mattes/migrate_test",
        "postgres://localhost:5432/database?sslmode=enable")
    m.Steps(2)
}
```

Want to use an existing database client?

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
    "github.com/golang-migrate/migrate/v4"
    "github.com/golang-migrate/migrate/v4/database/postgres"
    _ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
    db, err := sql.Open("postgres", "postgres://localhost:5432/database?sslmode=enable")
    driver, err := postgres.WithInstance(db, &postgres.Config{})
    m, err := migrate.NewWithDatabaseInstance(
        "file:///migrations",
        "postgres", driver)
    m.Up() // or m.Step(2) if you want to explicitly set the number of migrations to run
}
```

## Getting started

Go to [getting started](GETTING_STARTED.md)

## Tutorials

* [CockroachDB](database/cockroachdb/TUTORIAL.md)
* [PostgreSQL](database/postgres/TUTORIAL.md)

(more tutorials to come)

## Migration files

Each migration has an up and down migration. [Why?](FAQ.md#why-two-separate-files-up-and-down-for-a-migration)

```bash
1481574547_create_users_table.up.sql
1481574547_create_users_table.down.sql
```

[Best practices: How to write migrations.](MIGRATIONS.md)

## Coming from another db migration tool?

Check out [migradaptor](https://github.com/musinit/migradaptor/).
*Note: migradaptor is not affiliated or supported by this project*

## Versions

Version | Supported? | Import | Notes
--------|------------|--------|------
**master** | :white_check_mark: | `import "github.com/golang-migrate/migrate/v4"` | New features and bug fixes arrive here first |
**v4** | :white_check_mark: | `import "github.com/golang-migrate/migrate/v4"` | Used for stable releases |
**v3** | :x: | `import "github.com/golang-migrate/migrate"` (with package manager) or `import "gopkg.in/golang-migrate/migrate.v3"` (not recommended) | **DO NOT USE** - No longer supported |

## Development and Contributing

Yes, please! [`Makefile`](Makefile) is your friend,
read the [development guide](CONTRIBUTING.md).

Also have a look at the [FAQ](FAQ.md).

---

Looking for alternatives? [https://awesome-go.com/#database](https://awesome-go.com/#database).
