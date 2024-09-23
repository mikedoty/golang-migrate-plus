package postgres

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/golang-migrate/migrate/v4"

	"github.com/dhui/dktest"

	"github.com/golang-migrate/migrate/v4/database"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	"github.com/golang-migrate/migrate/v4/source"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const (
	pgPassword = "postgres"
)

var (
	opts = dktest.Options{
		Env:          map[string]string{"POSTGRES_PASSWORD": pgPassword},
		PortRequired: true, ReadyFunc: isReady}
	// Supported versions: https://www.postgresql.org/support/versioning/
	specs = []dktesting.ContainerSpec{
		// {ImageName: "postgres:9.5", Options: opts},
		// {ImageName: "postgres:9.6", Options: opts},
		// {ImageName: "postgres:10", Options: opts},
		// {ImageName: "postgres:11", Options: opts},
		// {ImageName: "postgres:12", Options: opts},
		{ImageName: "postgres:16-alpine", Options: opts},
	}
)

func pgConnectionString(host, port string, options ...string) string {
	options = append(options, "sslmode=disable")
	return fmt.Sprintf("postgres://postgres:%s@[%s]:%s/postgres?%s", pgPassword, host, port, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.FirstPort()
	if err != nil {
		return false
	}

	db, err := sql.Open("postgres", pgConnectionString(ip, port))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()
	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	return true
}

func mustRun(t *testing.T, d database.Driver, statements []string) {
	for _, statement := range statements {
		if err := d.Run(strings.NewReader(statement)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAllThenCleanup(t *testing.T) {
	t.Run("testConnection", TestConnection)
	t.Run("testMigrate", TestMigrate)
	t.Run("testMultipleStatements", TestMultipleStatements)
	t.Run("testMultipleStatementsInMultiStatementMode", TestMultipleStatementsInMultiStatementMode)
	t.Run("testErrorParsing", TestErrorParsing)
	t.Run("testFilterCustomQuery", TestFilterCustomQuery)
	t.Run("testWithSchema", TestWithSchema)
	t.Run("testMigrationTableOption", TestMigrationTableOption)
	t.Run("testFailToCreateTableWithoutPermissions", TestFailToCreateTableWithoutPermissions)
	t.Run("testCheckBeforeCreateTable", TestCheckBeforeCreateTable)
	t.Run("testParallelSchema", TestParallelSchema)
	t.Run("testPostgresLock", TestPostgresLock)
	t.Run("testWithInstanceConcurrent", TestWithInstanceConcurrent)
	t.Run("testWithConnection", TestWithConnection)

	t.Cleanup(func() {
		for _, spec := range specs {
			t.Log("Cleaning up ", spec.ImageName)
			if err := spec.Cleanup(); err != nil {
				t.Error("Error removing ", spec.ImageName, "error:", err)
			}
		}
	})
}

func TestConnection(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "postgres", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatements(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLE bar (bar text);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var exists bool
		if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'bar' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func TestMultipleStatementsInMultiStatementMode(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port, "x-multi-statement=true")
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE INDEX CONCURRENTLY idx_foo ON foo (foo);")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure created index exists
		var exists bool
		if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = (SELECT current_schema()) AND indexname = 'idx_foo')").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table bar to exist")
		}
	})
}

func TestErrorParsing(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		wantErr := `migration failed: syntax error at or near "TABLEE" (column 37) in line 1: CREATE TABLE foo ` +
			`(foo text); CREATE TABLEE bar (bar text); (details: pq: syntax error at or near "TABLEE")`
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo text); CREATE TABLEE bar (bar text);")); err == nil {
			t.Fatal("expected err but got nil")
		} else if err.Error() != wantErr {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}
	})
}

func TestFilterCustomQuery(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		connStr := pgConnectionString(ip, port, "x-custom=foobar")
		p := &Postgres{}
		d, err := p.Open(connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
	})
}

func TestWithSchema(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// create foobar schema
		if err := d.Run(strings.NewReader("CREATE SCHEMA foobar AUTHORIZATION postgres")); err != nil {
			t.Fatal(err)
		}
		if _, err := d.SetVersion(1, false, false, nil); err != nil {
			t.Fatal(err)
		}

		// re-connect using that schema
		connStr := pgConnectionString(ip, port, "search_path=foobar")
		d2, err := p.Open(connStr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d2.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		version, _, err := d2.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != database.NilVersion {
			t.Fatal("expected NilVersion")
		}

		// now update version and compare
		if _, err := d2.SetVersion(2, false, false, nil); err != nil {
			t.Fatal(err)
		}
		version, _, err = d2.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != 2 {
			t.Fatal("expected version 2")
		}

		// meanwhile, the public schema still has the other version
		version, _, err = d.Version()
		if err != nil {
			t.Fatal(err)
		}
		if version != 1 {
			t.Fatalf("expected version 1")
		}
	})
}

func TestMigrationTableOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, _ := p.Open(addr)
		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// create migrate schema
		if err := d.Run(strings.NewReader("CREATE SCHEMA migrate AUTHORIZATION postgres")); err != nil {
			t.Fatal(err)
		}

		// bad unquoted x-migrations-table parameter
		wantErr := "x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: migrate.schema_migrations"
		d, err = p.Open(pgConnectionString(ip, port, "x-migrations-table=migrate.schema_migrations", "x-migrations-table-quoted=1"))
		if err == nil {
			t.Fatalf("expected '%s' but got no error", wantErr)
		} else if (err != nil) && (err.Error() != wantErr) {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}

		// too many quoted x-migrations-table parameters
		wantErr = "\"\"migrate\".\"schema_migrations\".\"toomany\"\" MigrationsTable contains too many dot characters"
		d, err = p.Open(pgConnectionString(ip, port, `x-migrations-table="migrate"."schema_migrations"."toomany"`, "x-migrations-table-quoted=1"))
		if err == nil {
			t.Fatalf("expected '%s' but got no error", wantErr)
		} else if (err != nil) && (err.Error() != wantErr) {
			t.Fatalf("expected '%s' but got '%s'", wantErr, err.Error())
		}

		// good quoted x-migrations-table parameter
		d, err = p.Open(pgConnectionString(ip, port, `x-migrations-table="migrate"."schema_migrations"`, "x-migrations-table-quoted=1"))
		if err != nil {
			t.Fatal(err)
		}

		// make sure migrate.schema_migrations table exists
		var exists bool
		if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'schema_migrations' AND table_schema = 'migrate')").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table migrate.schema_migrations to exist")
		}

		// Sneaky confusing test - this creates a table called `migrate.schema_migrations`
		// in the public schema.  It does *not* create a `schema_migrations` table in the
		// migrate schema.
		//
		// The `migrate` schema and the "migrate"."schema_migrations" table within that
		// schema will currently exist within the test env because they were created
		// by prior tests.
		//
		// The current_schema() will typically return as "public".
		d, err = p.Open(pgConnectionString(ip, port, "x-migrations-table=migrate.schema_migrations"))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'migrate.schema_migrations' AND table_schema = (SELECT current_schema()))").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("expected table 'migrate.schema_migrations' to exist")
		}
	})
}

func TestMigrationHistoryTableOption(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		a, b, cerr := c.Port(5432)
		fmt.Printf("c.Port: %v, %v, %+v\n\n", a, b, cerr)

		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		// ip, port, err := FirstLocalIPv4Port(c, 5432)
		// if err != nil {
		// 	t.Fatal(err)
		// }

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// create migrate schema
		if err := d.Run(strings.NewReader("CREATE SCHEMA migrate AUTHORIZATION postgres")); err != nil {
			t.Fatal(err)
		}

		baseConnStr := pgConnectionString(ip, port, "x-migrations-history-enabled=true")
		for _, tc := range []struct {
			name    string
			connStr string
			wantErr string
		}{
			{
				"unquoted history table throws err",
				baseConnStr + `&x-migrations-table="migrate"."schema_migrations"&x-migrations-history-table=migrate.schema_migrations_history&x-migrations-table-quoted=1`,
				"x-migrations-history-table must be quoted (for instance '\"migrate\".\"schema_migrations_history\"') when x-migrations-table-quoted is enabled, current value is: migrate.schema_migrations_history",
			},
			{
				"only one dot separator in quoted table name",
				baseConnStr + `&x-migrations-table="migrate"."schema_migrations"&x-migrations-history-table="migrate"."toomany"."schema_migrations_history"&x-migrations-table-quoted=1`,
				`""migrate"."toomany"."schema_migrations_history"" MigrationsHistoryTable contains too many dot characters`,
			},
			{
				"valid quoted connstr no err",
				baseConnStr + `&x-migrations-table="migrate"."schema_migrations"&x-migrations-history-table="migrate"."schema_migrations_history"&x-migrations-table-quoted=1`,
				"",
			},
			{
				"valid unquoted connstr no err",
				baseConnStr + `&x-migrations-table=schema_migrations&x-migrations-history-table=schema_migrations_history&x-migrations-table-quoted=0`,
				"",
			},
			{
				"history table belongs to same schema as migrations table",
				baseConnStr + `&x-migrations-table="schema1"."whatever"&x-migrations-history-table="schema2"."whatever"&x-migrations-table-quoted=true`,
				"MigrationsHistoryTable must belong to the same schema as MigrationsTable",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := p.Open(tc.connStr)

				if tc.wantErr != "" && err == nil {
					t.Fatalf("expected err but got no err")
				} else if tc.wantErr != "" && err.Error() != tc.wantErr {
					t.Fatalf("expected err '%s' but got '%s'", tc.wantErr, err.Error())
				} else if tc.wantErr == "" && err != nil {
					t.Fatalf("unexpected err '%s'", err.Error())
				}
			})
		}

		t.Run("history table exists in migrate schema", func(t *testing.T) {
			var exists bool
			if err := d.(*Postgres).conn.QueryRowContext(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'schema_migrations_history' AND table_schema = 'migrate')").Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Fatalf("expected table migrate.schema_migrations_history to exist")
			}
		})
	})
}

func TestMigrationTableTriggers(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		baseConnStr := fmt.Sprintf("postgres://postgres:%s@[%v]:%v/postgres?sslmode=disable&x-migrations-history-enabled=true", pgPassword, ip, port)
		m, _ := migrate.New("file://examples/migrations/", baseConnStr)
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		pint := func(i int) *int {
			return &i
		}

		// Expect 9 files to exist in examples/migrations
		migrationsCount := 9
		t.Run("test migrate up non-manual history tracking", func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				query        string
				expectNumber *int
			}{
				{
					"has expected number of new dirty migrations that are pending validation",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and new_dirty = true",
					&migrationsCount,
				},
				{
					"has expected number of new non-dirty migrations that passed validation",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and new_dirty = false",
					&migrationsCount,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.expectNumber != nil {
						var result int
						if err := d.(*Postgres).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
							t.Fatal(err)
						} else if result != *tc.expectNumber {
							t.Fatalf("expected %d, received %d", *tc.expectNumber, result)
						}
					}
				})
			}
		})

		t.Run("test triggers from manual edits to migrations version table", func(t *testing.T) {
			for _, query := range []string{
				"update schema_migrations set dirty = false",
				"delete from schema_migrations",
				"insert into schema_migrations(version, dirty) values(12345, true)",
				"update schema_migrations set dirty = false",
				"truncate table schema_migrations",
				"insert into schema_migrations(version, dirty) values(56789, true)",
			} {
				if _, err := d.(*Postgres).conn.ExecContext(context.Background(), query); err != nil {
					t.Fatal(err)
				}
			}

			for _, tc := range []struct {
				name         string
				query        string
				expectNumber *int
			}{
				{
					"has correct count of prior app-automated migrations",
					"select count(*) as value from schema_migrations_history where manual_edit = false",
					pint(migrationsCount * 2), // true and false for dirty
				},
				{
					"has 2 update actions",
					"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'UPDATE'",
					pint(2),
				},
				{
					"has 1 NOOP update",
					"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'UPDATE' and notes = 'NOOP'",
					pint(1),
				},
				{
					"has 2 insert actions",
					"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'INSERT'",
					pint(2),
				},
				{
					"has 1 delete action",
					"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'DELETE'",
					pint(1),
				},
				{
					"has 1 truncate action",
					"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'TRUNCATE'",
					pint(1),
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.expectNumber != nil {
						var result int
						if err := d.(*Postgres).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
							t.Fatal(err)
						} else if result != *tc.expectNumber {
							t.Fatalf("expected %d, received %d", *tc.expectNumber, result)
						}
					}

					d.(*Postgres).conn.ExecContext(context.Background(), tc.query)
				})
			}
		})
	})
}

func TestMigrationDownHistoryTableReverts(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		baseConnStr := pgConnectionString(ip, port, "x-migrations-history-enabled=true")
		m, _ := migrate.New("file://examples/migrations/", baseConnStr)
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		// Expect 9 files to exist in examples/migrations
		migrationsCount := 9
		t.Run("test successful UP migration", func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				query        string
				expectNumber int
			}{
				{
					"has expected number of new dirty migrations that are pending validation",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and new_dirty = true",
					migrationsCount,
				},
				{
					"has expected number of new non-dirty migrations that passed validation",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and new_dirty = false",
					migrationsCount,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var result int
					if err := d.(*Postgres).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
						t.Fatal(err)
					} else if result != tc.expectNumber {
						t.Fatalf("expected %d, received %d", tc.expectNumber, result)
					}
				})
			}
		})

		err = m.Steps(-2)
		if err != nil {
			t.Fatal("err migrating down", err)
		}

		t.Run("test history tracking after DOWN migration * 2", func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				query        string
				expectNumber int
			}{
				{
					"has migration history for original n UP migrations",
					"select count(*) as value from schema_migrations_history where direction = 'up'",
					(2 * migrationsCount),
				},
				{
					"has 2 dirty down migration history items",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and direction = 'down' and new_dirty = true",
					2,
				},
				{
					"has 2 clean/validated down migration history items",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and direction = 'down' and new_dirty = true",
					2,
				},
				{
					"has (n - 2) validated up migrations",
					"select count(*) as value from schema_migrations_history where action = 'MIGRATE' and direction = 'up' and reverted = false and new_dirty = false",
					migrationsCount - 2,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var result int
					if err := d.(*Postgres).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
						t.Fatal(err)
					} else if result != tc.expectNumber {
						t.Fatalf("expected %d, received %d", tc.expectNumber, result)
					}
				})
			}
		})

		// Re-up migrations to reapply the 2 we rolled back
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		t.Run("test history tracking after re-upping migrations", func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				query        string
				expectNumber int
			}{
				{
					"has migration history for original UP migrations and 2 reapplied",
					"select count(*) as value from schema_migrations_history where direction = 'up'",
					(2 * migrationsCount) + (2 * 2), // should be +2 UP migrations
				},
				{
					"has (n) validated up migrations",
					"select count(*) as value from schema_migrations_history where direction = 'up' and reverted = false and new_dirty = false",
					migrationsCount,
				},
				{
					"has (2) previously validated but then reverted (due to down migration) up migrations",
					"select count(*) as value from schema_migrations_history where direction = 'up' and reverted = true and new_dirty = false",
					2,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var result int
					if err := d.(*Postgres).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
						t.Fatal(err)
					} else if result != tc.expectNumber {
						t.Fatalf("expected %d, received %d", tc.expectNumber, result)
					}
				})
			}
		})
	})
}

func TestListAppliedVersions(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		baseConnStr := pgConnectionString(ip, port, "x-migrations-history-enabled=true")
		m, _ := migrate.New("file://examples/migrations/", baseConnStr)
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		migrationCount := 9
		versions := []int{}

		t.Run("lists correct number of versions", func(t *testing.T) {
			versions, err = d.(*Postgres).ListAppliedVersions()
			if err != nil {
				t.Fatal(err)
			}
			if len(versions) != migrationCount {
				t.Fatalf("expected %d versions, received %d", migrationCount, len(versions))
			}
		})

		// Stash for later
		origVersions := versions

		t.Run("after migrate down, fewer versions are listed", func(t *testing.T) {
			if err := m.Steps(-3); err != nil {
				t.Fatal(err)
			}
			versions, err = d.(*Postgres).ListAppliedVersions()
			if err != nil {
				t.Fatal(err)
			}
			if len(versions) != migrationCount-3 {
				t.Fatalf("expected %d versions, received %d", migrationCount-3, len(versions))
			}
			if !slices.Equal(origVersions[0:migrationCount-3], versions) {
				t.Fatalf("did not receive correct versions list")
			}
		})

		t.Run("after migrate up 1 step, that versions is re-listed", func(t *testing.T) {
			if err := m.Steps(1); err != nil {
				t.Fatal(err)
			}
			versions, err = d.(*Postgres).ListAppliedVersions()
			if err != nil {
				t.Fatal(err)
			}
			if len(versions) != migrationCount-2 {
				t.Fatalf("expected %d versions, received %d", migrationCount-2, len(versions))
			}
			if !slices.Equal(origVersions[0:migrationCount-2], versions) {
				t.Fatalf("did not receive correct versions list")
			}
		})

		t.Run("after migrate completely up, all versions are listed", func(t *testing.T) {
			if err := m.Up(); err != nil {
				t.Fatal(err)
			}
			versions, err = d.(*Postgres).ListAppliedVersions()
			if err != nil {
				t.Fatal(err)
			}
			if len(versions) != migrationCount {
				t.Fatalf("expected %d versions, received %d", migrationCount-2, len(versions))
			}
			if !slices.Equal(origVersions, versions) {
				t.Fatalf("did not receive correct versions list")
			}
		})
	})
}

func TestExecStatements(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// Have to create a separate source driver to set it within the
		// test.  Normally this is done on the migrate instance and called
		// during m.Run(), but we're using disposable/throwaway sql migrations based on strings.NewReader.
		sourceDrv, err := source.Open("file://./examples/?x-migrations-path=migrations")
		if err != nil {
			t.Fatal("failed to open source", err)
		}
		d.SetSourceDriver(sourceDrv)

		ctx := context.Background()
		var count int

		for i := 0; i < 3; i++ {
			t.Run("test exec tblAssignments", func(t *testing.T) {
				// seeds file creates tblAssignments and inserts 5 rows
				// we should end up with 15 rows
				err = d.Run(strings.NewReader(`
					begin;
					exec "../seeds/tblAssignments.sql";
					commit;
				`))
				if err != nil {
					t.Fatal("error running seed", err)
				}

				if err := d.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from tblAssignments").Scan(&count); err != nil {
					t.Fatal(err)
				}

				expect := (i + 1) * 5
				if count != expect {
					t.Fatalf("expected count=%d, received count=%d", expect, count)
				}
			})
		}

		for i := 0; i < 4; i++ {
			t.Run("test multiple execs - tblAssignments and tblUsers", func(t *testing.T) {
				// seeds file creates tblAssignments and inserts 5 rows
				// we should end up with 15 rows
				err = d.Run(strings.NewReader(`
					begin;
					exec "../seeds/tblAssignments.sql";

					-- let's get crazy
					drop table if exists tblAssignments;
					exec "../seeds/tblAssignments.sql";

					exec "../seeds/tblUsers.sql";
					commit;
				`))
				if err != nil {
					t.Fatal("error running seed", err)
				}

				if err := d.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from tblAssignments").Scan(&count); err != nil {
					t.Fatal(err)
				}

				// Since we got CRAZY and dropped the table, we expect table
				// recreated with only 5 users each time
				expect := 5
				if count != expect {
					t.Fatalf("expected count=%d, received count=%d", expect, count)
				}

				if err := d.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from tblUsers").Scan(&count); err != nil {
					t.Fatal(err)
				}

				// Users table gets 3 per batch, no crazy drop table
				expect = (i + 1) * 3
				if count != expect {
					t.Fatalf("expected count=%d, received count=%d", expect, count)
				}
			})
		}
	})
}

func TestForceTransactionalMigrations(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		p := &Postgres{}
		dUnforced, err := p.Open(pgConnectionString(ip, port, "x-force-transactional-migrations=0"))
		if err != nil {
			t.Fatal(err)
		}
		dForced, err := p.Open(pgConnectionString(ip, port, "x-force-transactional-migrations=1"))
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := dUnforced.Close(); err != nil {
				t.Fatal(err)
			}
			if err := dForced.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// Have to create a separate source driver to set it within the
		// test.  Normally this is done on the migrate instance and called
		// during m.Run(), but we're using disposable/throwaway sql migrations based on strings.NewReader.
		sourceDrv, err := source.Open("file://./examples/?x-migrations-path=migrations")
		if err != nil {
			t.Fatal("failed to open source", err)
		}
		dUnforced.SetSourceDriver(sourceDrv)
		dForced.SetSourceDriver(sourceDrv)

		ctx := context.Background()
		var count int

		t.Run("test forced transactions disabled", func(t *testing.T) {
			// seeds file creates tblAssignments and inserts 5 rows
			// we should end up with 15 rows
			err = dUnforced.Run(strings.NewReader(`
				create table random_throwaway_table(x int);
				insert into random_throwaway_table(x) values (1), (2), (3);

				-- create intentionalsqlerror lol bye;
				exec "../seeds/invalid_sql.sql";
			`))
			if err == nil {
				t.Fatal("expected error on intentionally bad sql")
			}

			// Did not run as a forced or explicit transaction, so we expect
			// random_throwaway_table to exist
			if err := dUnforced.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_name = 'random_throwaway_table'").Scan(&count); err != nil {
				t.Fatal(err)
			} else if count != 1 {
				t.Fatal("expected random_throwaway_table to exist")
			}

			if err := dUnforced.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from random_throwaway_table").Scan(&count); err != nil {
				t.Fatal(err)
			} else if count != 3 {
				t.Fatalf("expected 3 rows in random_throwaway_table; received %d", count)
			}

			err = dUnforced.Run(strings.NewReader("drop table if exists random_throwaway_table"))
			if err != nil {
				t.Fatal("error dropping table", err)
			}
		})

		t.Run("test forced transactions enabled", func(t *testing.T) {
			// seeds file creates tblAssignments and inserts 5 rows
			// we should end up with 15 rows
			err = dForced.Run(strings.NewReader(`
				create table random_throwaway_table(x int);
				insert into random_throwaway_table(x) values (1), (2), (3);

				-- create intentionalsqlerror lol bye;
				exec "../seeds/invalid_sql.sql";
			`))
			if err == nil {
				t.Fatal("expected error on intentionally bad sql")
			}

			// *DID* run as a forced transaction, so we expect
			// random_throwaway_table not to exist because it got rolled back
			if err := dForced.(*Postgres).conn.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_name = 'random_throwaway_table'").Scan(&count); err != nil {
				t.Fatal(err)
			} else if count != 0 {
				t.Fatal("did not expect random_throwaway_table to exist")
			}
		})
	})
}

func TestFailToCreateTableWithoutPermissions(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)

		// Check that opening the postgres connection returns NilVersion
		p := &Postgres{}

		d, err := p.Open(addr)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create user who is not the owner. Although we're concatenating strings in an sql statement it should be fine
		// since this is a test environment and we're not expecting to the pgPassword to be malicious
		mustRun(t, d, []string{
			"CREATE USER not_owner WITH ENCRYPTED PASSWORD '" + pgPassword + "'",
			"CREATE SCHEMA barfoo AUTHORIZATION postgres",
			"GRANT USAGE ON SCHEMA barfoo TO not_owner",
			"REVOKE CREATE ON SCHEMA barfoo FROM PUBLIC",
			"REVOKE CREATE ON SCHEMA barfoo FROM not_owner",
		})

		// re-connect using that schema
		connStr := pgConnectionString(ip, port, "search_path=barfoo")
		d2, err := p.Open(connStr)

		defer func() {
			if d2 == nil {
				return
			}
			if err := d2.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		var e *database.Error
		if !errors.As(err, &e) || err == nil {
			t.Fatal("Unexpected error, want permission denied error. Got: ", err)
		}

		if !strings.Contains(e.OrigErr.Error(), "permission denied for schema barfoo") {
			t.Fatal(e)
		}

		// re-connect using that x-migrations-table and x-migrations-table-quoted
		connStr = pgConnectionString(ip, port, `x-migrations-table="barfoot"."schema_migrations"`, "x-migrations-table-quoted=1")
		d2, err = p.Open(connStr)

		if !errors.As(err, &e) || err == nil {
			t.Fatal("Unexpected error, want permission denied error. Got: ", err)
		}

		if !strings.Contains(e.OrigErr.Error(), "permission denied for schema barfoo") {
			t.Fatal(e)
		}
	})
}

func TestCheckBeforeCreateTable(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)

		// Check that opening the postgres connection returns NilVersion
		p := &Postgres{}

		d, err := p.Open(addr)

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create user who is not the owner. Although we're concatenating strings in an sql statement it should be fine
		// since this is a test environment and we're not expecting to the pgPassword to be malicious
		mustRun(t, d, []string{
			"CREATE USER not_owner WITH ENCRYPTED PASSWORD '" + pgPassword + "'",
			"CREATE SCHEMA barfoo AUTHORIZATION postgres",
			"GRANT USAGE ON SCHEMA barfoo TO not_owner",
			"GRANT CREATE ON SCHEMA barfoo TO not_owner",
		})

		// re-connect using that schema
		connStr := pgConnectionString(ip, port, "search_path=barfoo")
		d2, err := p.Open(connStr)

		if err != nil {
			t.Fatal(err)
		}

		if err := d2.Close(); err != nil {
			t.Fatal(err)
		}

		// revoke privileges
		mustRun(t, d, []string{
			"REVOKE CREATE ON SCHEMA barfoo FROM PUBLIC",
			"REVOKE CREATE ON SCHEMA barfoo FROM not_owner",
		})

		// re-connect using that schema
		// Don't use pgConnectionString because we're using an intentionally
		// incorrect username!
		d3, err := p.Open(fmt.Sprintf("postgres://not_owner:%s@[%v]:%v/postgres?sslmode=disable&search_path=barfoo",
			pgPassword, ip, port))

		if err != nil {
			t.Fatal(err)
		}

		version, _, err := d3.Version()

		if err != nil {
			t.Fatal(err)
		}

		if version != database.NilVersion {
			t.Fatal("Unexpected version, want database.NilVersion. Got: ", version)
		}

		defer func() {
			if err := d3.Close(); err != nil {
				t.Fatal(err)
			}
		}()
	})
}

func TestParallelSchema(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		// create foo and bar schemas
		if err := d.Run(strings.NewReader("CREATE SCHEMA foo AUTHORIZATION postgres")); err != nil {
			t.Fatal(err)
		}
		if err := d.Run(strings.NewReader("CREATE SCHEMA bar AUTHORIZATION postgres")); err != nil {
			t.Fatal(err)
		}

		// re-connect using that schemas
		dfoo, err := p.Open(pgConnectionString(ip, port, "search_path=foo"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := dfoo.Close(); err != nil {
				t.Error(err)
			}
		}()

		dbar, err := p.Open(pgConnectionString(ip, port, "search_path=bar"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := dbar.Close(); err != nil {
				t.Error(err)
			}
		}()

		if err := dfoo.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Lock(); err != nil {
			t.Fatal(err)
		}

		if err := dbar.Unlock(); err != nil {
			t.Fatal(err)
		}

		if err := dfoo.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPostgresLock(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		addr := pgConnectionString(ip, port)
		p := &Postgres{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		dt.Test(t, d, []byte("SELECT 1"))

		ps := d.(*Postgres)

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Lock()
		if err != nil {
			t.Fatal(err)
		}

		err = ps.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestWithInstanceConcurrent(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		// The number of concurrent processes running WithInstance
		const concurrency = 30

		// We can instantiate a single database handle because it is
		// actually a connection pool, and so, each of the below go
		// routines will have a high probability of using a separate
		// connection, which is something we want to exercise.
		db, err := sql.Open("postgres", pgConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		db.SetMaxIdleConns(concurrency)
		db.SetMaxOpenConns(concurrency)

		var wg sync.WaitGroup
		defer wg.Wait()

		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				defer wg.Done()
				_, err := WithInstance(db, &Config{})
				if err != nil {
					t.Errorf("process %d error: %s", i, err)
				}
			}(i)
		}
	})
}

func TestWithConnection(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}

		db, err := sql.Open("postgres", pgConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}()

		ctx := context.Background()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}

		p, err := WithConnection(ctx, conn, &Config{})
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := p.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, p, []byte("SELECT 1"))
	})
}

func Test_computeLineFromPos(t *testing.T) {
	testcases := []struct {
		pos      int
		wantLine uint
		wantCol  uint
		input    string
		wantOk   bool
	}{
		{
			15, 2, 6, "SELECT *\nFROM foo", true, // foo table does not exists
		},
		{
			16, 3, 6, "SELECT *\n\nFROM foo", true, // foo table does not exists, empty line
		},
		{
			25, 3, 7, "SELECT *\nFROM foo\nWHERE x", true, // x column error
		},
		{
			27, 5, 7, "SELECT *\n\nFROM foo\n\nWHERE x", true, // x column error, empty lines
		},
		{
			10, 2, 1, "SELECT *\nFROMM foo", true, // FROMM typo
		},
		{
			11, 3, 1, "SELECT *\n\nFROMM foo", true, // FROMM typo, empty line
		},
		{
			17, 2, 8, "SELECT *\nFROM foo", true, // last character
		},
		{
			18, 0, 0, "SELECT *\nFROM foo", false, // invalid position
		},
	}
	for i, tc := range testcases {
		t.Run("tc"+strconv.Itoa(i), func(t *testing.T) {
			run := func(crlf bool, nonASCII bool) {
				var name string
				if crlf {
					name = "crlf"
				} else {
					name = "lf"
				}
				if nonASCII {
					name += "-nonascii"
				} else {
					name += "-ascii"
				}
				t.Run(name, func(t *testing.T) {
					input := tc.input
					if crlf {
						input = strings.Replace(input, "\n", "\r\n", -1)
					}
					if nonASCII {
						input = strings.Replace(input, "FROM", "FRÖM", -1)
					}
					gotLine, gotCol, gotOK := computeLineFromPos(input, tc.pos)

					if tc.wantOk {
						t.Logf("pos %d, want %d:%d, %#v", tc.pos, tc.wantLine, tc.wantCol, input)
					}

					if gotOK != tc.wantOk {
						t.Fatalf("expected ok %v but got %v", tc.wantOk, gotOK)
					}
					if gotLine != tc.wantLine {
						t.Fatalf("expected line %d but got %d", tc.wantLine, gotLine)
					}
					if gotCol != tc.wantCol {
						t.Fatalf("expected col %d but got %d", tc.wantCol, gotCol)
					}
				})
			}
			run(false, false)
			run(true, false)
			run(false, true)
			run(true, true)
		})
	}
}
