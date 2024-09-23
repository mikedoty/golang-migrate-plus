package mysql

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dhui/dktest"
	"github.com/go-sql-driver/mysql"
	"github.com/mikedoty/golang-migrate-plus"
	dt "github.com/mikedoty/golang-migrate-plus/database/testing"
	"github.com/mikedoty/golang-migrate-plus/dktesting"
	"github.com/mikedoty/golang-migrate-plus/source"
	_ "github.com/mikedoty/golang-migrate-plus/source/file"
	"github.com/stretchr/testify/assert"
)

const defaultPort = 3306

var (
	opts = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
	}
	optsAnsiQuotes = dktest.Options{
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "root", "MYSQL_DATABASE": "public"},
		PortRequired: true, ReadyFunc: isReady,
		Cmd: []string{"--sql-mode=ANSI_QUOTES"},
	}
	// Supported versions: https://www.mysql.com/support/supportedplatforms/database.html
	specs = []dktesting.ContainerSpec{
		// {ImageName: "mysql:5.5", Options: opts},
		// {ImageName: "mysql:5.6", Options: opts},
		// {ImageName: "mysql:5.7", Options: opts},
		{ImageName: "mysql:8", Options: opts},
	}
	specsAnsiQuotes = []dktesting.ContainerSpec{
		// {ImageName: "mysql:5.5", Options: optsAnsiQuotes},
		// {ImageName: "mysql:5.6", Options: optsAnsiQuotes},
		// {ImageName: "mysql:5.7", Options: optsAnsiQuotes},
		// {ImageName: "mysql:8", Options: optsAnsiQuotes},
	}
)

func mysqlConnectionString(host, port string, options ...string) string {
	baseConnStr := fmt.Sprintf("mysql://root:root@tcp([%v]:%v)/public", host, port)
	if len(options) == 0 {
		return baseConnStr
	}

	connStr := fmt.Sprintf("%s?%s", baseConnStr, strings.Join(options, "&"))
	return connStr
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	// Don't use mysqlConnectionString helper here because we're connecting
	// directly via sql.Open, so we don't want the mysql:// driver prefix
	// Just make sure to wrap the hostname in [] to support ipv6
	db, err := sql.Open("mysql", fmt.Sprintf("root:root@tcp([%v]:%v)/public", ip, port))
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
		case sqldriver.ErrBadConn, mysql.ErrInvalidConn:
			return false
		default:
			fmt.Println(err)
		}
		return false
	}

	return true
}

func Test(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
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

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrate(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMigrateWithHistory(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port, "x-migrations-history-enabled=1")
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		// check ensureVersionTable, because dt.TestMigrate
		// drops everything after it's done so we need to recreate it
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}

		// check ensureHistoryTable
		if err := d.(*Mysql).ensureHistoryTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureHistoryTable(); err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		t.Run("history table exists", func(t *testing.T) {
			var result string
			query := `SHOW TABLES LIKE 'schema_migrations_history'`
			if err := d.(*Mysql).conn.QueryRowContext(ctx, query).Scan(&result); err != nil {
				t.Fatal("cannot confirm history table exists", err)
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

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		m, _ := migrate.New(
			"file://./examples/additional_migrations/",
			mysqlConnectionString(ip, port, "x-migrations-history-enabled=true"),
		)
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		pint := func(i int) *int {
			return &i
		}

		// Expect 9 files to exist in postgres's examples/migrations
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
						if err := d.(*Mysql).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
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
				if _, err := d.(*Mysql).conn.ExecContext(context.Background(), query); err != nil {
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
				// mysql doesn't support truncate triggers
				// {
				// 	"has 1 truncate action",
				// 	"select count(*) as value from schema_migrations_history where manual_edit = true and action = 'TRUNCATE'",
				// 	pint(1),
				// },
			} {
				t.Run(tc.name, func(t *testing.T) {
					if tc.expectNumber != nil {
						var result int
						if err := d.(*Mysql).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
							t.Fatal(err)
						} else if result != *tc.expectNumber {
							t.Fatalf("expected %d, received %d", *tc.expectNumber, result)
						}
					}

					d.(*Mysql).conn.ExecContext(context.Background(), tc.query)
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

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		// baseConnStr := fmt.Sprintf("%s?x-migrations-history-enabled=true&x-xmigrations-table=a&x-xmigrations-history-table=b", addr)
		m, _ := migrate.New(
			"file://./examples/additional_migrations/",
			mysqlConnectionString(ip, port, "x-migrations-history-enabled=true"),
		)

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
					if err := d.(*Mysql).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
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
					if err := d.(*Mysql).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
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
					if err := d.(*Mysql).conn.QueryRowContext(context.Background(), tc.query).Scan(&result); err != nil {
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

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
		}()

		m, _ := migrate.New(
			"file://./examples/additional_migrations/",
			mysqlConnectionString(ip, port, "x-migrations-history-enabled=true"),
		)
		err = m.Up()
		if err != nil {
			t.Fatal("error migrating up", err)
		}

		migrationCount := 9
		versions := []int{}

		t.Run("lists correct number of versions", func(t *testing.T) {
			versions, err = d.(*Mysql).ListAppliedVersions()
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
			versions, err = d.(*Mysql).ListAppliedVersions()
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
			versions, err = d.(*Mysql).ListAppliedVersions()
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
			versions, err = d.(*Mysql).ListAppliedVersions()
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

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
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

				if err := d.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from tblAssignments").Scan(&count); err != nil {
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

				if err := d.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from tblAssignments").Scan(&count); err != nil {
					t.Fatal(err)
				}

				// Since we got CRAZY and dropped the table, we expect table
				// recreated with only 5 users each time
				expect := 5
				if count != expect {
					t.Fatalf("expected count=%d, received count=%d", expect, count)
				}

				if err := d.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from tblUsers").Scan(&count); err != nil {
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

		p := &Mysql{}
		dUnforced, err := p.Open(mysqlConnectionString(ip, port, "x-force-transactional-migrations=0"))
		if err != nil {
			t.Fatal(err)
		}
		dForced, err := p.Open(mysqlConnectionString(ip, port, "x-force-transactional-migrations=true"))
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
				-- create table will persist despite the rollback
				--
				-- Have to use temporary tables to avoid implicitly
				-- committing the active transaction
				create temporary table random_throwaway_table(x int);

				-- this insert table will also persist because we didn't
				-- enable forced transaction mode
				insert into random_throwaway_table(x) values (1), (2), (30);

				-- create intentionalsqlerror lol bye;
				exec "../seeds/invalid_sql.sql";
			`))
			if err == nil {
				t.Fatal("expected error on intentionally bad sql")
			}

			// Did not run as a forced or explicit transaction, so we expect
			// random_throwaway_table to exist
			if err := dUnforced.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from random_throwaway_table").Scan(&count); err != nil {
				if err != sql.ErrNoRows {
					t.Fatal("expected random_throwaway_table to exist", err)
				}
			}

			if err := dUnforced.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from random_throwaway_table").Scan(&count); err != nil {
				t.Fatal(err)
			} else if count != 3 {
				t.Fatalf("expected 3 rows in random_throwaway_table; received %d", count)
			}

			err = dUnforced.Run(strings.NewReader("drop temporary table if exists random_throwaway_table"))
			if err != nil {
				t.Fatal("error dropping table", err)
			}
		})

		t.Run("test forced transactions enabled", func(t *testing.T) {
			// seeds file creates tblAssignments and inserts 5 rows
			// we should end up with 15 rows
			err = dForced.Run(strings.NewReader(`
				-- create table will persist despite the rollback
				--
				-- Have to use temporary tables to avoid implicitly
				-- committing the active transaction
				create temporary table random_throwaway_table2(x int);

				-- this insert table will NOT persist
				insert into random_throwaway_table2(x) values (1), (2), (3), (4), (5), (6), (7);

				-- create intentionalsqlerror lol bye;
				exec "../seeds/invalid_sql.sql";
			`))
			if err == nil {
				t.Fatal("expected error on intentionally bad sql")
			}

			// *DID* run as a forced transaction.
			//
			// Interesting!  Mysql forces commit on create table.  We can test
			// using temporary tables instead.  However - per the docs,
			// after rolling back the transaction, the temporary table
			// will still exist!
			//
			// Docs:  However, although no implicit commit occurs, neither can the statement be rolled back, which means that the use of such statements causes transactional atomicity to be violated. For example, if you use CREATE TEMPORARY TABLE and then roll back the transaction, the table remains in existence.
			//
			// Thus, our best bet to check the rollback worked is not to check
			// existence of temp table, but merely to confirm that the INSERT got
			// rolled back and we have 0 rows...
			if err := dForced.(*Mysql).conn.QueryRowContext(ctx, "select count(*) from random_throwaway_table2").Scan(&count); err != nil {
				t.Fatal("temporary random_throwaway_table2 should still exist in mysql after rollback - see docs", err)
			} else if err == nil {
				if count != 0 {
					t.Fatalf("expected insert on random_throwaway_table2 to be rollback back (count = %d)", count)
				}
			}
		})
	})
}

func TestMigrateAnsiQuotes(t *testing.T) {
	// mysql.SetLogger(mysql.Logger(log.New(io.Discard, "", log.Ltime)))

	dktesting.ParallelTest(t, specsAnsiQuotes, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "public", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)

		// check ensureVersionTable
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
		// check again
		if err := d.(*Mysql).ensureVersionTable(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		dt.Test(t, d, []byte("SELECT 1"))

		ms := d.(*Mysql)

		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}

		// make sure the 2nd lock works (RELEASE_LOCK is very finicky)
		err = ms.Lock()
		if err != nil {
			t.Fatal(err)
		}
		err = ms.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestNoLockParamValidation(t *testing.T) {
	ip := "127.0.0.1"
	port := "3306"
	addr := mysqlConnectionString(ip, port)
	p := &Mysql{}
	_, err := p.Open(addr + "?x-no-lock=not-a-bool")
	if !errors.Is(err, strconv.ErrSyntax) {
		t.Fatal("Expected syntax error when passing a non-bool as x-no-lock parameter")
	}
}

func TestNoLockWorks(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := mysqlConnectionString(ip, port)
		p := &Mysql{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}

		lock := d.(*Mysql)

		p = &Mysql{}
		d, err = p.Open(addr + "?x-no-lock=true")
		if err != nil {
			t.Fatal(err)
		}

		noLock := d.(*Mysql)

		// Should be possible to take real lock and no-lock at the same time
		if err = lock.Lock(); err != nil {
			t.Fatal(err)
		}
		if err = noLock.Lock(); err != nil {
			t.Fatal(err)
		}
		if err = lock.Unlock(); err != nil {
			t.Fatal(err)
		}
		if err = noLock.Unlock(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestExtractCustomQueryParams(t *testing.T) {
	testcases := []struct {
		name                 string
		config               *mysql.Config
		expectedParams       map[string]string
		expectedCustomParams map[string]string
		expectedErr          error
	}{
		{name: "nil config", expectedErr: ErrNilConfig},
		{
			name:                 "no params",
			config:               mysql.NewConfig(),
			expectedCustomParams: map[string]string{},
		},
		{
			name:                 "no custom params",
			config:               &mysql.Config{Params: map[string]string{"hello": "world"}},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{},
		},
		{
			name: "one param, one custom param",
			config: &mysql.Config{
				Params: map[string]string{"hello": "world", "x-foo": "bar"},
			},
			expectedParams:       map[string]string{"hello": "world"},
			expectedCustomParams: map[string]string{"x-foo": "bar"},
		},
		{
			name: "multiple params, multiple custom params",
			config: &mysql.Config{
				Params: map[string]string{
					"hello": "world",
					"x-foo": "bar",
					"dead":  "beef",
					"x-cat": "hat",
				},
			},
			expectedParams:       map[string]string{"hello": "world", "dead": "beef"},
			expectedCustomParams: map[string]string{"x-foo": "bar", "x-cat": "hat"},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			customParams, err := extractCustomQueryParams(tc.config)
			if tc.config != nil {
				assert.Equal(t, tc.expectedParams, tc.config.Params,
					"Expected config params have custom params properly removed")
			}
			assert.Equal(t, tc.expectedErr, err, "Expected errors to match")
			assert.Equal(t, tc.expectedCustomParams, customParams,
				"Expected custom params to be properly extracted")
		})
	}
}

func createTmpCert(t *testing.T) string {
	tmpCertFile, err := os.CreateTemp("", "migrate_test_cert")
	if err != nil {
		t.Fatal("Failed to create temp cert file:", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(tmpCertFile.Name()); err != nil {
			t.Log("Failed to cleanup temp cert file:", err)
		}
	})

	r := rand.New(rand.NewSource(0))
	pub, priv, err := ed25519.GenerateKey(r)
	if err != nil {
		t.Fatal("Failed to generate ed25519 key for temp cert file:", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(0),
	}
	derBytes, err := x509.CreateCertificate(r, &tmpl, &tmpl, pub, priv)
	if err != nil {
		t.Fatal("Failed to generate temp cert file:", err)
	}
	if err := pem.Encode(tmpCertFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatal("Failed to encode ")
	}
	if err := tmpCertFile.Close(); err != nil {
		t.Fatal("Failed to close temp cert file:", err)
	}
	return tmpCertFile.Name()
}

func TestURLToMySQLConfig(t *testing.T) {
	tmpCertFilename := createTmpCert(t)
	tmpCertFilenameEscaped := url.PathEscape(tmpCertFilename)

	testcases := []struct {
		name        string
		urlStr      string
		expectedDSN string // empty string signifies that an error is expected
	}{
		{name: "no user/password", urlStr: "mysql://tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user", urlStr: "mysql://username@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded :",
			urlStr:      "mysql://username%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "only user - with encoded @",
			urlStr:      "mysql://username%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password", urlStr: "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		// Not supported yet: https://github.com/go-sql-driver/mysql/issues/591
		// {name: "user/password - user with encoded :",
		// 	urlStr:      "mysql://username%3A:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
		// 	expectedDSN: "username::password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - user with encoded @",
			urlStr:      "mysql://username%40:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username@:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded :",
			urlStr:      "mysql://username:password%3A@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password:@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "user/password - password with encoded @",
			urlStr:      "mysql://username:password%40@tcp(127.0.0.1:3306)/myDB?multiStatements=true",
			expectedDSN: "username:password@@tcp(127.0.0.1:3306)/myDB?multiStatements=true"},
		{name: "custom tls",
			urlStr:      "mysql://username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped,
			expectedDSN: "username:password@tcp(127.0.0.1:3306)/myDB?multiStatements=true&tls=custom&x-tls-ca=" + tmpCertFilenameEscaped},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := urlToMySQLConfig(tc.urlStr)
			if err != nil {
				t.Fatal("Failed to parse url string:", tc.urlStr, "error:", err)
			}
			dsn := config.FormatDSN()
			if dsn != tc.expectedDSN {
				t.Error("Got unexpected DSN:", dsn, "!=", tc.expectedDSN)
			}
		})
	}
}
