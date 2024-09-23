package sourcing

import (
	"embed"
	"fmt"
	"log"
	"testing"

	"github.com/mikedoty/golang-migrate-plus/source/iofs"
)

//go:embed testdata/migrations
var fs embed.FS

func TestImportSourcing(t *testing.T) {
	drv, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		log.Fatal(err)
	}

	for _, tc := range []struct {
		name         string
		migrationSql string
		expectedSql  string
		wantError    bool
	}{
		{
			"replace with good syntax and double quotes",
			"CREATE TABLE table1;\nsource \"./views/view1.sql\";\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with good syntax and single quotes",
			"CREATE TABLE table1;\nsource './views/view1.sql';\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with some leading whitespace",
			"CREATE TABLE table1;\n     source \"./views/view1.sql\";\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with trailing whitespace",
			"CREATE TABLE table1;\nsource \"./views/view1.sql\";    \n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with gratuitous whitespace before semicolon",
			"CREATE TABLE table1;\nsource \"./views/view1.sql\"    ;\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with missing semicolon",
			"CREATE TABLE table1;\nsource \"./views/view1.sql\"   \n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"replace with unnecessary whitespace everywhere",
			"CREATE TABLE table1;\n\t  source  \t   \"./views/view1.sql\"\t\t\t   \t;\t   \t\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			false,
		},
		{
			"error if file doesn't exist",
			"CREATE TABLE table1;\nsource \"./views/DOES_NOT_EXIST.sql\";\n-- eof",
			"CREATE TABLE table1;\nCREATE VIEW view1;\n-- eof",
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytes, err := ImportSourcing(drv, []byte(tc.migrationSql))

			if tc.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if string(bytes) != tc.expectedSql {
					t.Fatalf(`Expected "%s" but received "%s"`, tc.expectedSql, string(bytes))
				}
			}
		})
	}
}

func TestGatherExec(t *testing.T) {
	drv, err := iofs.New(fs, "testdata/migrations")
	if err != nil {
		log.Fatal(err)
	}

	for _, tc := range []struct {
		name                  string
		migrationSql          string
		expectedSqls          []string
		expectedInTransaction bool
		wantError             bool
	}{
		{
			"replace with good syntax and double quotes",
			"CREATE TABLE table1;\nexec \"./procedures/proc1.sql\";\n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"replace with good syntax and single quotes",
			"CREATE TABLE table1;\nexec './procedures/proc1.sql';\n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"replace with leading whitespace",
			"   CREATE TABLE table1;\n\n    \n   exec './procedures/proc1.sql';\n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"replace with trailing whitespace",
			"CREATE TABLE table1;\nexec './procedures/proc1.sql';      \n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"replace with gratuitous whitespace before semicolon",
			"CREATE TABLE table1;\nexec './procedures/proc1.sql'  \t \t   ;\n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"replace with missing semicolon",
			"CREATE TABLE table1;\nexec './procedures/proc1.sql'\n-- eof",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"-- eof",
			},
			false,
			false,
		},
		{
			"error if file doesn't exist",
			"CREATE TABLE table1;\nexec './procedures/NOT_EXISTS_FILE.sql';\n-- eof",
			[]string{},
			false,
			true,
		},
		{
			"run in transaction flag is true",
			"BEGIN;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\nCOMMIT;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
			},
			true,
			false,
		},
		{
			"run in transaction flag is true when lower case",
			"begin;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\ncommit;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
			},
			true,
			false,
		},
		{
			"run in transaction flag is true when capital first cased",
			"Begin;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\nCommit;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
			},
			true,
			false,
		},
		{
			"run in transaction flag is true when cRaZY CasEd",
			"BeGIn;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\ncOmmIt;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
			},
			true,
			false,
		},
		{
			"will handle multiple execs with mixed quotes and a missing semicolon",
			"BEGIN;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\nexec \"./procedures/proc2.sql\"\nCOMMIT;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"CREATE PROCEDURE proc2;",
			},
			true,
			false,
		},
		{
			"will handle multiple execs with sql in the middles and a missing semicolon",
			"BEGIN;\nCREATE TABLE table1;\nexec './procedures/proc1.sql';\nSELECT 5;\nSELECT 10;\nexec \"./procedures/proc2.sql\"\nSELECT 20;\nCOMMIT;",
			[]string{
				"CREATE TABLE table1;",
				"CREATE PROCEDURE proc1;",
				"SELECT 5;\nSELECT 10;",
				"CREATE PROCEDURE proc2;",
				"SELECT 20;",
			},
			true,
			false,
		},
		{
			"will handle multiple execs with only trailing sql",
			"BEGIN;\nexec './procedures/proc1.sql';\n\n\n\n\n\n\n\n\n\n\n\nexec \"./procedures/proc2.sql\"\nSELECT 20;\nCOMMIT;",
			[]string{
				"CREATE PROCEDURE proc1;",
				"CREATE PROCEDURE proc2;",
				"SELECT 20;",
			},
			true,
			false,
		},
		{
			"will handle multiple execs with only leading sql",
			"BEGIN;SELECT 0 -- no trailing semicolon\n\n\n\n\n      \n\n\nexec './procedures/proc1.sql';\n\n\n\n\n\n\n\n\n\n\n\nexec \"./procedures/proc2.sql\"\nCOMMIT;",
			[]string{
				"SELECT 0 -- no trailing semicolon",
				"CREATE PROCEDURE proc1;",
				"CREATE PROCEDURE proc2;",
			},
			true,
			false,
		},
		{
			"returns 0 sql expressions for an empty transaction",
			"BEGIN;\nCOMMIT;",
			[]string{},
			true,
			false,
		},
		{
			"returns 0 sql expressions for an empty transaction with random whitespace",
			"BEGIN;\n\n\n      \n\n\n    \nCOMMIT;",
			[]string{},
			true,
			false,
		},
		{
			"returns 0 sql expressions for a non-transaction with only whitespace",
			"   \n   \t\t   \t  \n\n   \n     \n\n  \t     \t\t\t  \n",
			[]string{},
			false,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bytesList, runInTransaction, err := GatherExecs(drv, []byte(tc.migrationSql))

			if tc.wantError {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if len(bytesList) != len(tc.expectedSqls) {
					for asdf := range bytesList {
						fmt.Printf("%+v\n", string(bytesList[asdf]))
					}
					t.Fatalf(`Expected %d sql byte arrays but received %d`, len(tc.expectedSqls), len(bytesList))
				}

				if tc.expectedInTransaction != runInTransaction {
					t.Fatalf(`Expected "%v" but received "%v" for runIntransaction`, tc.expectedInTransaction, runInTransaction)
				}

				for i := 0; i < len(tc.expectedSqls); i++ {
					if tc.expectedSqls[i] != string(bytesList[i]) {
						t.Fatalf(`Expected "%s" but received "%s"`, tc.expectedSqls[i], string(bytesList[i]))
					}
				}
			}
		})
	}
}
