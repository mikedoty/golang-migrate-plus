//go:build go1.9
// +build go1.9

package singlestore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/sourcing"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/hashicorp/go-multierror"
)

var _ database.Driver = (*Singlestore)(nil) // explicit compile time type check

func init() {
	database.Register("singlestore", &Singlestore{})
}

func ptr[T comparable](t T) *T {
	return &t
}

var (
	DefaultMigrationsTable = "schema_migrations"
	DefaultHistoryTable    = "schema_migrations_history"
)

var (
	ErrDatabaseDirty    = fmt.Errorf("database is dirty")
	ErrNilConfig        = fmt.Errorf("no config")
	ErrNoDatabaseName   = fmt.Errorf("no database name")
	ErrAppendPEM        = fmt.Errorf("failed to append PEM")
	ErrTLSCertKeyConfig = fmt.Errorf("To use TLS client authentication, both x-tls-cert and x-tls-key must not be empty")
)

type Config struct {
	MigrationsTable              string
	MigrationsHistoryTable       string
	MigrationsHistoryEnabled     bool
	DatabaseName                 string
	NoLock                       bool
	StatementTimeout             time.Duration
	ForceTransactionalMigrations bool
}

type Singlestore struct {
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	config *Config

	sourceDrv source.Driver
}

// connection instance must have `multiStatements` set to true
func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Singlestore, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	mx := &Singlestore{
		conn:   conn,
		db:     nil,
		config: config,
	}

	if config.DatabaseName == "" {
		query := `SELECT DATABASE()`
		var databaseName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName.String) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName.String
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}
	if len(config.MigrationsHistoryTable) == 0 {
		config.MigrationsHistoryTable = DefaultHistoryTable
	}

	if err := mx.ensureVersionTable(); err != nil {
		return nil, err
	}
	if config.MigrationsHistoryEnabled {
		if err := mx.ensureHistoryTable(); err != nil {
			return nil, err
		}
	}

	return mx, nil
}

// instance must have `multiStatements` set to true
func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	mx, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}

	mx.db = instance

	return mx, nil
}

// extractCustomQueryParams extracts the custom query params (ones that start with "x-") from
// singlestore.Config.Params (connection parameters) as to not interfere with connecting to Singlestore
func extractCustomQueryParams(c *mysql.Config) (map[string]string, error) {
	if c == nil {
		return nil, ErrNilConfig
	}
	customQueryParams := map[string]string{}

	for k, v := range c.Params {
		if strings.HasPrefix(k, "x-") {
			customQueryParams[k] = v
			delete(c.Params, k)
		}
	}
	return customQueryParams, nil
}

func urlToMySQLConfig(url string) (*mysql.Config, error) {
	// Need to parse out custom TLS parameters and call
	// mysql.RegisterTLSConfig() before mysql.ParseDSN() is called
	// which consumes the registered tls.Config
	// Fixes: https://github.com/golang-migrate/migrate/issues/411
	//
	// Can't use url.Parse() since it fails to parse MySQL DSNs
	// mysql.ParseDSN() also searches for "?" to find query parameters:
	// https://github.com/go-sql-driver/mysql/blob/46351a8/dsn.go#L344
	if idx := strings.LastIndex(url, "?"); idx > 0 {
		rawParams := url[idx+1:]
		parsedParams, err := nurl.ParseQuery(rawParams)
		if err != nil {
			return nil, err
		}

		ctls := parsedParams.Get("tls")
		if len(ctls) > 0 {
			if _, isBool := readBool(ctls); !isBool && strings.ToLower(ctls) != "skip-verify" {
				rootCertPool := x509.NewCertPool()
				pem, err := os.ReadFile(parsedParams.Get("x-tls-ca"))
				if err != nil {
					return nil, err
				}

				if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
					return nil, ErrAppendPEM
				}

				clientCert := make([]tls.Certificate, 0, 1)
				if ccert, ckey := parsedParams.Get("x-tls-cert"), parsedParams.Get("x-tls-key"); ccert != "" || ckey != "" {
					if ccert == "" || ckey == "" {
						return nil, ErrTLSCertKeyConfig
					}
					certs, err := tls.LoadX509KeyPair(ccert, ckey)
					if err != nil {
						return nil, err
					}
					clientCert = append(clientCert, certs)
				}

				insecureSkipVerify := false
				insecureSkipVerifyStr := parsedParams.Get("x-tls-insecure-skip-verify")
				if len(insecureSkipVerifyStr) > 0 {
					x, err := strconv.ParseBool(insecureSkipVerifyStr)
					if err != nil {
						return nil, err
					}
					insecureSkipVerify = x
				}

				err = mysql.RegisterTLSConfig(ctls, &tls.Config{
					RootCAs:            rootCertPool,
					Certificates:       clientCert,
					InsecureSkipVerify: insecureSkipVerify,
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	config, err := mysql.ParseDSN(strings.TrimPrefix(url, "singlestore://"))
	if err != nil {
		return nil, err
	}

	config.MultiStatements = true

	// Keep backwards compatibility from when we used net/url.Parse() to parse the DSN.
	// net/url.Parse() would automatically unescape it for us.
	// See: https://play.golang.org/p/q9j1io-YICQ
	user, err := nurl.QueryUnescape(config.User)
	if err != nil {
		return nil, err
	}
	config.User = user

	password, err := nurl.QueryUnescape(config.Passwd)
	if err != nil {
		return nil, err
	}
	config.Passwd = password

	return config, nil
}

func (m *Singlestore) SetSourceDriver(sourceDrv source.Driver) error {
	m.sourceDrv = sourceDrv
	return nil
}

func (m *Singlestore) Open(url string) (database.Driver, error) {
	config, err := urlToMySQLConfig(url)
	if err != nil {
		return nil, err
	}

	customParams, err := extractCustomQueryParams(config)
	if err != nil {
		return nil, err
	}

	noLockParam, noLock := customParams["x-no-lock"], false
	if noLockParam != "" {
		noLock, err = strconv.ParseBool(noLockParam)
		if err != nil {
			return nil, fmt.Errorf("could not parse x-no-lock as bool: %w", err)
		}
	}

	statementTimeoutParam := customParams["x-statement-timeout"]
	statementTimeout := 0
	if statementTimeoutParam != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutParam)
		if err != nil {
			return nil, fmt.Errorf("could not parse x-statement-timeout as float: %w", err)
		}
	}

	migrationsHistoryEnabled := false
	if s := customParams["x-migrations-history-enabled"]; s != "" {
		migrationsHistoryEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-migrations-history-enabled: %w", err)
		}
	}

	forceTransactionalMigrations := false
	if s := customParams["x-force-transactional-migrations"]; s != "" {
		forceTransactionalMigrations, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-force-transactional-migrations: %w", err)
		}
	}

	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	mx, err := WithInstance(db, &Config{
		DatabaseName:                 config.DBName,
		MigrationsTable:              customParams["x-migrations-table"],
		MigrationsHistoryTable:       customParams["x-migrations-history-table"],
		MigrationsHistoryEnabled:     migrationsHistoryEnabled,
		NoLock:                       noLock,
		StatementTimeout:             time.Duration(statementTimeout) * time.Millisecond,
		ForceTransactionalMigrations: forceTransactionalMigrations,
	})
	if err != nil {
		return nil, err
	}

	return mx, nil
}

func (m *Singlestore) Close() error {
	connErr := m.conn.Close()
	var dbErr error
	if m.db != nil {
		dbErr = m.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

func (m *Singlestore) Lock() error {
	if !m.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (m *Singlestore) Unlock() error {
	if !m.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (m *Singlestore) Run(migration io.Reader) error {
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if m.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.config.StatementTimeout)
		defer cancel()
	}

	migr, err = sourcing.ImportSourcing(m.sourceDrv, migr)
	if err != nil {
		return err
	}

	migrBytesList, runInTransaction, err := sourcing.GatherExecs(m.sourceDrv, migr)
	if err != nil {
		return err
	}

	if runInTransaction || m.config.ForceTransactionalMigrations {
		tx, err := m.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "transaction start failed"}
		}

		for _, migrBytes := range migrBytesList {
			query := string(migrBytes[:])
			if _, err := tx.ExecContext(ctx, query); err != nil {
				if errRollback := tx.Rollback(); errRollback != nil {
					err = multierror.Append(err, errRollback)
				}
				return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
			}
		}

		if err := tx.Commit(); err != nil {
			return &database.Error{OrigErr: err, Err: "transaction commit failed"}
		}
	} else {
		for _, migrBytes := range migrBytesList {
			query := string(migrBytes[:])
			if _, err := m.conn.ExecContext(ctx, query); err != nil {
				return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
			}
		}
	}

	// query := string(migr[:])
	// if _, err := m.conn.ExecContext(ctx, query); err != nil {
	// 	return database.Error{OrigErr: err, Err: "migration failed", Query: migr}
	// }

	return nil
}

func (m *Singlestore) SetVersion(version int, dirty bool, forced bool, knownDirection *source.Direction) (*source.Direction, error) {
	ctx := context.Background()
	tx, err := m.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	var query string

	oldVersion := ptr(0)
	oldDirty := ptr(false)

	if m.config.MigrationsHistoryEnabled {
		query := fmt.Sprintf(`
			SELECT version, dirty FROM `+"`%s`"+` LIMIT 1
		`, m.config.MigrationsTable)
		if err = tx.QueryRowContext(ctx, query).Scan(oldVersion, oldDirty); err != nil {
			if err == sql.ErrNoRows {
				// Ignore err, set prev version and dirty to nil - they don't exist
				oldVersion = nil
				oldDirty = nil
			} else {
				if errRollback := tx.Rollback(); errRollback != nil {
					err = multierror.Append(err, errRollback)
				}
				return nil, &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	direction := source.Up
	if version == database.NilVersion || (oldVersion != nil && version < *oldVersion) {
		direction = source.Down
	}
	if knownDirection != nil {
		// If the migration direction was known prior to calling SetVersion,
		// then accept it as the true direction.  This typically happens between
		// the pair of SetVersion calls during migrate.runMigrations, where one
		// call sets dirty=true; and the next sets dirty=false.
		direction = *knownDirection
	}

	if m.config.MigrationsHistoryEnabled {
		msg := fmt.Sprintf("Migration tool is preparing to attempt to validate version %d.  Setting dirty flag to true until migration is validated.", version)
		if !dirty {
			msg = fmt.Sprintf("Migration tool validated version %d.", version)

			if forced {
				msg = fmt.Sprintf("Migration tool forced version to %d.  Forcing removes the dirty flag if present.", version)
			} else if oldVersion == nil || (*oldVersion == version) {
				msg = fmt.Sprintf("Migration tool validated version %d and is removing dirty flag.", version)
			}
		}

		// "MIGRATE" isn't a traditional database action but it stands out
		// more clearly when viewing history logs.  MIGRATE represents an action
		// that was performed by the migrate tool itself (not a user edit).
		query = `
			INSERT INTO @historytablename@(action, direction, old_version, new_version, old_dirty, new_dirty, user, manual_edit, notes)
			VALUES('MIGRATE', ?, ?, ?, ?, ?, user(), false, ?);
		`
		if err = m.execWithTx(ctx, tx, query, direction, oldVersion, version, oldDirty, dirty, msg); err != nil {
			return nil, err
		}

		if direction == source.Down {
			// When app is migrating down, flag previous history records
			// of "successful up migration" as reverted so that we can ignore
			// them when calculating which migrations are actively applied.
			// When user migrates up again, they'll generate new "reverted=false"
			// rows...
			query = `
				UPDATE
					@historytablename@
				SET
					reverted = true,
					reverted_at = current_timestamp
				WHERE
					direction = ?
					AND reverted = false
					AND new_version > ?
					AND action IN ('MIGRATE', 'INSERT', 'UPDATE')
			`
			if err = m.execWithTx(ctx, tx, query, source.Up, version); err != nil {
				return nil, err
			}
		}
	}

	query = "DELETE FROM `" + m.config.MigrationsTable + "` LIMIT 1"
	if _, err := tx.ExecContext(ctx, query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query := "INSERT INTO `" + m.config.MigrationsTable + "` (version, dirty) VALUES (?, ?)"
		if _, err := tx.ExecContext(ctx, query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return &direction, nil
}

func (m *Singlestore) Version() (version int, dirty bool, err error) {
	query := "SELECT version, dirty FROM `" + m.config.MigrationsTable + "` LIMIT 1"
	err = m.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*mysql.MySQLError); ok {
			if e.Number == 0 {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (m *Singlestore) ListAppliedVersions() ([]int, error) {
	ctx := context.Background()
	rows, err := m.conn.QueryContext(ctx, `
		select
			distinct new_version
		from
			schema_migrations_history
		where
			action in ('MIGRATE', 'INSERT', 'UPDATE')
			and new_dirty = false
			and direction = 'up'
			and reverted = false
		order by
			new_version asc
	`)

	if err != nil {
		return nil, err
	}

	versions := []int{}
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}

	return versions, nil
}

func (m *Singlestore) Drop() (err error) {
	// select all tables
	query := `SHOW TABLES LIKE '%'`
	tables, err := m.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}
	if err := tables.Err(); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if len(tableNames) > 0 {
		// disable checking foreign key constraints until finished
		query = `SET foreign_key_checks = 0`
		if _, err := m.conn.ExecContext(context.Background(), query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}

		defer func() {
			// enable foreign key checks
			_, _ = m.conn.ExecContext(context.Background(), `SET foreign_key_checks = 1`)
		}()

		// delete one by one ...
		for _, t := range tableNames {
			query = "DROP TABLE IF EXISTS `" + t + "`"
			if _, err := m.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Mysql type.
func (m *Singlestore) ensureVersionTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	// check if migration table exists
	var result string
	query := `SHOW TABLES LIKE '` + m.config.MigrationsTable + `'`
	if err := m.conn.QueryRowContext(ctx, query).Scan(&result); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// if not, create the empty migration table
	query = "CREATE TABLE `" + m.config.MigrationsTable + "` (version bigint not null primary key, dirty boolean not null)"
	if _, err := m.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

// ensureHistoryTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Mysql type.
func (m *Singlestore) ensureHistoryTable() (err error) {
	if err = m.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := m.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	// check if history table exists
	var result string
	query := `SHOW TABLES LIKE '` + m.config.MigrationsHistoryTable + `'`
	if err := m.conn.QueryRowContext(ctx, query).Scan(&result); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}

	// If not, create empty history table
	if err = m.exec(ctx, `
		CREATE TABLE IF NOT EXISTS @historytablename@(
			id bigint auto_increment primary key,
			action varchar(32) not null,
			direction varchar(4) null,
			old_version bigint null,
			new_version bigint null,
			old_dirty bool null,
			new_dirty bool null,
			user text null,
			manual_edit bool not null default false,
			reverted bool not null default false,
			reverted_at timestamp null,
			created timestamp not null default current_timestamp,
			notes text null
		);
	`); err != nil {
		return err
	}

	return nil
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
// See https://github.com/go-sql-driver/mysql/blob/a059889267dc7170331388008528b3b44479bffb/utils.go#L71
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

// Use a keyword/symbol-based string replace approach
// Otherwise there are too many %s and it is hard to read
func (m *Singlestore) replaceSymbols(query string) string {
	for k, v := range map[string]string{
		"@migrationstablename@": "" + m.config.MigrationsTable + "",
		"@historytablename@":    "" + m.config.MigrationsHistoryTable + "",
	} {
		query = strings.ReplaceAll(query, k, v)
	}

	return query
}

func (m *Singlestore) exec(ctx context.Context, query string, args ...any) error {
	query = m.replaceSymbols(query)

	if _, err := m.conn.ExecContext(ctx, query, args...); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

func (m *Singlestore) execWithTx(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	query = m.replaceSymbols(query)

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
