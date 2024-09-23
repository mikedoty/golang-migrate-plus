//go:build go1.9
// +build go1.9

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/mikedoty/golang-migrate-plus"
	"github.com/mikedoty/golang-migrate-plus/database"
	"github.com/mikedoty/golang-migrate-plus/database/multistmt"
	"github.com/mikedoty/golang-migrate-plus/database/sourcing"
	"github.com/mikedoty/golang-migrate-plus/source"
	"github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
)

func init() {
	db := Postgres{}
	database.Register("postgres", &db)
	database.Register("postgresql", &db)
}

func ptr[T comparable](t T) *T {
	return &t
}

var (
	multiStmtDelimiter = []byte(";")

	DefaultMigrationsTable       = "schema_migrations"
	DefaultHistoryTable          = "schema_migrations_history"
	DefaultMultiStatementMaxSize = 10 * 1 << 20 // 10 MB
)

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable              string
	MigrationsHistoryTable       string
	MigrationsHistoryEnabled     bool
	MigrationsTableQuoted        bool
	MultiStatementEnabled        bool
	DatabaseName                 string
	SchemaName                   string
	migrationsSchemaName         string
	migrationsTableName          string
	migrationsHistoryTableName   string
	StatementTimeout             time.Duration
	MultiStatementMaxSize        int
	MigrationsRelativePath       string // Optionally specify a path (e.g. ./migrations) where the migration files exist on the given file mount point (i.e. sourceURL)
	ForceTransactionalMigrations bool
}

type Postgres struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked atomic.Bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config

	sourceDrv source.Driver
}

func WithConnection(ctx context.Context, conn *sql.Conn, config *Config) (*Postgres, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := conn.QueryRowContext(ctx, query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		var schemaName sql.NullString
		if err := conn.QueryRowContext(ctx, query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if !schemaName.Valid {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName.String
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}
	if len(config.MigrationsHistoryTable) == 0 {
		config.MigrationsHistoryTable = DefaultHistoryTable
	}

	config.migrationsSchemaName = config.SchemaName

	config.migrationsTableName = config.MigrationsTable
	config.migrationsHistoryTableName = config.MigrationsHistoryTable

	if config.MigrationsTableQuoted {
		re := regexp.MustCompile(`"(.*?)"`)

		result := re.FindAllStringSubmatch(config.MigrationsTable, -1)
		config.migrationsTableName = result[len(result)-1][1]
		if len(result) == 2 {
			config.migrationsSchemaName = result[0][1]
		} else if len(result) > 2 {
			return nil, fmt.Errorf("\"%s\" MigrationsTable contains too many dot characters", config.MigrationsTable)
		}

		if config.MigrationsHistoryEnabled {
			result = re.FindAllStringSubmatch(config.MigrationsHistoryTable, -1)
			config.migrationsHistoryTableName = result[len(result)-1][1]
			if len(result) == 2 {
				if result[0][1] != config.migrationsSchemaName {
					return nil, fmt.Errorf("MigrationsHistoryTable must belong to the same schema as MigrationsTable")
				}
			} else if len(result) > 2 {
				return nil, fmt.Errorf("\"%s\" MigrationsHistoryTable contains too many dot characters", config.MigrationsHistoryTable)
			}
		}
	}

	px := &Postgres{
		conn:   conn,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	if config.MigrationsHistoryEnabled {
		if err := px.ensureHistoryTable(); err != nil {
			return nil, err
		}
	}

	return px, nil
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	ctx := context.Background()

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	conn, err := instance.Conn(ctx)
	if err != nil {
		return nil, err
	}

	px, err := WithConnection(ctx, conn, config)
	if err != nil {
		return nil, err
	}
	px.db = instance
	return px, nil
}

func (p *Postgres) SetSourceDriver(sourceDrv source.Driver) error {
	p.sourceDrv = sourceDrv
	return nil
}

func (p *Postgres) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", migrate.FilterCustomQuery(purl).String())
	if err != nil {
		return nil, err
	}

	migrationsHistoryEnabled := false
	if s := purl.Query().Get("x-migrations-history-enabled"); len(s) > 0 {
		migrationsHistoryEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-migrations-history-enabled: %w", err)
		}
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	migrationsHistoryTable := purl.Query().Get("x-migrations-history-table")

	migrationsTableQuoted := false
	if s := purl.Query().Get("x-migrations-table-quoted"); len(s) > 0 {
		migrationsTableQuoted, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-migrations-table-quoted: %w", err)
		}
	}
	if (len(migrationsTable) > 0) && (migrationsTableQuoted) && ((migrationsTable[0] != '"') || (migrationsTable[len(migrationsTable)-1] != '"')) {
		return nil, fmt.Errorf("x-migrations-table must be quoted (for instance '\"migrate\".\"schema_migrations\"') when x-migrations-table-quoted is enabled, current value is: %s", migrationsTable)
	}
	if (len(migrationsHistoryTable) > 0) && (migrationsTableQuoted) && ((migrationsHistoryTable[0] != '"') || (migrationsHistoryTable[len(migrationsHistoryTable)-1] != '"')) {
		return nil, fmt.Errorf("x-migrations-history-table must be quoted (for instance '\"migrate\".\"schema_migrations_history\"') when x-migrations-table-quoted is enabled, current value is: %s", migrationsHistoryTable)
	}

	if migrationsTableQuoted {
		if len(migrationsTable) == 0 {
			return nil, fmt.Errorf("x-migrations-table must be provided when x-migrations-table-quoted is enabled")
		}
		if migrationsHistoryEnabled && len(migrationsHistoryTable) == 0 {
			return nil, fmt.Errorf("x-migrations-history-table must be provided when x-migrations-table-quoted is enabled")
		}
	}

	statementTimeoutString := purl.Query().Get("x-statement-timeout")
	statementTimeout := 0
	if statementTimeoutString != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutString)
		if err != nil {
			return nil, err
		}
	}

	multiStatementMaxSize := DefaultMultiStatementMaxSize
	if s := purl.Query().Get("x-multi-statement-max-size"); len(s) > 0 {
		multiStatementMaxSize, err = strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		if multiStatementMaxSize <= 0 {
			multiStatementMaxSize = DefaultMultiStatementMaxSize
		}
	}

	multiStatementEnabled := false
	if s := purl.Query().Get("x-multi-statement"); len(s) > 0 {
		multiStatementEnabled, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-multi-statement: %w", err)
		}
	}

	forceTransactionalMigrations := false
	if s := purl.Query().Get("x-force-transactional-migrations"); len(s) > 0 {
		forceTransactionalMigrations, err = strconv.ParseBool(s)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse option x-force-transactional-migrations: %w", err)
		}
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:                 purl.Path,
		MigrationsHistoryEnabled:     migrationsHistoryEnabled,
		MigrationsTable:              migrationsTable,
		MigrationsHistoryTable:       migrationsHistoryTable,
		MigrationsTableQuoted:        migrationsTableQuoted,
		StatementTimeout:             time.Duration(statementTimeout) * time.Millisecond,
		MultiStatementEnabled:        multiStatementEnabled,
		MultiStatementMaxSize:        multiStatementMaxSize,
		ForceTransactionalMigrations: forceTransactionalMigrations,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Postgres) Close() error {
	connErr := p.conn.Close()
	var dbErr error
	if p.db != nil {
		dbErr = p.db.Close()
	}

	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Postgres) Lock() error {
	return database.CasRestoreOnErr(&p.isLocked, false, true, database.ErrLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		// This will wait indefinitely until the lock can be acquired.
		query := `SELECT pg_advisory_lock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
		}

		return nil
	})
}

func (p *Postgres) Unlock() error {
	return database.CasRestoreOnErr(&p.isLocked, true, false, database.ErrNotLocked, func() error {
		aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.migrationsSchemaName, p.config.migrationsTableName)
		if err != nil {
			return err
		}

		query := `SELECT pg_advisory_unlock($1)`
		if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
		return nil
	})
}

func (p *Postgres) Run(migration io.Reader) error {
	ctx := context.Background()

	if p.config.MultiStatementEnabled {
		var err error
		if e := multistmt.ParseWithSourcing(p.sourceDrv, migration, multiStmtDelimiter, p.config.MultiStatementMaxSize, func(m []byte) bool {
			if err = p.runStatement(ctx, nil, m); err != nil {
				return false
			}
			return true
		}); e != nil {
			return e
		}
		return err
	}
	migr, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	migr, err = sourcing.ImportSourcing(p.sourceDrv, migr)
	if err != nil {
		return err
	}

	migrBytesList, runInTransaction, err := sourcing.GatherExecs(p.sourceDrv, migr)
	if err != nil {
		return err
	}

	if runInTransaction || p.config.ForceTransactionalMigrations {
		tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return &database.Error{OrigErr: err, Err: "transaction start failed"}
		}

		for _, migrBytes := range migrBytesList {
			if err := p.runStatement(ctx, tx, migrBytes); err != nil {
				if errRollback := tx.Rollback(); errRollback != nil {
					err = multierror.Append(err, errRollback)
				}
				return &database.Error{OrigErr: err, Err: "migration failed", Query: migr}
			}
		}

		if err := tx.Commit(); err != nil {
			return &database.Error{OrigErr: err, Err: "transaction commit failed"}
		}

		return nil
	} else {
		for _, migrBytes := range migrBytesList {
			if err := p.runStatement(ctx, nil, migrBytes); err != nil {
				return err
			}
		}

		return nil
	}

	// return p.runStatement(migr)
}

func (p *Postgres) runStatement(ctx context.Context, tx *sql.Tx, statement []byte) error {
	if p.config.StatementTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.StatementTimeout)
		defer cancel()
	}
	query := string(statement)
	if strings.TrimSpace(query) == "" {
		return nil
	}

	var err error
	if tx == nil {
		_, err = p.conn.ExecContext(ctx, query)
	} else {
		_, err = tx.ExecContext(ctx, query)
	}

	if err != nil {
		// if _, err := maybeTx.ExecContext(ctx, query); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			var line uint
			var col uint
			var lineColOK bool
			if pgErr.Position != "" {
				if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
					line, col, lineColOK = computeLineFromPos(query, int(pos))
				}
			}
			message := fmt.Sprintf("migration failed: %s", pgErr.Message)
			if lineColOK {
				message = fmt.Sprintf("%s (column %d)", message, col)
			}
			if pgErr.Detail != "" {
				message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
			}
			return database.Error{OrigErr: err, Err: message, Query: statement, Line: line}
		}
		return database.Error{OrigErr: err, Err: "migration failed", Query: statement}
	}
	return nil
}

func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.Replace(s, "\r\n", "\n", -1)
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

func (p *Postgres) SetVersion(version int, dirty bool, forced bool, knownDirection *source.Direction) (*source.Direction, error) {
	ctx := context.Background()
	tx, err := p.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	var query string

	oldVersion := ptr(0)
	oldDirty := ptr(false)

	if p.config.MigrationsHistoryEnabled {
		query := fmt.Sprintf(`
		SELECT version, dirty FROM %s.%s LIMIT 1
	`, pq.QuoteIdentifier(p.config.migrationsSchemaName), pq.QuoteIdentifier(p.config.migrationsTableName))
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

	if p.config.MigrationsHistoryEnabled {
		msg := fmt.Sprintf("Migration tool is preparing to attempt to validate version %d.  Setting dirty flag to true until migration is validated.", version)
		if !dirty {
			msg = fmt.Sprintf("Migration tool validated version %d.", version)

			if forced {
				msg = fmt.Sprintf("Migration tool forced version to %d.  Forcing removes the dirty flag if present.", version)
			} else if oldVersion == nil || (*oldVersion == version) {
				msg = fmt.Sprintf("Migration tool validated version %d and is removing dirty flag.", version)
			}
		}

		// Disable before-insert trigger so it doesn't get flagged as is_manual=true
		query = `ALTER TABLE "@schema@"."@historytablename@" DISABLE TRIGGER @historytablename@_before_insert_trigger;`
		if err = p.execWithTx(tx, query); err != nil {
			return nil, err
		}

		// "MIGRATE" isn't a traditional database action but it stands out
		// more clearly when viewing history logs.  MIGRATE represents an action
		// that was performed by the migrate tool itself (not a user edit).
		query = `
			INSERT INTO "@schema@"."@historytablename@"(action, direction, old_version, new_version, old_dirty, new_dirty, application_name, manual_edit, notes)
			VALUES('MIGRATE', $1, $2, $3, $4, $5, current_setting('application_name'), false, $6);
		`
		if err = p.execWithTx(tx, query, direction, oldVersion, version, oldDirty, dirty, msg); err != nil {
			return nil, err
		}

		// Re-enable trigger when we're done
		query = `ALTER TABLE @schema@.@historytablename@ ENABLE TRIGGER @historytablename@_before_insert_trigger;`
		if err = p.execWithTx(tx, query); err != nil {
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
					@schema@.@historytablename@
				SET
					reverted = true,
					reverted_at = current_timestamp
				WHERE
					direction = $1
					AND reverted = false
					AND new_version > $2
					AND action IN ('MIGRATE', 'INSERT', 'UPDATE')
			`
			if err = p.execWithTx(tx, query, source.Up, version); err != nil {
				return nil, err
			}
		}
	}

	if p.config.MigrationsHistoryEnabled {
		query = `ALTER TABLE "@schema@"."@migrationstablename@" DISABLE TRIGGER @migrationstablename@_before_truncate_trigger;`
		if err = p.execWithTx(tx, query); err != nil {
			return nil, err
		}
	}

	query = `TRUNCATE ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName)
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return nil, &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if p.config.MigrationsHistoryEnabled {
		query = `ALTER TABLE "@schema@"."@migrationstablename@" ENABLE TRIGGER @migrationstablename@_before_truncate_trigger;`
		if err = p.execWithTx(tx, query); err != nil {
			return nil, err
		}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		if p.config.MigrationsHistoryEnabled {
			// Disable after insert trigger; we handle insert into history table
			// manually in code when using migrate tool, triggers are for manual
			// user edits in tools like DBeaver
			query = `ALTER TABLE "@schema@"."@migrationstablename@" DISABLE TRIGGER @migrationstablename@_after_insert_trigger;`
			if err = p.execWithTx(tx, query); err != nil {
				return nil, err
			}
		}

		query = `INSERT INTO ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version, dirty) VALUES ($1, $2)`
		if err = p.execWithTx(tx, query, version, dirty); err != nil {
			return nil, err
		}

		if p.config.MigrationsHistoryEnabled {
			query = `ALTER TABLE "@schema@"."@migrationstablename@" ENABLE TRIGGER @migrationstablename@_after_insert_trigger;`
			if err = p.execWithTx(tx, query); err != nil {
				return nil, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return &direction, nil
}

func (p *Postgres) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (p *Postgres) ListAppliedVersions() ([]int, error) {
	ctx := context.Background()
	rows, err := p.conn.QueryContext(ctx, `
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

func (p *Postgres) Drop() (err error) {
	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := p.conn.QueryContext(context.Background(), query)
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
		// delete one by one ...
		for _, t := range tableNames {
			query = `DROP TABLE IF EXISTS ` + pq.QuoteIdentifier(t) + ` CASCADE`
			if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Postgres type.
func (p *Postgres) ensureVersionTable() (err error) {
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	// This block checks whether the `MigrationsTable` already exists. This is useful because it allows read only postgres
	// users to also check the current version of the schema. Previously, even if `MigrationsTable` existed, the
	// `CREATE TABLE IF NOT EXISTS...` query would fail because the user does not have the CREATE permission.
	// Taken from https://github.com/mattes/migrate/blob/master/database/postgres/postgres.go#L258
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := p.conn.QueryRowContext(ctx, query, p.config.migrationsSchemaName, p.config.migrationsTableName)

	var count int
	err = row.Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config.migrationsSchemaName) + `.` + pq.QuoteIdentifier(p.config.migrationsTableName) + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err = p.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// ensureHistoryTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Postgres type.
func (p *Postgres) ensureHistoryTable() (err error) {
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	ctx := context.Background()

	// This block checks whether the `MigrationsHistoryTable` already exists. This is useful because it allows read only postgres
	// users to also check the current version of the schema. Previously, even if `MigrationsTable` existed, the
	// `CREATE TABLE IF NOT EXISTS...` query would fail because the user does not have the CREATE permission.
	// Taken from https://github.com/mattes/migrate/blob/master/database/postgres/postgres.go#L258
	query := `SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`
	row := p.conn.QueryRowContext(ctx, query, p.config.migrationsSchemaName, p.config.migrationsHistoryTableName)

	var count int
	err = row.Scan(&count)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	if count == 1 {
		return nil
	}

	query = p.replaceSymbols(`
		CREATE TABLE IF NOT EXISTS @schema@.@historytablename@(
			id bigserial primary key,
			action varchar(32) not null,
			direction varchar(4) null,
			old_version bigint null,
			new_version bigint null,
			old_dirty bool null,
			new_dirty bool null,
			application_name text null,
			manual_edit bool not null default false,
			reverted bool not null default false,
			reverted_at timestamp null,
			created timestamp not null default current_timestamp,
			notes text null
		);

		CREATE OR REPLACE FUNCTION @migrationstablename@_update_func() RETURNS trigger AS $$
		BEGIN
			INSERT INTO @schema@.@historytablename@(action, old_version, new_version, old_dirty, new_dirty, application_name, notes)
			VALUES('UPDATE', old.version, new.version, old.dirty, new.dirty, current_setting('application_name'), (
			CASE
				WHEN OLD.version != NEW.version THEN
					'Manual update of migration version'
				WHEN OLD.dirty != NEW.dirty THEN
					'Manual update of dirty flag'
				ELSE
					'NOOP'
				END
			));

			RETURN NEW;
		END
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER @migrationstablename@_update_trigger AFTER UPDATE ON @schema@.@migrationstablename@
		FOR EACH ROW EXECUTE FUNCTION @migrationstablename@_update_func();

		CREATE OR REPLACE FUNCTION @historytablename@_before_insert_func() RETURNS trigger AS $$
		BEGIN
			NEW.manual_edit = true;
			RETURN NEW;
		END
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER @historytablename@_before_insert_trigger BEFORE INSERT ON @schema@.@historytablename@
		FOR EACH ROW EXECUTE FUNCTION @historytablename@_before_insert_func();

		CREATE OR REPLACE FUNCTION @migrationstablename@_after_insert_func() RETURNS trigger AS $$
		BEGIN
			INSERT INTO @schema@.@historytablename@(action, old_version, new_version, old_dirty, new_dirty, application_name, manual_edit, notes)
			VALUES('INSERT', NULL, new.version, NULL, new.dirty, current_setting('application_name'), true, 'Manual row insert into migrations table.  Please make sure there is only one row in migrations table!');

			RETURN NEW;
		END
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER @migrationstablename@_after_insert_trigger AFTER INSERT ON @schema@.@migrationstablename@
		FOR EACH ROW EXECUTE FUNCTION @migrationstablename@_after_insert_func();

		CREATE OR REPLACE FUNCTION @migrationstablename@_after_delete_func() RETURNS trigger AS $$
		BEGIN
			INSERT INTO @schema@.@historytablename@(action, old_version, new_version, old_dirty, new_dirty, application_name, manual_edit, notes)
			VALUES('DELETE', old.version, NULL, old.dirty, NULL, current_setting('application_name'), true, 'Manual delete from migrations table.');

			RETURN NEW;
		END
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER @migrationstablename@_after_delete_trigger AFTER DELETE ON @schema@.@migrationstablename@
		FOR EACH ROW EXECUTE FUNCTION @migrationstablename@_after_delete_func();

		CREATE OR REPLACE FUNCTION @migrationstablename@_before_truncate_func() RETURNS trigger AS $$
		BEGIN
			INSERT INTO @schema@.@historytablename@(action, old_version, new_version, old_dirty, new_dirty, application_name, manual_edit, notes)
			SELECT
				'TRUNCATE', version, NULL, dirty, NULL, current_setting('application_name'), true, 'Manual truncate executed on migrations table.'
			FROM
				@schema@.@migrationstablename@;

			RETURN NULL;
		END
		$$ LANGUAGE plpgsql;

		CREATE OR REPLACE TRIGGER @migrationstablename@_before_truncate_trigger BEFORE TRUNCATE ON @schema@.@migrationstablename@
		FOR EACH STATEMENT EXECUTE FUNCTION @migrationstablename@_before_truncate_func();
	`)

	if _, err = p.conn.ExecContext(ctx, query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}

// Use a keyword/symbol-based string replace approach
// Otherwise there are too many %s and it is hard to read
func (p *Postgres) replaceSymbols(query string) string {
	for k, v := range map[string]string{
		"@schema@":              p.config.migrationsSchemaName,
		"@migrationstablename@": p.config.migrationsTableName,
		"@historytablename@":    p.config.migrationsHistoryTableName,
	} {
		query = strings.ReplaceAll(query, k, v)
	}

	return query
}

func (p *Postgres) execWithTx(tx *sql.Tx, query string, args ...any) error {
	query = p.replaceSymbols(query)

	if _, err := tx.Exec(query, args...); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
