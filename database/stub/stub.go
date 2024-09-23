package stub

import (
	"io"
	"reflect"

	"go.uber.org/atomic"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/source"
)

func init() {
	database.Register("stub", &Stub{})
}

type Stub struct {
	Url               string
	Instance          interface{}
	CurrentVersion    int
	MigrationSequence []string
	LastRunMigration  []byte // todo: make []string
	IsDirty           bool
	isLocked          atomic.Bool

	Config    *Config
	sourceDrv source.Driver
}

// This database driver does not currently support file sourcing.
func (s *Stub) SetSourceDriver(sourceDrv source.Driver) error {
	s.sourceDrv = sourceDrv
	return nil
}

func (s *Stub) Open(url string) (database.Driver, error) {
	return &Stub{
		Url:               url,
		CurrentVersion:    database.NilVersion,
		MigrationSequence: make([]string, 0),
		Config:            &Config{},
	}, nil
}

type Config struct{}

func WithInstance(instance interface{}, config *Config) (database.Driver, error) {
	return &Stub{
		Instance:          instance,
		CurrentVersion:    database.NilVersion,
		MigrationSequence: make([]string, 0),
		Config:            config,
	}, nil
}

func (s *Stub) Close() error {
	return nil
}

func (s *Stub) Lock() error {
	if !s.isLocked.CAS(false, true) {
		return database.ErrLocked
	}
	return nil
}

func (s *Stub) Unlock() error {
	if !s.isLocked.CAS(true, false) {
		return database.ErrNotLocked
	}
	return nil
}

func (s *Stub) Run(migration io.Reader) error {
	m, err := io.ReadAll(migration)
	if err != nil {
		return err
	}
	s.LastRunMigration = m
	s.MigrationSequence = append(s.MigrationSequence, string(m[:]))
	return nil
}

func (s *Stub) SetVersion(version int, state bool, forced bool, knownDirection *source.Direction) (*source.Direction, error) {
	s.CurrentVersion = version
	s.IsDirty = state
	return nil, nil
}

func (s *Stub) Version() (version int, dirty bool, err error) {
	return s.CurrentVersion, s.IsDirty, nil
}

func (s *Stub) ListAppliedVersions() ([]int, error) {
	// Not implemented
	return []int{}, nil
}

const DROP = "DROP"

func (s *Stub) Drop() error {
	s.CurrentVersion = database.NilVersion
	s.LastRunMigration = nil
	s.MigrationSequence = append(s.MigrationSequence, DROP)
	return nil
}

func (s *Stub) EqualSequence(seq []string) bool {
	return reflect.DeepEqual(seq, s.MigrationSequence)
}
