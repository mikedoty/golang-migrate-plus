//go:build singlestore
// +build singlestore

package cli

import (
	_ "github.com/golang-migrate/migrate/v4/database/singlestore"
)
