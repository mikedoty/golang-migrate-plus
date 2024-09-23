//go:build clickhouse
// +build clickhouse

package cli

import (
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/mikedoty/golang-migrate-plus/database/clickhouse"
)
