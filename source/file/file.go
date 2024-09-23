package file

import (
	nurl "net/url"
	"os"
	"path/filepath"

	"github.com/mikedoty/golang-migrate-plus/source"
	"github.com/mikedoty/golang-migrate-plus/source/iofs"
)

func init() {
	source.Register("file", &File{})
}

type File struct {
	iofs.PartialDriver
	url  string
	path string
}

func (f *File) Open(url string) (source.Driver, error) {
	mountPath, migrationsPath, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	nf := &File{
		url:  url,
		path: mountPath,
	}

	// Migrations may be located in a subfolder of the mount path,
	// e.g.:
	//
	// /migrations
	//   /1_foo.up.sql
	//   ...
	// /views
	//   /vw_some_view.sql
	if migrationsPath == "" {
		migrationsPath = "."
	}
	if err := nf.Init(os.DirFS(mountPath), migrationsPath); err != nil {
		return nil, err
	}
	return nf, nil
}

func parseURL(url string) (mountPath string, migrationsPath string, err error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", "", err
	}
	// concat host and path to restore full path
	// host might be `.`
	p := u.Opaque
	if len(p) == 0 {
		p = u.Host + u.Path
	}

	migrationsPath = u.Query().Get("x-migrations-path")

	if len(p) == 0 {
		// default to current directory if no path
		wd, err := os.Getwd()
		if err != nil {
			return "", "", err
		}
		p = wd

	} else if p[0:1] == "." || p[0:1] != "/" {
		// make path absolute if relative
		abs, err := filepath.Abs(p)
		if err != nil {
			return "", "", err
		}
		p = abs
	}
	return p, migrationsPath, nil
}
