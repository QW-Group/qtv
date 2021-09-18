package qfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode"

	"github.com/hashicorp/go-multierror"
)

var (
	searchPath = []string{"qtv", "qw", "id1"}
)

// Perform path clean and basic security validation.
func BasePath(base string, name string) (string, error) {
	if base == "" {
		base = "."
	}
	fullName := filepath.Join(base, filepath.Clean(string(os.PathSeparator)+name))
	if filepath.IsAbs(fullName) ||
		(len(fullName) >= 1 && (fullName[0] == '\\' || fullName[0] == '/')) ||
		(len(fullName) >= 2 && (fullName[1] == ':')) {
		return "", fmt.Errorf("absolute path is not allowed: %s", fullName)
	}
	if len(fullName) > 0 && fullName[0] == '.' {
		return "", fmt.Errorf("leading dot in file name is not allowed: %s", fullName)
	}
	return fullName, nil
}

func open(base string, name string) (*os.File, error) {
	if fullName, err := BasePath(base, name); err != nil {
		return nil, err
	} else {
		return os.Open(fullName)
	}
}

// Attempt to open file for reading from any search path.
// You can provie additional search path with 'base' argument.
// Empty 'base' argument allows root QTV directory (not recommended from security point of view).
func Open(base string, name string) (*os.File, int64, error) {
	var mErr error
	sp := make([]string, 0, 1+len(searchPath))
	sp = append(sp, base)
	sp = append(sp, searchPath...)
	for i, v := range sp {
		if i > 0 && v == base {
			continue
		}
		f, err := open(v, name)
		if err != nil {
			mErr = multierror.Append(mErr, err)
			continue
		}
		if fi, err := f.Stat(); err != nil {
			f.Close()
			mErr = multierror.Append(mErr, err)
			continue
		} else {
			// Return file, ignore previous errors inside mErr if any.
			return f, fi.Size(), nil
		}
	}
	return nil, 0, multierror.Prefix(mErr, "Open:")
}

// Attempt to read file from any search path.
// You can provie additional search path with 'base' argument.
// Empty 'base' argument allows root QTV directory (not recommended from security point of view).
func Read(base string, name string) ([]byte, error) {
	f, _, err := Open(base, name)
	if err != nil {
		return nil, multierror.Prefix(err, "Read:")
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Return true if 's' contains only letters/digits or underscore.
func IsSimplePath(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			continue
		}
		return false
	}
	return true
}
