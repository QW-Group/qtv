package info

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"
)

//
// Classic Quake info strings manipulation.
//

const (
	MAX_INFO_KEY = 64
)

var (
	ErrTooLong   = errors.New("info: too long name or value")
	ErrEmptyName = errors.New("info: empty name")
	ErrBadChars  = errors.New("info: bad characters inside name or value")
)

// Info strings storage object.
type Info struct {
	info map[string]string
}

// Allocate new info strings object.
func NewInfo() *Info {
	info := &Info{}
	info.Reset()
	return info
}

// Allocate new info strings object from string, str is in form of `\key1\value1\key2\value2`.
func NewInfoFromStr(str string) (*Info, error) {
	info := NewInfo()
	err := info.FromStr(str)
	return info, err
}

// Remove all info strings from the inforstring object.
func (info *Info) Reset() {
	*info = Info{
		info: map[string]string{},
	}
}

// Return true if info string object contains nothing.
func (info *Info) IsEmpty() bool {
	return len(info.info) == 0
}

// Get info string value for key name.
func (info *Info) Get(name string) string {
	value, _ := info.GetOK(name)
	return value
}

// Get info string value for key name with OK idiom.
func (info *Info) GetOK(name string) (value string, ok bool) {
	value, ok = info.info[name]
	return value, ok
}

var (
	badChars = "\\\"\r\n$;"
)

// Validate string.
func strHasInvalidBytes(s string) bool {
	if strings.ContainsAny(s, badChars) {
		return true
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == '\\'+128 || b <= 13 {
			return true
		}
	}
	return false
}

// Set info string value for key name.
func (info *Info) set(name string, value string) error {
	if name == "" {
		return ErrEmptyName
	}

	if len(name) >= MAX_INFO_KEY || len(value) >= MAX_INFO_KEY {
		return ErrTooLong
	}

	if strHasInvalidBytes(name) || strHasInvalidBytes(value) {
		return ErrBadChars
	}

	if info.info == nil {
		info.Reset() // Allow using nil value.
	}

	info.info[name] = value

	return nil
}

// Set info string value for key name.
func (info *Info) Set(name string, a ...interface{}) error {
	v := fmt.Sprint(a...)
	err := info.set(name, v)
	return multierror.Prefix(err, "Info.Set:")
}

// Set info string value for key name.
func (info *Info) Setf(name string, format string, a ...interface{}) error {
	v := fmt.Sprintf(format, a...)
	err := info.set(name, v)
	return multierror.Prefix(err, "Info.Setf:")
}

// FromStr converts string like this: `\key1\value1\key2\value2` into map.
func (info *Info) FromStr(str string) (mErr error) {
	defer func() { mErr = multierror.Prefix(mErr, "Info.FromStr:") }()

	info.Reset()
	if str == "" {
		return nil
	}

	tokens := strings.Split(str, "\\")
	if len(tokens) < 3 || tokens[0] != "" {
		// First token should always be empty.
		mErr = multierror.Append(mErr, errors.New("invalid userinfo"))
		return mErr
	}
	tokens = tokens[1:]
	for i := 0; i+1 < len(tokens); i += 2 {
		name := tokens[i]
		value := tokens[i+1]
		err := info.Set(name, value)
		if err != nil {
			mErr = multierror.Append(mErr, err)
			continue
		}
	}
	return mErr
}

// String converts map to the string like this: `\key1\value1\key2\value2`.
func (info *Info) String() string {
	tokens := make([]string, 0, len(info.info)+1)
	tokens = append(tokens, "")
	for k, v := range info.info {
		if v == "" {
			continue // Do not put empty value.
		}
		tokens = append(tokens, k)
		tokens = append(tokens, v)
	}
	str := strings.Join(tokens, "\\")
	return str
}

// PrintList print map as human readable list.
func (info *Info) PrintList() string {
	var b strings.Builder

	for k, v := range info.info {
		fmt.Fprintf(&b, "%-19s %s\n", k, v)
	}

	return b.String()
}
