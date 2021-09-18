package info

import (
	"sync"
)

// Thread safe version of Info.
type InfoTs struct {
	info Info
	mu   sync.Mutex
}

func NewInfoTs() *InfoTs {
	its := &InfoTs{}
	its.info.Reset()
	return its
}

func NewInfoTsFromStr(str string) (*InfoTs, error) {
	its := NewInfoTs()
	err := its.info.FromStr(str)
	return its, err
}

func (its *InfoTs) Reset() {
	its.mu.Lock()
	defer its.mu.Unlock()
	its.info.Reset()
}

func (its *InfoTs) IsEmpty() bool {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.IsEmpty()
}

func (its *InfoTs) Get(name string) string {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.Get(name)
}

func (its *InfoTs) GetOK(name string) (value string, ok bool) {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.GetOK(name)
}

func (its *InfoTs) Set(name string, a ...interface{}) error {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.Set(name, a...)
}

func (its *InfoTs) Setf(name string, format string, a ...interface{}) error {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.Setf(name, format, a...)
}

func (its *InfoTs) FromStr(str string) (mErr error) {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.FromStr(str)
}

func (its *InfoTs) String() string {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.String()
}

func (its *InfoTs) PrintList() string {
	its.mu.Lock()
	defer its.mu.Unlock()
	return its.info.PrintList()
}
