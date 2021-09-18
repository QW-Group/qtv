package qtv

import (
	"fmt"
	"sync"

	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

//
// QTV variables storage.
//

type qVarOnChange func(qtv *QTV, name string, cur *qVar, new *qVar) (allowed bool)

type qVar struct {
	Str      string
	Float    float64
	Int      int
	Dur      time.Duration
	Bool     bool
	Modified atomic.Bool
	Flags    qVarFlags
	OnChange qVarOnChange
}

type qVarFlags int

const (
	qVarFlagReadOnly   = 1 << iota // Variable is read only.
	qVarFlagInitOnly               // Variable could be changed only while QTV is not fully initialized.
	qVarFlagServerInfo             // Variable mirrored inside server info.
)

func newQVar(value string) *qVar {
	qv := &qVar{}
	qv.Reset(value)
	return qv
}

func (qv *qVar) Reset(value string) {
	fv, _ := strconv.ParseFloat(value, 64)
	*qv = qVar{
		Str:      value,
		Float:    fv,
		Int:      int(fv),
		Dur:      time.Duration(fv),
		Bool:     fv != 0,
		Flags:    qv.Flags,    // Keep.
		OnChange: qv.OnChange, // Keep.
	}
	qv.Modified.Store(true)
}

type qVarStorage struct {
	mu          sync.Mutex   // Used for setters only.
	v           atomic.Value // copy-on-write. (except for QVar.Modified field which is atomic)
	initialized bool         // True if QTV initialized.
	qtv         *QTV
}

type qvarMap map[string]*qVar

func newQVarStorage(qtv *QTV) *qVarStorage {
	qs := &qVarStorage{
		qtv: qtv,
	}
	qs.v.Store(make(qvarMap))

	qs.regCommands(qtv)

	return qs
}

func (qs *qVarStorage) regCommands(qtv *QTV) {
	// There is no need for set command right now.
	// qtv.cmd.Register("set", setCmd)
	qtv.cmd.Register("varlist", varListCmd)
}

// Returns ptr to value or nil. You should NOT modify it.
func (qs *qVarStorage) Find(name string) *qVar {
	vars := qs.v.Load().(qvarMap)

	if qv, ok := vars[name]; ok {
		return qv
	}
	return nil
}

// QTV notify storage what it was initialized.
func (qs *qVarStorage) QtvWasInitializedNotify() {
	// Block other setters.
	qs.mu.Lock()
	defer qs.mu.Unlock()

	qs.initialized = true
}

// Returns ptr to value or panic if not found. You should NOT modify it.
func (qs *qVarStorage) Get(name string) *qVar {
	qv := qs.Find(name)
	if qv == nil {
		log.Panic().Str("ctx", "qVarStorage").Msgf("QVar not found: \"%v\"", name)
	}
	return qv
}

// Set sets new value for variable.
func (qs *qVarStorage) Set(name string, value string) {
	qv := newQVar(value)
	qs.set(name, qv, true, true)
}

// Setf sets new value for variable.
func (qs *qVarStorage) Setf(name string, format string, a ...interface{}) {
	qs.Set(name, fmt.Sprintf(format, a...))
}

// set sets new value for variable, copy-on-write is used.
func (qs *qVarStorage) set(name string, newValue *qVar, keepCurOnChange bool, keepCurFlags bool) {
	// Block other setters.
	qs.mu.Lock()
	defer qs.mu.Unlock()

	if newValue != nil {
		cur := qs.Find(name)
		// Keep OnChange callback and flags if we just updating value for variable.
		if cur != nil {
			if (cur.Flags&qVarFlagInitOnly) != 0 && qs.initialized {
				log.Error().Str("ctx", "qVarStorage").Msgf("can't change variable after QTV initialized: %q", name)
				return
			}
			if (cur.Flags & qVarFlagReadOnly) != 0 {
				log.Error().Str("ctx", "qVarStorage").Msgf("read only variable: %q", name)
				return
			}

			if keepCurOnChange {
				newValue.OnChange = cur.OnChange
			}
			if keepCurFlags {
				newValue.Flags = cur.Flags
			}
		}

		// Convert "0" to empty "" string, so it would be completely removed from serverinfo.
		if (newValue.Flags&qVarFlagServerInfo) != 0 && newValue.Str == "0" {
			newValue.Reset("")
		}

		if newValue.OnChange != nil && !newValue.OnChange(qs.qtv, name, cur, newValue) {
			return // Change was not allowed.
		}

		if (newValue.Flags & qVarFlagServerInfo) != 0 {
			qs.qtv.serverInfo.Set(name, newValue.Str)
		}
	}

	m1 := qs.v.Load().(qvarMap) // Load current value.
	m2 := make(qvarMap)         // Create a new value.
	for k, v := range m1 {
		m2[k] = v // Copy all data from the current object to the new one.
	}
	// Do the update that we need.
	if newValue != nil {
		m2[name] = newValue // Set new value.
	} else {
		delete(m2, name) // Actually someone trying to delete value.
	}
	qs.v.Store(m2) // Atomically replace the current object with the new one.

	// At this point all new readers start working with the new version.
	// The old version will be garbage collected once the existing readers
	// (if any) are done with it.
}

// Register variable.
func (qs *qVarStorage) RegEx(name string, value interface{}, flags qVarFlags, OnChange qVarOnChange) {
	qv := qs.Find(name)
	if qv != nil {
		log.Panic().Str("ctx", "qVarStorage").Msgf("variable %q already registered", name)
		return
	}
	qv = newQVar(fmt.Sprint(value))
	qv.Flags = flags
	qv.OnChange = OnChange
	qs.set(name, qv, false, false)
}

// Register variable.
func (qs *qVarStorage) Reg(name string, value string) {
	qs.RegEx(name, value, 0, nil)
}

// Register variable.
func (qs *qVarStorage) Regf(name string, format string, a ...interface{}) {
	qs.Reg(name, fmt.Sprintf(format, a...))
}

// Delete variable.
func (qs *qVarStorage) Del(name string) {
	qs.set(name, nil, false, false)
}

func (qs *qVarStorage) ExpandString(str string) string {
	mapper := func(varName string) string {
		qv := qs.Find(varName)
		if qv == nil {
			return ""
		}
		return qv.Str
	}
	return os.Expand(str, mapper)
}

func (qs *qVarStorage) CommandIsVar(qtv *QTV, cmdArgs *qCmdArgs) (bool, error) {
	if cmdArgs.Argc() == 0 {
		return false, nil
	}
	name := cmdArgs.Name()
	qv := qs.Find(name)
	if qv == nil {
		return false, nil
	}
	if cmdArgs.Argc() == 1 {
		fmt.Printf("%q is %q\n", name, qv.Str)
	} else {
		qs.Set(name, cmdArgs.Args())
	}
	return true, nil
}

// func setCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
// 	defer func() { err = multierror.Prefix(err, "setCmd:") }()

// 	if cmdArgs.Argc() != 3 {
// 		return fmt.Errorf("Usage: %s <name> <value>: set or create variable\n", cmdArgs.Name())
// 	}
// 	qtv.qvs.Set(cmdArgs.Argv(1), cmdArgs.Argv(2))
// 	return nil
// }

func varListCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "varListCmd:") }()

	var r *regexp.Regexp
	if cmdArgs.Argc() > 1 {
		if r, err = regexp.Compile("(?i)" + cmdArgs.Argv(1)); err != nil {
			return err
		}
	}
	vars := qtv.qvs.v.Load().(qvarMap)
	sortedVars := make([]string, 0, len(vars))
	for k := range vars {
		if r != nil && !r.Match([]byte(k)) {
			continue
		}
		sortedVars = append(sortedVars, k)
	}
	sort.Slice(sortedVars, func(i, j int) bool { return sortedVars[i] < sortedVars[j] })
	var b strings.Builder
	for _, v := range sortedVars {
		fmt.Fprintln(&b, v)
	}
	fmt.Println("list of variables:")
	fmt.Print(b.String())
	return nil
}
