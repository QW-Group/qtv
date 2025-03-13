package qtv

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/hashicorp/go-multierror"
	"github.com/qw-group/qtv-go/pkg/qfs"
	"github.com/rs/zerolog/log"
)

//
// Tokenize console input, expand variables, execute commands,
//

type qCmd struct {
	qtv      *QTV               // Parent object.
	buf      string             // Console input collected here, Exec() process it.
	commands map[string]cmdFunc // Mapping between console command name and handler function.
}

func newQCmd(qtv *QTV) *qCmd {
	cmd := &qCmd{
		qtv: qtv,
	}

	cmd.Register("echo", echoCmd)
	cmd.Register("quit", quitCmd)
	cmd.Register("exec", execCmd)
	cmd.Register("cmdlist", cmdListCmd)
	return cmd
}

type cmdFunc func(qtv *QTV, cmdArgs *qCmdArgs) error

// qCmdArgs is useful wrapping around command name and arguments.
type qCmdArgs struct {
	args []string
}

func newQCmdArgs(args []string) *qCmdArgs {
	cmdArgs := &qCmdArgs{args: args}
	cmdArgs.SetArgv(0, strings.ToLower(cmdArgs.Argv(0)))
	return cmdArgs
}

func (cmdArgs *qCmdArgs) Argc() int {
	return len(cmdArgs.args)
}

func (cmdArgs *qCmdArgs) Argv(arg int) string {
	if arg >= cmdArgs.Argc() {
		return ""
	}
	return cmdArgs.args[arg]
}

// Replace argv with new one, skip it if argv does not exists.
func (cmdArgs *qCmdArgs) SetArgv(arg int, v string) {
	if arg >= cmdArgs.Argc() {
		return
	}
	cmdArgs.args[arg] = v
}

func (cmdArgs *qCmdArgs) ArgvAtoi(arg int) int {
	v := cmdArgs.Argv(arg)
	iv, _ := strconv.Atoi(v)
	return iv
}

func (cmdArgs *qCmdArgs) Args() string {
	if argc := cmdArgs.Argc(); argc <= 1 {
		return ""
	} else {
		return strings.Join(cmdArgs.args[1:argc], " ")
	}
}

func (cmdArgs *qCmdArgs) Name() string {
	return cmdArgs.Argv(0)
}

// Exec scans buf line by line, tokenize it and executes.
func (cmd *qCmd) Exec() error {
	var mErr error
	var s scanner.Scanner
	for cmd.buf != "" {
		// We re-initialize scanner since we could change cmd.buf.
		s.Init(strings.NewReader(cmd.buf))
		s.Mode = scanner.ScanIdents | scanner.ScanChars | scanner.ScanStrings | scanner.ScanRawStrings |
			scanner.ScanComments /* We scan comments but skip them manually since we need detect \n */
		s.Whitespace ^= scanner.GoWhitespace
		s.IsIdentRune = func(ch rune, i int) bool {
			switch ch {
			case '"', '/', ';', '`', '\'':
				return false
			}
			if unicode.IsSpace(ch) {
				return false
			}
			return unicode.IsPrint(ch)
		}

		cmdArgs := newQCmdArgs(cmd.scanLine(&s))
		p := s.Pos()
		if p.Offset == 0 {
			cmd.buf = ""
			mErr = multierror.Append(mErr, fmt.Errorf("could not exec string:%q", cmd.buf))
			break
		}
		cmd.buf = cmd.buf[p.Offset:]

		if err := cmd.execLine(cmdArgs); err != nil {
			mErr = multierror.Append(mErr, err)
		}
	}
	return multierror.Prefix(mErr, "Exec:")
}

// scanLine gets one line from buf and tokenize it.
func (cmd *qCmd) scanLine(s *scanner.Scanner) (tokens []string) {
	var errFound bool
	// Help to guess if we need concatenate tokens
	// since I could not figure out how to configure scanner.
	prevTok := ' '
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		if s.ErrorCount > 0 {
			errFound = true
		}
		if tok == '\n' || tok == ';' {
			break
		}
		ts := s.TokenText()
		if errFound {
			if strings.Contains(ts, "\n") {
				break // Attempt to recover better from errors.
			}
			continue
		}
		if unicode.IsSpace(tok) {
			prevTok = tok
			continue
		}
		if strings.HasPrefix(ts, "//") {
			if strings.HasSuffix(ts, "\n") {
				break // Comment has new line, we should break and execute tokens we collected.
			}
			continue
		}
		if strings.HasPrefix(ts, "/*") {
			// Skip multiline comment even if it contains new line, pretend like nothing was here.
			continue
		}

		ts, unquoted, quote, err := cmdUnquoteToken(ts)
		if err != nil {
			errFound = true
			if strings.Contains(ts, "\n") {
				break // Attempt to recover better from errors.
			}
			continue
		}
		// Do not expand raw string.
		if quote != '`' {
			ts = cmd.qtv.qvs.ExpandString(ts)
		}
		subTokens := []string{ts} // We have at least one sub token.
		// If token was not quoted lets try to split it into sub tokens.
		if !unquoted {
			subTokens = strings.Fields(ts) // If token is empty string or spaces only it will result in zero sub tokens.
			if len(subTokens) == 0 {
				if ts != "" {
					// token is spaces only, lets pretend previous token was space, that prevents wrong concatenation.
					prevTok = ' '
				}
				continue
			}
		}
		concatenate := func(ts string, tok rune) {
			if len(tokens) == 0 || unicode.IsSpace(prevTok) {
				tokens = append(tokens, ts)
			} else {
				// Concatenate consecutive tokens if they not separated by space,
				// e.g.: "123""456" or 123"456" or 123/456.
				tokens[len(tokens)-1] += ts
			}
			prevTok = tok
		}
		switch len(subTokens) {
		case 1:
			concatenate(ts, tok)
		default:
			for _, ts := range subTokens {
				concatenate(ts, ' ') // Do not concatenate subTokens.
			}
		}
	}

	if errFound {
		// Skip tokens if error detected.
		tokens = []string{}
	}
	return tokens
}

// execLine execute one tokenized command.
func (cmd *qCmd) execLine(cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "execLine:") }()

	if cmdArgs.Argc() == 0 {
		return nil
	}
	name := cmdArgs.Name()
	command := cmd.commands[name]
	if command != nil {
		return command(cmd.qtv, cmdArgs)
	}
	isVar, err := cmd.qtv.qvs.CommandIsVar(cmd.qtv, cmdArgs)
	if err != nil {
		return err
	} else if isVar {
		return nil
	}
	return fmt.Errorf("unknown command: %q", name)
}

// Unquote token, token could single-quoted, double-quoted, or backquoted Go string literal,
// single-quoted token should be Go character literal like '\u263a'.
func cmdUnquoteToken(str string) (string, bool, byte, error) {
	if len(str) < 2 {
		return str, false, 0, nil
	}
	quote := str[0]
	switch quote {
	case '"', '`', '\'':
		unquoted, err := strconv.Unquote(str)
		if err != nil {
			return str, false, 0, err
		}
		return unquoted, true, quote, nil
	}
	return str, false, 0, nil
}

// Set sets buf.
func (cmd *qCmd) Set(s string) {
	cmd.buf = s
}

// Append appends data at the end of the buf.
func (cmd *qCmd) Append(s string) {
	if cmd.buf == "" {
		cmd.Set(s)
		return
	}
	cmd.buf = cmd.buf + "\n" + s
}

// Prepend appends data at the start of the buf.
func (cmd *qCmd) Prepend(s string) {
	if cmd.buf == "" {
		cmd.Set(s)
		return
	}
	cmd.buf = s + "\n" + cmd.buf
}

// Register register command.
func (cmd *qCmd) Register(name string, f cmdFunc) {
	if cmd.commands == nil {
		cmd.commands = make(map[string]cmdFunc)
	}
	name = strings.ToLower(name)
	cmd.commands[name] = f
}

func echoCmd(qtv *QTV, cmdArgs *qCmdArgs) error {
	if cmdArgs.Argc() < 2 {
		fmt.Println()
		return nil
	}
	fmt.Println(cmdArgs.Args())
	return nil
}

func quitCmd(qtv *QTV, cmdArgs *qCmdArgs) error {
	// Perform graceful quit if at least one argument provided.
	if cmdArgs.Argc() > 1 {
		return multierror.Prefix(qtv.Stop(), "quit:")
	} else {
		os.Exit(0)
	}
	return nil
}

func execCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "execCmd:") }()

	if cmdArgs.Argc() < 2 {
		return fmt.Errorf("Usage: %s filename.cfg: execute a script file", cmdArgs.Name())
	}

	name := cmdArgs.Argv(1)

	switch filepath.Ext(name) {
	case "":
		name += ".cfg"
	case ".cfg":
	default:
		return errors.New("cfg extension required")
	}

	// For backward compatibility with C version of QTV we also
	// execute config files from the root directory without gamedir, would be nice to get rid of it.
	data, err := qfs.Read("", name)
	if err != nil {
		return err
	}
	qtv.cmd.Prepend(string(data))
	log.Info().Str("exec", name).Msg("")
	return nil
}

func cmdListCmd(qtv *QTV, cmdArgs *qCmdArgs) (err error) {
	defer func() { err = multierror.Prefix(err, "cmdListCmd:") }()

	var r *regexp.Regexp
	if cmdArgs.Argc() > 1 {
		if r, err = regexp.Compile("(?i)" + cmdArgs.Argv(1)); err != nil {
			return err
		}
	}
	cmd := qtv.cmd
	sortedCmds := make([]string, 0, len(cmd.commands))
	for k := range cmd.commands {
		if r != nil && !r.Match([]byte(k)) {
			continue
		}
		sortedCmds = append(sortedCmds, k)
	}
	sort.Slice(sortedCmds, func(i, j int) bool { return sortedCmds[i] < sortedCmds[j] })
	var b strings.Builder
	for _, v := range sortedCmds {
		fmt.Fprintln(&b, v)
	}
	fmt.Println("list of commands:")
	fmt.Print(b.String())
	return nil
}
