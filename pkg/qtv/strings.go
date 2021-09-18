package qtv

import (
	"math/rand"
	"strconv"
	"strings"
	"unicode"
)

//
// Misc string utils.
//

type tokenizerResult struct {
	tokens []string
	args   string
}

func (tr *tokenizerResult) Argc() int {
	return len(tr.tokens)
}

func (tr *tokenizerResult) Argv(arg int) string {
	if arg >= tr.Argc() {
		return ""
	}
	return tr.tokens[arg]
}

func (tr *tokenizerResult) SetArgv(arg int, v string) {
	if arg >= tr.Argc() {
		return
	}
	tr.tokens[arg] = v
}

func (tr *tokenizerResult) ArgvAtoi(arg int) int {
	v := tr.Argv(arg)
	iv, _ := strconv.Atoi(v)
	return iv
}

func (tr *tokenizerResult) Args() string {
	return tr.args
}

// Basic string tokenization, does not support UTF-8 since quake does not support it either.
// Returns slice of tokens, args and position where we stopped parsing input string.
// For example if input is `cmd arg1 "arg2 still-arg2" arg3` then after parsing
// tokens = ["cmd", "arg1", "arg2 still-arg2", "arg3"]
// args = `arg1 "arg2 still-arg2" arg3`
func tokenizeString(s string) (tr tokenizerResult, i int) {
	var (
		tokens []string
	)

	for ; i < len(s) && s[i] == '\n'; i++ {
		// Skip leading new lines.
	}

	argsStart := 0

	for i < len(s) {
		if s[i] == '\n' {
			i++
			break // New line separates commands in the buffer.
		}

		if unicode.IsSpace(rune(s[i])) {
			i++ // Skip spaces.
			continue
		}

		// If we parsed first token, then remember args start position.
		// args would contain comments if they present.
		if len(tokens) == 1 && argsStart == 0 {
			argsStart = i
		}

		// Skip comments.
		if s[i] == '/' && i+1 < len(s) {
			if s[i+1] == '/' {
				for ; i < len(s) && s[i] != '\n'; i++ {
					// Searching for new line.
				}
				continue
			} else if s[i+1] == '*' {
				for i += 2; i+1 < len(s) && (s[i] != '*' || s[i+1] != '/'); i++ {
					// Searching for comment closing sequence.
				}
				i += 2
				continue
			}
		}

		// Handle quoted strings.
		if s[i] == '"' {
			i++
			start := i
			for ; i < len(s); i++ {
				if s[i] == '"' {
					break
				}
			}
			tokens = append(tokens, s[start:i])
			i++
			continue
		}
		// Handle words.
		start := i
		for ; i < len(s); i++ {
			if unicode.IsSpace(rune(s[i])) {
				break
			}
		}
		tokens = append(tokens, s[start:i])
	}

	if i > len(s) {
		i = len(s)
	}

	tr.tokens = tokens
	if argsStart > 0 {
		tr.args = s[argsStart:i]
	}

	return tr, i
}

func unquote(s string) string {
	if strings.HasPrefix(s, "\"") {
		s = s[1:]
	}
	if strings.HasSuffix(s, "\"") {
		s = s[0 : len(s)-1]
	}
	return s
}

// Replace old bytes with new bytes in string. Does not honor UTF-8.
func replaceAll(s string, old byte, new byte) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		if b[i] == old {
			b[i] = new
		}
	}
	return string(b)
}

func redText(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		if b[i] > 32 && b[i] < 128 {
			b[i] |= 1 << 7
		}
	}
	return string(b)
}

var (
	qfontTable = [256]byte{
		0, '#', '#', '#', '#', '.', '#', '#',
		'#', 9, 10, '#', ' ', 13, '.', '.',
		'[', ']', '0', '1', '2', '3', '4', '5',
		'6', '7', '8', '9', '.', '<', '=', '>',
		' ', '!', '"', '#', '$', '%', '&', '\'',
		'(', ')', '*', '+', ',', '-', '.', '/',
		'0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', ':', ';', '<', '=', '>', '?',
		'@', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
		'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
		'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
		'X', 'Y', 'Z', '[', '\\', ']', '^', '_',
		'`', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
		'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
		'x', 'y', 'z', '{', '|', '}', '~', '<',

		'<', '=', '>', '#', '#', '.', '#', '#',
		'#', '#', ' ', '#', ' ', '>', '.', '.',
		'[', ']', '0', '1', '2', '3', '4', '5',
		'6', '7', '8', '9', '.', '<', '=', '>',
		' ', '!', '"', '#', '$', '%', '&', '\'',
		'(', ')', '*', '+', ',', '-', '.', '/',
		'0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', ':', ';', '<', '=', '>', '?',
		'@', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
		'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
		'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
		'X', 'Y', 'Z', '[', '\\', ']', '^', '_',
		'`', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
		'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
		'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
		'x', 'y', 'z', '{', '|', '}', '~', '<',
	}
)

func normalizeText(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		out[i] = qfontTable[s[i]]
	}
	return string(out)
}

func randomString(size int) string {
	var b strings.Builder
	for i := 0; i < size; i++ {
		b.WriteByte('A' + byte(rand.Uint32())%('Z'-'A'+1))
	}
	return b.String()
}

func isEnabledFromBool(isEnabled bool) string {
	if isEnabled {
		return "enabled"
	} else {
		return "disabled"
	}
}
