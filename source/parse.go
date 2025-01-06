package source

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrParse = fmt.Errorf("no match")
)

var (
	DefaultParse = Parse
	DefaultRegex = Regex
)

// Regex matches the following pattern:
//
//	123_name.up.ext
//	123_name.down.ext
var Regex = regexp.MustCompile(`^([0-9]+)_(.*)\.(` + string(Down) + `|` + string(Up) + `)\.(.*)$`)

// Parse returns Migration for matching Regex pattern.
func Parse(raw string) (*Migration, error) {
	m := Regex.FindStringSubmatch(raw)
	if len(m) == 5 {
		versionUint64, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, err
		}
		return &Migration{
			Version:    uint(versionUint64),
			Identifier: m[2],
			Direction:  Direction(m[3]),
			Raw:        raw,
		}, nil
	}
	return nil, ErrParse
}

func StripSqlComments(raw string) (string, error) {
	i := 0
	maxLength := len(raw)
	for i < maxLength {
		if raw[i] == '\'' || raw[i] == '"' {
			end := findClosingExpressionIndex(raw, i+1, string(raw[i]), true)
			i = end
		} else if i < maxLength-1 && raw[i] == '-' && raw[i+1] == '-' {
			// Found a single line comment (or trailing comment), so
			// skip ahead to end of string
			end := findClosingExpressionIndex(raw, i+1, "\n", false)

			// Prefer to preserve the newline - the returned pos goes 1 step past
			// it, so go back 1 as needed.
			//
			// This matches the behavior of multiline, which also does not
			// "swallow" the final trailing newline (instead leaving a blank line).
			if end > 0 && raw[end-1] == '\n' {
				if i > 0 && raw[i-1] == '\n' {
					// If single-line comment is on its own line,
					// don't keep the newline after all...
				} else {
					end--
				}
			}

			// Prefer to strip trailing whitespace before the end-of-line comment (which we're removing)
			// This avoids "trailing whitespace" after we remove the comment text
			start := i
			for start > 0 && raw[start-1] == ' ' {
				start--
			}

			raw = raw[0:start] + raw[end:]
			maxLength = len(raw)
		} else if i < maxLength-1 && raw[i] == '/' && raw[i+1] == '*' {
			// Found multiline comment, skip entire contents
			end := findClosingExpressionIndex(raw, i+1, "*/", false)
			start := i

			raw = raw[0:start] + raw[end:]
			maxLength = len(raw)
		} else {
			i++
		}
	}

	return strings.TrimSpace(raw), nil
}

func findClosingExpressionIndex(s string, posStart int, expr string, isEscaped bool) int {
	cursor := posStart

	sLength := len(s)
	exprLength := len(expr)

	for cursor < sLength-exprLength {
		if s[cursor:cursor+exprLength] == expr {
			if !isEscaped || s[cursor-1] != '\\' {
				return cursor + exprLength
			}
		}
		cursor++
	}

	return cursor + exprLength
}
