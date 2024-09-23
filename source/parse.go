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
	output := ""

	// inSingleQuoteString := false
	// inDoubleQuoteString := false

	// inSingleLineComment := false
	// inMultilineComment := false

	i := 0
	for i < len(raw) {
		next1 := string(raw[i])
		next2 := string(raw[i])
		if i < len(raw)-1 {
			next2 = string(raw[i : i+2])
		}

		// fmt.Println(i)
		if next2 == "--" {
			// Found a single line comment (or trailing comment), so
			// skip ahead to end of string
			i = findClosingExpressionIndex(raw, i+1, "\n", false)

			// Prefer to preserve the newline -the returned pos goes 1 step past
			// it, so go back 1 as needed.
			//
			// This matches the behavior of multiline, which also does not
			// "swallow" the final trailing newline (instead leaving a blank line).
			if i > 0 && raw[i-1] == '\n' {
				i--
			}

			// Do prefer to strip trailing whitespace before the end-of-line comment
			for len(output) > 0 && output[len(output)-1] == ' ' {
				output = output[0 : len(output)-1]
			}
			// fmt.Printf("remaining: '%s'\n", raw[i:])
		} else if next2 == "/*" {
			// Found multiline comment, skip entire contents
			i = findClosingExpressionIndex(raw, i+1, "*/", false)
			// if i < len(raw) && raw[i] == '\n' {
			// 	i++
			// }
		} else if next1 == "'" || next1 == "\"" {
			end := findClosingExpressionIndex(raw, i+1, next1, true)
			output += raw[i:end]
			i = end
		} else {
			output += next1
			i++
		}
	}

	return strings.TrimSpace(output), nil
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
