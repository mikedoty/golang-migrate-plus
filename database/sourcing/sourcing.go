package sourcing

import (
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/mikedoty/golang-migrate-plus/source"
)

func ImportSourcing(sourceDrv source.Driver, sqlBytes []byte) ([]byte, error) {
	if sourceDrv == nil {
		return sqlBytes, nil
	}

	sqlString := string(sqlBytes)

	// source path can be enclosed in either single or double quotes
	// Be "generous" and let users make minor whitespace mistakes/irregularities,
	// and the trailing semicolon is allowed to be forgotten for source directive
	rx := regexp.MustCompile(`(?m)^\s*source\s*(['"])(.*)(['"])\s*;?\s*$`)
	arrGroupPts := rx.FindAllStringSubmatchIndex(sqlString, -1)

	// Loop in reverse, so that as we perform string replaces, we
	// don't change the start:end string pos for subsequent matches
	// as we're looping through.
	for i := len(arrGroupPts) - 1; i >= 0; i-- {
		groupPts := arrGroupPts[i]

		openQuote := sqlString[groupPts[2]:groupPts[3]]
		closeQuote := sqlString[groupPts[6]:groupPts[7]]

		if openQuote != closeQuote {
			continue
		}

		relativeFilepath := sqlString[groupPts[4]:groupPts[5]]

		r, err := sourceDrv.ReadAny(relativeFilepath)
		if err != nil {
			return nil, err
		}

		bytes, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}

		start := groupPts[0]
		end := groupPts[1]

		sqlString = sqlString[0:start] + string(bytes) + sqlString[end:]
	}

	return []byte(sqlString), nil
}

func GatherExecs(sourceDrv source.Driver, sqlBytes []byte) ([][]byte, bool, error) {
	sqlString := string(sqlBytes)
	sqlString, _ = source.StripSqlComments(sqlString)
	sqlString = strings.TrimSpace(sqlString)

	runInTransaction := (strings.HasPrefix(strings.ToLower(sqlString), "begin;") &&
		strings.HasSuffix(strings.ToLower(sqlString), "commit;"))

	if runInTransaction {
		// Remove "BEGIN;" and "COMMIT;"
		sqlString = sqlString[6 : len(sqlString)-7]
	}

	// exec path can be enclosed in either single or double quotes
	// Be "generous" and let users make minor whitespace mistakes/irregularities,
	// and the trailing semicolon is allowed to be forgotten for source directive
	rx := regexp.MustCompile(`(?m)^\s*exec\s*(['"])(.*?\.sql)(['"])\s*;?\s*$`)
	arrGroupPts := rx.FindAllStringSubmatchIndex(sqlString, -1)

	results := [][]byte{}

	if len(arrGroupPts) == 0 {
		// No exec statements found, so return the sqlString and
		// transaction flag.
		sqlString = strings.TrimSpace(sqlString)
		if len(sqlString) > 0 {
			results = append(results, []byte(sqlString))
		}

		return results, runInTransaction, nil
	}

	cursorPos := 0
	for {
		// If there is sql before the next detected exec statement,
		// insert it as the first []byte result
		leadingSqlString := strings.TrimSpace(sqlString[cursorPos:arrGroupPts[0][0]])
		if len(leadingSqlString) > 0 {
			results = append(results, []byte(leadingSqlString))
		}

		// Always take the first item; we pop the first item off of the array
		// when we're done with each loop
		groupPts := arrGroupPts[0]

		openQuote := sqlString[groupPts[2]:groupPts[3]]
		closeQuote := sqlString[groupPts[6]:groupPts[7]]

		if openQuote != closeQuote {
			return nil, false, errors.New("mismatched quote characters in relative filepath")
		}

		relativeFilepath := sqlString[groupPts[4]:groupPts[5]]

		r, err := sourceDrv.ReadAny(relativeFilepath)
		if err != nil {
			return nil, false, err
		}

		bytes, err := io.ReadAll(r)
		if err != nil {
			return nil, false, err
		}

		asString := string(bytes)
		asString = strings.TrimSpace(asString)

		// Add the sql for the exec filepath as the next result
		if len(asString) > 0 {
			results = append(results, []byte(asString))
		}

		// Remove the regex result; keep going until we process all of them
		if len(arrGroupPts) == 1 {
			// If we just processed the last regex result, then break
			// loop so we can check for any trailing (i.e. final) sql statement
			break
		} else {
			// Otherwise, pop the first item in the regex matches
			// and keep going in linear order...
			cursorPos = arrGroupPts[0][1]
			arrGroupPts = arrGroupPts[1:]
		}
	}

	// Check for any final trailing sql statement
	// (Unlike the "before" sql which happens in the loop in case there
	// is "orphaned" sql between exec calls, this check happens outside of
	// the loop as it's the end of the file contents...
	trailingSqlString := strings.TrimSpace(sqlString[arrGroupPts[0][1]:])
	if len(trailingSqlString) > 0 {
		results = append(results, []byte(trailingSqlString))
	}

	return results, runInTransaction, nil
}
