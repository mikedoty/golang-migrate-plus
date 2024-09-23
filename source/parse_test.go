package source

import (
	"testing"
)

func TestParse(t *testing.T) {
	tt := []struct {
		name            string
		expectErr       error
		expectMigration *Migration
	}{
		{
			name:      "1_foobar.up.sql",
			expectErr: nil,
			expectMigration: &Migration{
				Version:    1,
				Identifier: "foobar",
				Direction:  Up,
				Raw:        "1_foobar.up.sql",
			},
		},
		{
			name:      "1_foobar.down.sql",
			expectErr: nil,
			expectMigration: &Migration{
				Version:    1,
				Identifier: "foobar",
				Direction:  Down,
				Raw:        "1_foobar.down.sql",
			},
		},
		{
			name:      "1_f-o_ob+ar.up.sql",
			expectErr: nil,
			expectMigration: &Migration{
				Version:    1,
				Identifier: "f-o_ob+ar",
				Direction:  Up,
				Raw:        "1_f-o_ob+ar.up.sql",
			},
		},
		{
			name:      "1485385885_foobar.up.sql",
			expectErr: nil,
			expectMigration: &Migration{
				Version:    1485385885,
				Identifier: "foobar",
				Direction:  Up,
				Raw:        "1485385885_foobar.up.sql",
			},
		},
		{
			name:      "20170412214116_date_foobar.up.sql",
			expectErr: nil,
			expectMigration: &Migration{
				Version:    20170412214116,
				Identifier: "date_foobar",
				Direction:  Up,
				Raw:        "20170412214116_date_foobar.up.sql",
			},
		},
		{
			name:            "-1_foobar.up.sql",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
		{
			name:            "foobar.up.sql",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
		{
			name:            "1.up.sql",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
		{
			name:            "1_foobar.sql",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
		{
			name:            "1_foobar.up",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
		{
			name:            "1_foobar.down",
			expectErr:       ErrParse,
			expectMigration: nil,
		},
	}

	for i, v := range tt {
		f, err := Parse(v.name)

		if err != v.expectErr {
			t.Errorf("expected %v, got %v, in %v", v.expectErr, err, i)
		}

		if v.expectMigration != nil && *f != *v.expectMigration {
			t.Errorf("expected %+v, got %+v, in %v", *v.expectMigration, *f, i)
		}
	}
}

func TestStripSqlComments(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sql      string
		expected string
	}{
		{
			"strip trailing single line comment",
			`select 1; -- this is a comment`,
			`select 1;`,
		},
		{
			"don't touch -- within single quote string",
			`select '-- this is a comment'; select 2;`,
			`select '-- this is a comment'; select 2;`,
		},
		{
			"do remove trailing comment while preserving -- within single quote",
			`select '-- this is a comment'; select 3;    -- trailing comment should be removed!`,
			`select '-- this is a comment'; select 3;`,
		},
		{
			"don't touch -- within double quote string",
			`select "-- this is a comment"; select 2;`,
			`select "-- this is a comment"; select 2;`,
		},
		{
			"do remove trailing comment while preserving -- within double quote",
			`select "-- this is a comment"; select 3;    -- trailing comment should be removed!`,
			`select "-- this is a comment"; select 3;`,
		},
		{
			"strip same-line multistring comments",
			"select /* useless comment */ 1 as /* i love aliases? */ value;",
			"select  1 as  value;",
		},
		{
			"strip same-line multistring comments and also trailing -- comment",
			"select /* useless comment */ 1 as /* i love aliases? */ value; -- more comments here",
			"select  1 as  value;",
		},
		{
			"don't touch mulitline syntax that appears within single quote string",
			`select '/* this is an example of a comment */' as tutorial_name; -- strip me at the end tho`,
			`select '/* this is an example of a comment */' as tutorial_name;`,
		},
		{
			"don't touch mulitline syntax that appears within double quote string",
			`select "/* this is an example of a comment */" as tutorial_name; -- strip me at the end tho`,
			`select "/* this is an example of a comment */" as tutorial_name;`,
		},
		{
			"ignore single quotes within a double quote string",
			`select "don't worry i'm tryin' lots'a 'single quotes' with'n this string" as value; -- comment`,
			`select "don't worry i'm tryin' lots'a 'single quotes' with'n this string" as value;`,
		},
		{
			"ignore double quotes within a single quote string",
			`select 'you "really" like "quotes" do you "not?"' as value; -- comment`,
			`select 'you "really" like "quotes" do you "not?"' as value;`,
		},
		{
			"ignore backslash escaped single quotes in single quote string",
			`select 'don\'t worry i\'m tryin\' lots\'a \'single quotes\' with\'n this string' as value; -- comment`,
			`select 'don\'t worry i\'m tryin\' lots\'a \'single quotes\' with\'n this string' as value;`,
		},
		{
			"ignore double single quote escaped single quotes in single quote string",
			`select 'don''t worry -- keep i''m tryin'' lots''a /* ignore this */ ''single quotes'' with''n this string' as value; -- comment`,
			`select 'don''t worry -- keep i''m tryin'' lots''a /* ignore this */ ''single quotes'' with''n this string' as value;`,
		},
		{
			"strip true multiline comment",
			`
/*
This is a sql file
*/
select 1;`,
			`select 1;`,
		},
		{
			"strip multiple multiline comments",
			`
/*
select the first value
      here....
*/
select 1;

/* i want to
       select another
			value here...
				mixing tabs and spaces...
*/
select 2;`,
			`select 1;


select 2;`,
		},
		{
			"strip mixes of true multiline comments and trailing comments",
			`
/*
This is a sql file
*/
select 1; -- the first value
/*
more dumb comments
*/
select /* midline comment */2 as val2;
-- ignore me
-- i'll be another blank line
select 3;`,
			`select 1;

select 2 as val2;


select 3;`,
		},
		{
			"useless whitespace at start/end gets trimmed after comment replaces to allow runInTransaction to return TRUE",
			`/* This is a comment
at the top of the file
*/
begin;

create table x;

commit; -- ok
-- END OF FILE COMMENT`,
			`begin;

create table x;

commit;`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			val, _ := StripSqlComments(tc.sql)
			if val != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, val)
			}
		})
	}
}
