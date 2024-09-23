-- We may choose to run this seed multiple times to
-- generate additional row inserts.  There are no constraints
-- on the id, it's just dummy data, so we can re-run it
-- infinite number of times.
create table if not exists tblUsers(id int, name varchar(80));

insert into tblUsers(id, name)
values
    (10, 'some user'),
    (20, 'another user'),
    (30, 'last user')
;