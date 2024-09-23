-- We may choose to run this seed multiple times to
-- generate additional row inserts.  There are no constraints
-- on the id, it's just dummy data, so we can re-run it
-- infinite number of times.
create table if not exists tblAssignments(id int, name varchar(80));

insert into tblAssignments(id, name)
values
    (1, 'assignment 1'),
    (2, 'assignment 2'),
    (3, 'assignment 3'),
    (4, 'assignment 4'),
    (5, 'assignment 5')
;