lazy sqlite3 lua bindings
-------------------------
This library provides somewhat weird sqlite bindings.

To open a database:

`local db = require('lazylite.db').open('test.db')`

Executing write statements:

count = db:exec("UPDATE foo SET bar=? WHERE baz=?", bar, baz)

Number of rows modified is returned. But what if you need last rowid?

Then you do:

```
count,rowid = db:exec("INSERT INTO foo values(?); SELECT rowid FROM foo", bar, baz)
```

This is a concept seen throughout this library - statements can be chained in
one call.

Subsequent values returned by `exec()` are simply more columns from next
statement, pasted together as one virtual row of results. The first number
returned is number of modified rows for all statements.

Retrieving a single row of data with named columns
==================================================

`local row = db:col("SELECT foo, bar FROM baz")`

Will fill in `row.foo` and `row.bar`. Again, you can execute multiple statements:

```
local row = db:col("SELECT foo, bar FROM baz; SELECT duh FROM bleh")
```

Will return `row.foo`, `row.bar` and `row.duh`. You might want to use AS to rename
colliding fields, otherwise those get silently overwritten.

Finally, already existing table can be supplied where fields will be filled
out, like so:

db:tcol(row, "SELECT foo, bar FROM baz; SELECT duh FROM bleh")

(same table is also returned)

This is useful if you need to modify object you cant easily change reference
to (typically 'self'). You can also inspect values as they come in via
`__newindex` on this table.

Retrieving a single row of data with columns as return values
=============================================================

`local foo, bar = db:row("SELECT foo, bar FROM baz")`
`local foo, bar, duh = db:row("SELECT foo, bar FROM baz; SELECT duh FROM bleh")`

This is just a short hand of the :col() method above, so you can assign columns
to variables directly.

Retrieving multiple rows with named columns
===========================================
for st_idx, tab in db:cols("SELECT foo FROM bar; SELECT boo FROM baz") do
	print(st_idx, tab.foo, tab.boo)
end

Statement chaining works differently when fetching multiple rows -
each clause will be executed sequentially - once all rows are finished in
one statement, the st_idx is bumped and rows are returned for next statement
and so on.
If 'bar' and 'baz' tables contain 2 rows each, the output of the above will be:

1 	foo1 	nil
1 	foo2 	nil
2 	nil 	boo1
2 	nil 	boo2

Typically, you can chain multiple tables by carefully renaming the fields,
like so:
for st_idx, tab in db:cols("SELECT foo FROM bar; SELECT boo As foo FROM baz") do
	print(st_idx, tab.foo, tab.boo)
end

And then you get:
1 	foo1
1 	foo2
2 	boo1
2 	boo2

Retrieving multiple rows with columns as return values
======================================================
Finally, for direct access to columns, you use:

for st_idx, foo_or_boo in db:rows("SELECT foo FROM bar; SELECT boo FROM baz") do
	print(st_idx, foo_or_boo)
end

The statement index works exactly same as with db:cols(). The most common
usage is to simply ignore the statement index and just use:

```
for _, blah in db:rows("SELECT foo FROM bar; SELECT boo FROM baz") do
	print(blah)
end
```

This method is particularly efficient as no new tables will be polluting
the GC.

Named variables
===============
All functions also accept named arguments, like so:
```
tab = { fieldfoo = "foo", tabfoo = "bar" }
for _, foo, bar in db:rows("SELECT $fieldfoo FROM $tabfoo", tab) do
  ...
end
```

Standard sqlite prefixes - $:#@ can be used.

You can also mix named and unnamed variables:

```
tab = {v1 = 1, v2 = 2}
db:exec("INSERT INTO tab(?,?) values($v1,$v1)", tab, 'f1', 'f2)
```

evaluates (but with proper escaping) to:

```
db:exec("INSERT INTO tab(f1,f2) values("..tab.v1..","..tab.v2")")
```

Notice that table must be always first, so the parser knows
where to look for symbolic names whenever those are encountered. All numbered
? values are then scanned positionally, ignoring the table and named arguments.
Named and unnamed variables live in "separate worlds" so to speak.

FAQ
===

Why all this statement chaining madness?
========================================
To avoid slow joins for trivial "look all over the place" sort of queries.
Simple queries are streamed on the go with sqlite, whereas joins have to
wait for the whole result to complete only to throw away most of it - resulting
much heavier disk IO.

What about statement handles, cursors?
======================================
This is all handled automatically behind the scenes, cached and garbage
collected. Just make sure to never keep a reference to the hidden variables
returned by loop iterators when fetching rows (ie don't save in a variable, but
always use direct notation with the for loop).

Oh, and don't construct query format strings from ever changing values.
Those are cache keys, and never collected - it would leak memory. Use statement
variables for variation of the query, tables can be specified too.

I want bunch of rows in a table
===============================
Don't do this, it is slow and pollutes heap. If you absolutely must, write
a simple wrapper where generator saves results in a table.
