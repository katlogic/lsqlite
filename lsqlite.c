#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sqlite3.h>
#include <assert.h>

typedef	struct {
	sqlite3 *db;
	int changes;
	char name[1];
} sqlite_db;

typedef struct {
	int nst, npars;
	sqlite3_stmt *st[0];
} sqlite_st;

#define SQLITEDB "sqlite.db*"
#define SQLITEST "sqlite.st*"
#define SQLITEUV "sqlite.uv*"
#define SQLITEQS "sqlite.qs*"
#define MAX_ST 128

#define SLUV_ST lua_upvalueindex(1) /* Metatable of statement entries. */
#define SLUV_DB lua_upvalueindex(2) /* Metatable of DB object entries. */
#define SLUV_QS lua_upvalueindex(3) /* Table of stmt->qs mappings. */
#define SLUV_ROWS lua_upvalueindex(4) /* db:rows() aux. */
#define SLUV_COLS lua_upvalueindex(5) /* db:cols() aux. */

/* Lua 5.[12] compat */
#if LUA_VERSION_NUM < 503
static void lua_setuservalue(lua_State *L, int idx)
{
	lua_pushvalue(L, idx); /* +1 target */
	lua_pushliteral(L, SQLITEUV); 
	lua_rawget(L, LUA_REGISTRYINDEX); /* +2 tab */
	lua_pushvalue(L, -2);
	lua_pushvalue(L, -4); /* top stack caller */
	lua_rawset(L, -3);
	lua_pop(L, 3);
}
static void lua_getuservalue(lua_State *L, int idx)
{
	lua_pushvalue(L, idx);
	lua_pushliteral(L, SQLITEUV);
	lua_rawget(L, LUA_REGISTRYINDEX);
	lua_pushvalue(L, -2);
	lua_rawget(L, -2);
	lua_replace(L, -3);
	lua_pop(L, 1);
}
#if LUA_VERSION_NUM < 502
#define luaL_setfuncs(L, reg, nup) luaL_openlib(L, NULL, reg, nup)
#endif
#endif

/* Load uservalue of given metatable type */
static void *sl_uvdata(lua_State *L, int idx, int uvidx)
{
	void *data = lua_touserdata(L, idx);
	if (!data)
		return data;
	if (!lua_getmetatable(L, idx))
		return NULL;
	if (!lua_rawequal(L, -1, uvidx))
		data = NULL;
	lua_pop(L, 1);
	return data;
}

/* Load statement object */
static	inline	sqlite_st *sl_tost(lua_State *L, int idx, int chk)
{
	sqlite_st *st = sl_uvdata(L, idx, SLUV_ST);
	if (!st || (chk && st->nst == -1)) {
		luaL_argerror(L, idx, "invalid sqlite statement (stale ref?)");
	}
	return st;
}


/* Load DB object */
static	inline	sqlite_db *sl_todb(lua_State *L, int idx, int chk)
{
	sqlite_db *db = sl_uvdata(L, idx, SLUV_DB);
	if (!db || (chk && !db->db))
		luaL_argerror(L, idx, "invalid sqlite database (stale ref?)");
	return db;
}

/* 
 * Statements caching works as follows:
 *
 * 1. when statement emerges in Lua world (that is, exposed via iterator),
 *    it is uncached, otherwise it is cached.
 *
 * 2. uncached statements have their underlying database set as uservalue, they
 *    also get a metatable attached, where sl_cacheback() is __gc.
 *
 * 3. cached statements are stored in a cache table for each db object. db
 *    objects have that table as uservalue. cached statements don't have __gc
 *    set. their uservalue forms a linked list to next cached statement under
 *    same query string. it looks like this:
 *
 *    db->uservalue[qs] = { stmt->uservalue { stmt->uservalue { ... }}
 *
 * All of this allows us to cache statements with little to no gc pressure.
 */
static void nuke_stmt(lua_State *L, sqlite_st *st)
{
	int i;
	for (i = 0; i < st->nst; i++)
		sqlite3_finalize(st->st[i]);
	st->nst = -1;
	lua_pushlightuserdata(L, st);
	lua_pushnil(L);
	lua_rawset(L, SLUV_QS);
}

static int sl_cacheback(lua_State *L)
{
	sqlite_st *st = sl_tost(L, 1, 0);
	sqlite_db *db;
	int i;
	lua_settop(L,1);
	lua_getuservalue(L, 1); /* load db, stack = 2 */
	db = sl_todb(L, 2, 0);

	/* Meanwhile the database closed */
	if (!db->db) {
		/* So just clear statements. */
		nuke_stmt(L, st);
		return 0;
	}

	lua_getuservalue(L, 2); /* load db's cache table, stack = 3 */
	assert(lua_istable(L, -1));
	assert(lua_gettop(L) == 3);

	/* Lookup the query string */
	lua_pushlightuserdata(L, st);
	lua_rawget(L, SLUV_QS); /* stack = 4 */
	assert(lua_isstring(L, -1));

	/* And load cache chain */
	lua_pushvalue(L, -1);
	lua_rawget(L, 3);

	/* Chain that result into our uservalue. */
	lua_setuservalue(L, 1);

	/* And set it as cache entry. */
	lua_pushvalue(L, 4); /* qs */
	lua_pushvalue(L, 1); /* our entry */
	lua_rawset(L, 3);

	/* Kill metatable of this statement .*/
	lua_pushnil(L);
	lua_setmetatable(L, 1);

	/* So that sqlite frees unused memory. */
	for (i = 0; i < st->nst; i++) {
		sqlite3_clear_bindings(st->st[i]);
		sqlite3_reset(st->st[i]);
	}
	return 0;
}

/* Find or create statement. Returns statement on top, cache table just below. */
static	sqlite_st *do_prepare(lua_State *L, int dbidx, int qsidx, int uncache,
		sqlite_db **db)
{
	size_t	qsl;
	const	char *qs, *qsp;
	sqlite3_stmt *stmts[MAX_ST];
	int	count = 0;
	int 	npars = 0;
	sqlite_st *st;

	/* Get cache. */
	*db = sl_todb(L, dbidx, 1);
	lua_getuservalue(L, dbidx);
	assert(lua_istable(L, -1));

	/* Lookup query string. */
	lua_pushvalue(L, qsidx);
	lua_rawget(L, -2);

	/* Found cached? */
	if (!lua_isnil(L, -1)) {
		if (uncache) {
			/* Unlink the entry. */
			lua_pushvalue(L, qsidx);
			lua_getuservalue(L, -2); /* Next in chain. */
			lua_rawset(L, -4);
		}
		st = lua_touserdata(L, -1);
	} else {
		lua_pop(L, 1);
		/* Build new. */
		qs = luaL_checklstring(L, 2, &qsl);
		for (qsp = qs; *qsp; ) {
			sqlite3_stmt *s = NULL;
			int err;

			if (count >= MAX_ST) {
				int i;
				for (i = 0; i < count; i++)
					sqlite3_finalize(stmts[i]);
				luaL_error(L, "Too many statements (max %d)", MAX_ST);
			}
			err = sqlite3_prepare_v2((*db)->db, qsp, qs + qsl - qsp,
					stmts + count++, &qsp);
			if (err != SQLITE_OK) {
				int i;
				for (i = 0; i < count; i++)
					sqlite3_finalize(stmts[i]);
				luaL_error(L, "%s", sqlite3_errmsg((*db)->db));
			}
			npars += sqlite3_bind_parameter_count(s);
		}
		st = lua_newuserdata(L, sizeof(*st) + count * sizeof(st->st[0]));
		memset(st, 0, sizeof(*st) + count * sizeof(st->st[0]));
		memcpy(st->st, stmts, count*sizeof(st->st[0]));
		st->npars = npars;
		st->nst = count;

		/* Remember for UD->QS lookup */
		lua_pushlightuserdata(L, st);
		lua_pushvalue(L, 2);
		lua_rawset(L, SLUV_QS);
	}
	if (!uncache) {
		/* Link in the entry. */
		lua_pushvalue(L, qsidx);
		lua_pushvalue(L, -2);
		assert(lua_istable(L, -4));
		lua_rawset(L, -4);
	} else {
		/* Set its metatable if it gets uncached. */
		lua_pushvalue(L, SLUV_ST);
		lua_setmetatable(L, -2);
		/* And uservalue to db */
		lua_pushvalue(L, dbidx);
		lua_setuservalue(L, -2);
	}
	return st;
}

/* Bind one statement. Return number of parameters bound. */
static	int do_bind(lua_State *L, sqlite3 *db, sqlite3_stmt *st,
		int pars, int count, int names)
{
	int err,i,j,bn = sqlite3_bind_parameter_count(st);
	i = 0;

	if ((err = sqlite3_reset(st)) != SQLITE_OK)
		goto out;
	for (i = 1, j = pars; i <= bn; i++) {
		const char *bname;
		int tj = j;
		if (names && (bname = sqlite3_bind_parameter_name(st, i))) {
			lua_getfield(L, names, bname + 1);
			tj = -1;
		} else {
			if (j >= pars+count)
				break;
			j++;
		}
		if (lua_isboolean(L, tj)) {
			err = sqlite3_bind_int(st, i, lua_toboolean(L, tj));
#if LUA_VERSION_NUM >= 503
		} else if (lua_isinteger(L, tj)) {
			err = sqlite3_bind_int64(st, i, lua_tointeger(L, tj));
#endif
		} else  if (lua_isnumber(L, tj)) {
			err = sqlite3_bind_double(st, i, lua_tonumber(L, tj));
		} else if (lua_isnil(L, tj)) {
			err = sqlite3_bind_null(st, i);
		} else {
			size_t sl;
			const char *s = luaL_checklstring(L, tj, &sl);
			err = sqlite3_bind_text(st, i, s, sl, NULL);
		}
		if (tj == -1)
			lua_pop(L, 1);
		if (err != SQLITE_OK)
			goto out;
	}
	for (; i <= bn; i++) {
		j++;
		if ((err = sqlite3_bind_null(st, i))!=SQLITE_OK)
			break;
	}
out:;
	luaL_argcheck(L, (err == SQLITE_OK), j-1, sqlite3_errmsg(db));
	return bn;
}

/* Pull statement from idx, bind arguments. Must have clean (call) stack top. */
static sqlite_st *do_binds(lua_State *L, int dbidx, int qsidx, int uncache,
		sqlite_db **db)
{
	int avail = lua_gettop(L) - qsidx;
	sqlite_st *st = do_prepare(L, dbidx, qsidx, uncache, db);
	int i, parpos = qsidx+1;
	int names = 0;
	if (avail && lua_istable(L, parpos)) {
		avail--;
		names = parpos++;
	}
	for (i = 0; i < st->nst; i++) {
		int got = do_bind(L, (*db)->db, st->st[i], parpos,
				avail<0?0:avail, names);
		avail -= got;
		parpos += got;
	}
	return st;
}

/* Push one row column value at `idx` to the stack. */
static	void push_field(lua_State *L, struct sqlite3_stmt *row, int idx)
{
	switch (sqlite3_column_type(row, idx)) {
		case SQLITE_INTEGER:
			lua_pushinteger(L, sqlite3_column_int64(row, idx));
			return;
		case SQLITE_FLOAT:
			lua_pushnumber(L, sqlite3_column_double(row, idx));
			return;
		case SQLITE_TEXT:
		case SQLITE_BLOB: {
			const char *p = sqlite3_column_blob(row, idx);
			if (!p) lua_pushnil(L); else
				lua_pushlstring(L, p, sqlite3_column_bytes(row, idx));
			return;
		}
		case SQLITE_NULL:
			lua_pushnil(L);
			return;
		default:
			abort();
	}
}

/* Push all columns of a row on the stack. Returns number of columns. */
static int push_fields(lua_State *L, sqlite3_stmt *row)
{
	int i, n = sqlite3_data_count(row);
	for (i = 0; i < n; i++)
		push_field(L, row, i);
	return n;
}

/* Set columns as named keys/values in table `tab`. */
static void set_fields(lua_State *L, sqlite3_stmt *row, int tab)
{
	int i, n = sqlite3_data_count(row);
	for (i = 0; i < n; i++) {
		push_field(L, row, i);
		lua_setfield(L, tab, sqlite3_column_name(row, i));
	}
}

/* Do one row step, accumulate columns on stack (concatenate rows). */
static int row_step(lua_State *L, sqlite_st *st, sqlite_db *db)
{
	int i, total = 0;
	for (i = 0; i < st->nst; i++) {
		int err = sqlite3_step(st->st[i]);
		if (err == SQLITE_DONE) {
			sqlite3_reset(st->st[i]);
		} else if (err == SQLITE_ROW) {
			total += push_fields(L, st->st[i]);
			sqlite3_reset(st->st[i]);
		} else {
			luaL_error(L, "while executing statement #%d: %s", i,
					sqlite3_errmsg(db->db));
		}
	}
	return total;
}

/* changed, col1, col2, .. = db:exec(stmts) */
static	int sl_exec(lua_State *L)
{
	sqlite_db *db;
	sqlite_st *st = do_binds(L, 1, 2, 0, &db);
	int total, bchanges = sqlite3_total_changes(db->db);
	total = row_step(L, st, db);
	lua_pushinteger(L, sqlite3_total_changes(db->db) - bchanges);
	lua_replace(L, -total-2);
	return total+1;
}

/* col1, col2 .. = db:row(stmts) */
static	int sl_row(lua_State *L)
{
	sqlite_db *db;
	sqlite_st *st = do_binds(L, 1, 2, 0, &db);
	return row_step(L, st, db);
}

/* Perform one step and collect all columns of a row as k/vs into table `tab`. */
static void col_step(lua_State *L, sqlite_st *st, sqlite_db *db, int ttab)
{
	int i;
	for (i = 0; i < st->nst; i++) {
		int err = sqlite3_step(st->st[i]);
		if (err == SQLITE_DONE) {
			sqlite3_reset(st->st[i]);
		} else if (err == SQLITE_ROW) {
			set_fields(L, st->st[i], ttab);
			sqlite3_reset(st->st[i]);
		} else {
			luaL_error(L, "while executing statement #%d: %s", i,
					sqlite3_errmsg(db->db));
		}
	}
}

/* {tab.colname, tab.colname2..} = db:col(stmts) */
static int sl_col(lua_State *L)
{
	sqlite_db *db;
	sqlite_st *st = do_binds(L, 1, 2, 0, &db);
	lua_newtable(L);
	col_step(L, st, db, lua_gettop(L));
	return 1;
}

/* {tab.colname, tab.colname2..} = db:tcol(tab, stmts) */
static int sl_tcol(lua_State *L)
{
	sqlite_db *db;
	sqlite_st *st = do_binds(L, 1, 3, 0, &db);
	col_step(L, st, db, 2);
	lua_settop(L, 2);
	return 1;
}

/* Return number of rows changed since last call. */
static	int sl_changes(lua_State *L)
{
	sqlite_db *db = sl_todb(L, 1, 1);
	int prev = db->changes;
	db->changes = sqlite3_total_changes(db->db);
	lua_pushinteger(L, db->changes - prev);
	return 1;
}

/* For loop iterator for db:cols() */
static int sl_cols_aux(lua_State *L)
{
	sqlite_st *st = sl_tost(L, 1, 1);
	int err, curridx = luaL_checkinteger(L, 2);
	lua_settop(L, 2);
retry:;
	if (curridx > st->nst) /* We're done here, put back into cache. */
		return sl_cacheback(L);
	err = sqlite3_step(st->st[curridx-1]);
	if (err == SQLITE_DONE) {
		lua_pop(L, 1);
		curridx++;
		lua_pushinteger(L, curridx);
		goto retry;
	} else if (err == SQLITE_ROW) {
		lua_createtable(L, 0, sqlite3_data_count(st->st[curridx-1])+1);
		set_fields(L, st->st[curridx-1], -1);
		return 2;
	} else {
		sqlite_db *db;
		lua_getuservalue(L, 1);
		db = sl_todb(L, -1, 0);
		sl_cacheback(L);
		luaL_error(L, "while executing statement #%d: %s", curridx,
					sqlite3_errmsg(db->db));
	}
	return 0;
}

/* for idx,tab in db:cols() iterator producer */
static int sl_cols(lua_State *L)
{
	sqlite_db *db;
	do_binds(L, 1, 2, 1, &db);
	lua_pushvalue(L, SLUV_COLS);
	lua_pushvalue(L, -2);
	lua_pushinteger(L, 1);
	return 3;
}

/* db:rows() iterator */
static int sl_rows_aux(lua_State *L)
{
	sqlite_st *st = sl_tost(L, 1, 1);
	int err, curridx = luaL_checkinteger(L, 2);
	lua_settop(L, 2);
retry:;
	if (curridx > st->nst) /* We're done here, put back into cache. */
		return sl_cacheback(L);
	err = sqlite3_step(st->st[curridx-1]);
	if (err == SQLITE_DONE) {
		lua_pop(L, 1);
		curridx++;
		lua_pushinteger(L, curridx);
		goto retry;
	} else if (err == SQLITE_ROW) {
		return push_fields(L, st->st[curridx-1])+1;
	} else {
		sqlite_db *db;
		lua_getuservalue(L, 1);
		db = sl_todb(L, -1, 0);
		sl_cacheback(L);
		luaL_error(L, "while executing statement #%d: %s", curridx,
					sqlite3_errmsg(db->db));
	}
	return 0;
}

/* for idx,col1,col2.. in db:rows() */
static int sl_rows(lua_State *L)
{
	sqlite_db *db;
	do_binds(L, 1, 2, 1, &db);
	lua_pushvalue(L, SLUV_ROWS);
	lua_pushvalue(L, -2);
	lua_pushinteger(L, 1);
	return 3;
}

/* Open a database file. */
static	int	sl_open(lua_State *L)
{
	sqlite3 *sql = NULL;
	sqlite_db *db;
	size_t nlen;
	const char *fn = luaL_checklstring(L, 1, &nlen);
	
	int err = sqlite3_open(fn, &sql);
	if (!sql)
		luaL_error(L, "failed to open '%s': %s", fn, sqlite3_errstr(err));
	db = lua_newuserdata(L, sizeof(*db) + nlen);
	db->db = sql;
	db->changes = sqlite3_total_changes(sql);
	strcpy(db->name, fn);
	lua_pushvalue(L, SLUV_DB);
	lua_setmetatable(L, -2);
	lua_newtable(L);
	lua_setuservalue(L, -2);
	return 1;
}

/* Close the handle. */
static	int	sl_close(lua_State *L)
{
	int err = 0;
	sqlite_db *db = sl_uvdata(L, 1, SLUV_DB);
	if (!db) return 0;
	lua_settop(L, 1);
	if (db->db) {
		lua_getuservalue(L, 1);
		lua_pushnil(L);
		while (lua_next(L, 2)) { /* Drop cached statements. */
			do {
				sqlite_st *st = lua_touserdata(L, -1);
				nuke_stmt(L, st);
				lua_getuservalue(L, -1);
				lua_replace(L, -2);
			} while (!lua_isnil(L, -1));
			lua_pop(L, 1);
		}
		lua_pushnil(L); /* Drop cache table. */
		lua_setuservalue(L, 1);
 		err = sqlite3_close_v2(db->db);
		if (err == SQLITE_OK)
			db->db = NULL; /* Signal that it is closed. */
	}
	lua_pushinteger(L, err);
	return 1;
}

static	luaL_Reg	sl_api[] = {
	{ "open",	sl_open },
	{ "close",	sl_close },
	{ NULL, NULL }
};

static	luaL_Reg	db_meth[] = {
	{ "exec", 	sl_exec },
	{ "row", 	sl_row },
	{ "col", 	sl_col },
	{ "tcol", 	sl_tcol },
	{ "rows", 	sl_rows },
	{ "cols", 	sl_cols },
	{ "changes", 	sl_changes },
	{ "__gc",	sl_close },
	{ NULL, NULL }
};

int	luaopen_sqlite(lua_State *L)
{
	int i;
#if LUA_VERSION_NUM < 503
	luaL_newmetatable(L, SQLITEUV);
	lua_newtable(L);
	lua_pushliteral(L, "k");
	lua_setfield(L, -2, "__mode");
	lua_setmetatable(L, -2);
#endif
	lua_settop(L, 0);

	lua_newtable(L); /* SLUV_ST */
	lua_newtable(L); /* SLUV_DB */
	lua_newtable(L); /* SLUV_QS */
	for (i = 1; i <= 3; i++) lua_pushvalue(L, i);
	lua_pushcclosure(L, sl_rows_aux, 3); /* SLUV_ROWS */
	for (i = 1; i <= 3; i++) lua_pushvalue(L, i);
	lua_pushcclosure(L, sl_cols_aux, 3); /* SLUV_COLS */

	/* SLUV_ST */
	for (i = 1; i <= 5; i++) lua_pushvalue(L, i);
	lua_pushcclosure(L, sl_cacheback, 5);
	lua_setfield(L, 1, "__gc");
	lua_pushvalue(L, 1);
	lua_setmetatable(L, 1);

	/* SLUV_DB */
	lua_pushvalue(L, 2);
	lua_setfield(L, 2, "__index");
	lua_pushvalue(L, 2);
	for (i = 1; i <= 5; i++) lua_pushvalue(L, i);
	luaL_setfuncs(L, db_meth, 5);
	lua_pushvalue(L, 2);
	lua_setmetatable(L, 2);
	lua_pop(L, 1);

	/* API */
	lua_newtable(L);
	for (i = 1; i <= 5; i++) lua_pushvalue(L, i);
	luaL_setfuncs(L, sl_api, 5);
	return 1;
}
