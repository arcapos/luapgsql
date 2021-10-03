/*
 * Copyright (c) 2009 - 2021, Micro Systems Marc Balmer, CH-5073 Gipf-Oberfrick
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of Micro Systems Marc Balmer nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* PostgreSQL extension module (using Lua) */

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define htobe64(x) OSSwapHostToBigInt64(x)
#elif __FreeBSD__
#include <sys/endian.h>
#elif __linux__
#include <endian.h>
#endif
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <libpq-fe.h>
#include <libpq/libpq-fs.h>
#include <pg_config.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "luapgsql.h"

#if LUA_VERSION_NUM < 502
#define lua_setuservalue lua_setfenv
#define lua_getuservalue lua_getfenv

static void
luaL_setmetatable(lua_State *L, const char *tname)
{
	luaL_getmetatable(L, tname);
	lua_setmetatable(L, -2);
}
#endif

/*
 * Garbage collected memory
 */
static void *
gcmalloc(lua_State *L, size_t size)
{
	void **p;

	p = lua_newuserdata(L, size);
	*p = NULL;
	luaL_setmetatable(L, GCMEM_METATABLE);
	return p;
}

/* Memory can be free'ed immediately or left to the garbage collector */
static void
gcfree(void *p)
{
	void **mem = (void **)p;
	PQfreemem(*mem);
	*mem = NULL;
}

static int
gcmem_clear(lua_State *L)
{
	void **p = luaL_checkudata(L, 1, GCMEM_METATABLE);
	PQfreemem(*p);
	*p = NULL;
	return 0;
}

/*
 * Create a new connection object with a uservalue table
 */
static PGconn **
pgsql_conn_new(lua_State *L) {
	PGconn **data;

	data = lua_newuserdata(L, sizeof(PGconn *));
	*data = NULL;
	lua_newtable(L);
	lua_setuservalue(L, -2);
	luaL_setmetatable(L, CONN_METATABLE);
	return data;
}

/*
 * Database Connection Control Functions
 */
static int
pgsql_connectdb(lua_State *L)
{
	PGconn **data;
	const char *cstr;

	cstr = luaL_checkstring(L, 1);
	data = pgsql_conn_new(L);

	*data = PQconnectdb(cstr);
	if (*data == NULL)
		lua_pushnil(L);
	return 1;
}

static int
pgsql_connectStart(lua_State *L)
{
	PGconn **data;
	const char *cstr;

	cstr = luaL_checkstring(L, 1);
	data = pgsql_conn_new(L);
	*data = PQconnectStart(cstr);

	if (*data == NULL)
		lua_pushnil(L);
	return 1;
}

static PGconn *
pgsql_conn(lua_State *L, int n)
{
	PGconn **data;

	data = luaL_checkudata(L, n, CONN_METATABLE);
	luaL_argcheck(L, *data != NULL, n, "database connection is finished");
	return *data;
}

static int
pgsql_connectPoll(lua_State *L)
{
	lua_pushinteger(L, PQconnectPoll(pgsql_conn(L, 1)));
	return 1;
}

static int
pgsql_libVersion(lua_State *L)
{
	lua_pushinteger(L, PQlibVersion());
	return 1;
}

#if PG_VERSION_NUM >= 90100
static int
pgsql_ping(lua_State *L)
{
	lua_pushinteger(L, PQping(luaL_checkstring(L, 1)));
	return 1;
}
#endif

static int
pgsql_encryptPassword(lua_State *L)
{
	char const **pw;
	const char *passwd, *user;

	passwd = luaL_checkstring(L, 1);
	user = luaL_checkstring(L, 2);
	pw = gcmalloc(L, sizeof(char *));
	*pw = PQencryptPassword(passwd, user);
	lua_pushstring(L, *pw);
	gcfree(pw);
	return 1;
}

static int
pgsql_unescapeBytea(lua_State *L)
{
	unsigned char **p;
	size_t len;
	const char *bytea;

	bytea = luaL_checkstring(L, 1);
	p = gcmalloc(L, sizeof(char *));
	*p = PQunescapeBytea((const unsigned char *)bytea, &len);
	if (*p == NULL)
		lua_pushnil(L);
	else {
		lua_pushlstring(L, (const char *)*p, len);
		gcfree(p);
	}
	return 1;
}

static int
pgsql_initOpenSSL(lua_State *L)
{
	PQinitOpenSSL(lua_toboolean(L, 1), lua_toboolean(L, 2));
	return 0;
}

static int
conn_finish(lua_State *L)
{
	PGconn **conn;

	conn = luaL_checkudata(L, 1, CONN_METATABLE);
	if (*conn) {
		/*
		 * Check in the registry if a value has been stored at
		 * index '*conn'; if a value is found, don't close the
		 * connection.
		 * This mechanism can be used when the PostgreSQL connection
		 * object is provided to Lua from a C program that wants to
		 * ensure the connections stays open, even when the Lua
		 * program has terminated.
		 * To prevent the closing of the connection, use the following
		 * code to set a value in the registry at index '*conn' just
		 * before handing the connection object to Lua:
		 *
		 * PGconn *conn, **data;
		 *
		 * conn = PQconnectdb(...);
		 * data = lua_newuserdata(L, sizeof(PGconn *));
		 * *data = conn;
		 * lua_pushlightuserdata(L, *data);
		 * lua_pushboolean(L, 1);
		 * lua_settable(L, LUA_REGISTRYINDEX);
		 */
		lua_pushlightuserdata(L, *conn);
		lua_gettable(L, LUA_REGISTRYINDEX);
		if (lua_isnil(L, -1)) {
			PQfinish(*conn);
			*conn = NULL;
			/* clean out now invalidated keys from uservalue */
			lua_getuservalue(L, 1);
			lua_pushnil(L);
			lua_setfield(L, -2, "trace_file");
		}
	}
	return 0;
}

static int
conn_reset(lua_State *L)
{
	PQreset(pgsql_conn(L, 1));
	return 0;
}

static int
conn_resetStart(lua_State *L)
{
	lua_pushboolean(L, PQresetStart(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_resetPoll(lua_State *L)
{
	lua_pushinteger(L, PQresetPoll(pgsql_conn(L, 1)));
	return 1;
}

/*
 * Connection status functions
 */
static int
conn_db(lua_State *L)
{
	lua_pushstring(L, PQdb(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_user(lua_State *L)
{
	lua_pushstring(L, PQuser(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_pass(lua_State *L)
{
	lua_pushstring(L, PQpass(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_host(lua_State *L)
{
	lua_pushstring(L, PQhost(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_port(lua_State *L)
{
	lua_pushstring(L, PQport(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_tty(lua_State *L)
{
	lua_pushstring(L, PQtty(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_options(lua_State *L)
{
	lua_pushstring(L, PQoptions(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_status(lua_State *L)
{
	PGconn **conn;

	conn = luaL_checkudata(L, 1, CONN_METATABLE);
	lua_pushinteger(L, PQstatus(*conn));
	return 1;
}

static int
conn_transactionStatus(lua_State *L)
{
	lua_pushinteger(L, PQtransactionStatus(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_parameterStatus(lua_State *L)
{
	const char *status;

	status = PQparameterStatus(pgsql_conn(L, 1), luaL_checkstring(L, 2));
	if (status == NULL)
		lua_pushnil(L);
	else
		lua_pushstring(L, status);
	return 1;
}

static int
conn_protocolVersion(lua_State *L)
{
	lua_pushinteger(L, PQprotocolVersion(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_serverVersion(lua_State *L)
{
	lua_pushinteger(L, PQserverVersion(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_errorMessage(lua_State *L)
{
	lua_pushstring(L, PQerrorMessage(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_socket(lua_State *L)
{
	int fd;

	fd = PQsocket(pgsql_conn(L, 1));
	if (fd >= 0)
		lua_pushinteger(L, fd);
	else
		lua_pushnil(L);
	return 1;
}

static int
conn_backendPID(lua_State *L)
{
	lua_pushinteger(L, PQbackendPID(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_connectionNeedsPassword(lua_State *L)
{
	lua_pushboolean(L, PQconnectionNeedsPassword(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_connectionUsedPassword(lua_State *L)
{
	lua_pushboolean(L, PQconnectionUsedPassword(pgsql_conn(L, 1)));
	return 1;
}

#if PG_VERSION_NUM >= 90500
static int
conn_sslInUse(lua_State *L)
{
	lua_pushboolean(L, PQsslInUse(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_sslAttribute(lua_State *L)
{
	lua_pushstring(L,
		PQsslAttribute(pgsql_conn(L, 1), luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_sslAttributeNames(lua_State *L)
{
	const char * const *attribNames;
	int k;

	attribNames = PQsslAttributeNames(pgsql_conn(L, 1));
	lua_newtable(L);
	for (k = 1; *attribNames; k++, attribNames++) {
		lua_pushinteger(L, k);
		lua_pushstring(L, *attribNames);
		lua_settable(L, -3);
	}
	return 1;
}
#endif

/*
 * Command Execution Functions
 */
static int
conn_exec(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	const char *command;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexec(conn, command);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static void
get_param(lua_State *L, int t, int n, Oid *paramTypes, char **paramValues,
    int *paramLengths, int *paramFormats)
{
	switch (lua_type(L, t)) {
	case LUA_TBOOLEAN:
		if (paramTypes != NULL)
			paramTypes[n] = BOOLOID;
		if (paramValues != NULL) {
			paramValues[n] = lua_newuserdata(L, sizeof(char));
			*(char *)paramValues[n] = lua_toboolean(L, t);
			paramLengths[n] = 1;
		}
		if (paramFormats != NULL)
			paramFormats[n] = 1;
		break;
	case LUA_TNUMBER:
		if (paramTypes != NULL) {
#if LUA_VERSION_NUM >= 503
			if (lua_isinteger(L, t))
				paramTypes[n] = INT8OID;
			else
#endif
				paramTypes[n] = FLOAT8OID;
		}
		if (paramValues != NULL) {
			union {
				double v;
				uint64_t i;
			} swap;

#if LUA_VERSION_NUM >= 503
			if (lua_isinteger(L, t))
				swap.i = lua_tointeger(L, t);
			else
#endif
				swap.v = lua_tonumber(L, t);
			paramValues[n] = lua_newuserdata(L, sizeof(uint64_t));
			*(uint64_t *)paramValues[n] = htobe64(swap.i);
			paramLengths[n] = sizeof(uint64_t);
		}
		if (paramFormats != NULL)
			paramFormats[n] = 1;
		break;
	case LUA_TSTRING:
		if (paramTypes != NULL)
			paramTypes[n] = TEXTOID;
		if (paramValues != NULL) {
			const char *s;
			size_t len;

			s = lua_tolstring(L, t, &len);
			paramValues[n] = lua_newuserdata(L, len + 1);
			/*
			 * lua_tolstring returns a string with '\0' after
			 * the last character.
			 */
			memcpy(paramValues[n], s, len + 1);
		}
		if (paramFormats != NULL)
			paramFormats[n] = 0;
		break;
	case LUA_TNIL:
		if (paramTypes != NULL)
			paramTypes[n] = 0;
		if (paramValues != NULL)
			paramValues[n] = NULL;
		if (paramFormats != NULL)
			paramFormats[n] = 0;
		break;
	default:
		luaL_error(L, "unsupported PostgreSQL parameter type %s ("
		    "use table.unpack() for table types)", luaL_typename(L, t));
		/* NOTREACHED */
	}
}

static int
conn_execParams(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	Oid *paramTypes;
	char **paramValues;
	const char *command;
	int n, nParams, *paramLengths, *paramFormats;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);

	nParams = lua_gettop(L) - 2;	/* subtract connection and command */

	if (nParams > 65535)
		luaL_error(L, "number of parameters must not exceed 65535");

	if (nParams) {
		luaL_checkstack(L, 4 + nParams, "out of stack space");

		paramTypes = lua_newuserdata(L, nParams * sizeof(Oid));
		paramValues = lua_newuserdata(L, nParams * sizeof(char *));
		paramLengths = lua_newuserdata(L, nParams * sizeof(int));
		paramFormats = lua_newuserdata(L, nParams * sizeof(int));

		for (n = 0; n < nParams; n++)
			get_param(L, 3 + n, n, paramTypes, paramValues,
			    paramLengths, paramFormats);
	} else {
		paramTypes = NULL;
		paramValues = NULL;
		paramLengths = NULL;
		paramFormats = NULL;
	}
	luaL_checkstack(L, 2, "out of stack space");
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexecParams(conn, command, nParams, paramTypes,
	    (const char * const*)paramValues, paramLengths, paramFormats, 0);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static int
conn_prepare(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	Oid *paramTypes;
	const char *command, *name;
	int n, nParams;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);
	name = luaL_checkstring(L, 3);

	nParams = lua_gettop(L) - 3;	/* subtract connection, name, command */

	if (nParams > 65535)
		luaL_error(L, "number of parameters must not exceed 65535");

	if (nParams) {
		paramTypes = lua_newuserdata(L, nParams * sizeof(Oid));

		for (n = 0; n < nParams; n++)
			get_param(L, 4 + n, n, paramTypes, NULL, NULL, NULL);
	} else
		paramTypes = NULL;

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQprepare(conn, command, name, nParams, paramTypes);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static int
conn_execPrepared(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	char **paramValues;
	const char *command;
	int n, nParams, *paramLengths, *paramFormats;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);

	nParams = lua_gettop(L) - 2;	/* subtract connection and name */

	if (nParams > 65535)
		luaL_error(L, "number of parameters must not exceed 65535");

	if (nParams) {
		luaL_checkstack(L, 3 + nParams, "out of stack space");

		paramValues = lua_newuserdata(L, nParams * sizeof(char *));
		paramLengths = lua_newuserdata(L, nParams * sizeof(int));
		paramFormats = lua_newuserdata(L, nParams * sizeof(int));

		for (n = 0; n < nParams; n++)
			get_param(L, 3 + n, n, NULL, paramValues, paramLengths,
			    paramFormats);
	} else {
		paramValues = NULL;
		paramLengths = NULL;
		paramFormats = NULL;
	}
	luaL_checkstack(L, 2, "out of stack space");

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexecPrepared(conn, command, nParams,
	    (const char * const*)paramValues, paramLengths, paramFormats, 0);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static int
conn_describePrepared(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	const char *name;

	conn = pgsql_conn(L, 1);
	name = luaL_checkstring(L, 2);

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQdescribePrepared(conn, name);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static int
conn_describePortal(lua_State *L)
{
	PGconn *conn;
	PGresult **res;
	const char *name;

	conn = pgsql_conn(L, 1);
	name = luaL_checkstring(L, 2);

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQdescribePortal(conn, name);
	if (*res == NULL)
		lua_pushnil(L);
	else
		luaL_setmetatable(L, RES_METATABLE);
	return 1;
}

static int
conn_escapeString(lua_State *L)
{
	PGconn *d;
	size_t len;
	char *buf;
	const char *str;
	int error;

	d = pgsql_conn(L, 1);

	str = lua_tolstring(L, 2, &len);
	if (str == NULL) {
		lua_pushnil(L);
		return 1;
	}
	buf = lua_newuserdata(L, 2 * (len + 1));

	PQescapeStringConn(d, buf, str, len, &error);
	if (!error)
		lua_pushstring(L, buf);
	else
		lua_pushnil(L);
	return 1;
}

static int
conn_escapeLiteral(lua_State *L)
{
	const char *s;
	char **p;
	PGconn *d;
	size_t len;

	d = pgsql_conn(L, 1);
	s = luaL_checklstring(L, 2, &len);
	p = gcmalloc(L, sizeof(char *));
	*p = PQescapeLiteral(d, s, len);
	lua_pushstring(L, *p);
	gcfree(p);
	return 1;
}

static int
conn_escapeIdentifier(lua_State *L)
{
	const char *s;
	char  **p;
	PGconn *d;
	size_t len;

	d = pgsql_conn(L, 1);
	s = luaL_checklstring(L, 2, &len);
	p = gcmalloc(L, sizeof(char *));
	*p = PQescapeIdentifier(d, s, len);
	lua_pushstring(L, *p);
	gcfree(p);
	return 1;
}

static int
conn_escapeBytea(lua_State *L)
{
	unsigned char **p;
	const unsigned char *s;
	PGconn *d;
	size_t from_length, to_length;

	d = pgsql_conn(L, 1);
	s = (const unsigned char *)luaL_checklstring(L, 2, &from_length);
	p = gcmalloc(L, sizeof(char *));
	*p = PQescapeByteaConn(d, s, from_length, &to_length);
	if (*p) {
		lua_pushlstring(L, (const char *)*p, to_length - 1);
		gcfree(p);
	} else
		lua_pushnil(L);
	return 1;
}

/*
 * Asynchronous Command Execution Functions
 */
static int
conn_sendQuery(lua_State *L)
{
	lua_pushboolean(L, PQsendQuery(pgsql_conn(L, 1),
	    luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_sendQueryParams(lua_State *L)
{
	PGconn *conn;
	Oid *paramTypes;
	char **paramValues;
	const char *command;
	int n, nParams, *paramLengths, *paramFormats;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);

	nParams = lua_gettop(L) - 2;	/* subtract connection and command */

	if (nParams) {
		luaL_checkstack(L, 4 + nParams, "out of stack space");

		paramTypes = lua_newuserdata(L, nParams * sizeof(Oid));
		paramValues = lua_newuserdata(L, nParams * sizeof(char *));
		paramLengths = lua_newuserdata(L, nParams * sizeof(int));
		paramFormats = lua_newuserdata(L, nParams * sizeof(int));

		for (n = 0; n < nParams; n++)
			get_param(L, 3 + n, n, paramTypes, paramValues,
			   paramLengths, paramFormats);
	} else {
		paramTypes = NULL;
		paramValues = NULL;
		paramLengths = NULL;
		paramFormats = NULL;
	}
	lua_pushboolean(L,
	    PQsendQueryParams(conn, command, nParams, paramTypes,
	    (const char * const*)paramValues, paramLengths, paramFormats, 0));
	return 1;
}

static int
conn_sendPrepare(lua_State *L)
{
	PGconn *conn;
	Oid *paramTypes;
	const char *command, *name;
	int n, nParams;

	conn = pgsql_conn(L, 1);
	command = luaL_checkstring(L, 2);
	name = luaL_checkstring(L, 3);

	nParams = lua_gettop(L) - 3;	/* subtract connection, name, command */

	if (nParams) {
		paramTypes = lua_newuserdata(L, nParams * sizeof(Oid));

		for (n = 0; n < nParams; n++)
			get_param(L, 4 + n, n, paramTypes, NULL, NULL, NULL);
	} else
		paramTypes = NULL;
	lua_pushboolean(L,
	    PQsendPrepare(conn, command, name, nParams, paramTypes));
	return 1;
}

static int
conn_sendQueryPrepared(lua_State *L)
{
	PGconn *conn;
	char **paramValues;
	const char *name;
	int n, nParams, *paramLengths, *paramFormats;

	conn = pgsql_conn(L, 1);
	name = luaL_checkstring(L, 2);

	nParams = lua_gettop(L) - 2;	/* subtract connection and name */

	if (nParams) {
		luaL_checkstack(L, 3 + nParams, "out of stack space");

		paramValues = lua_newuserdata(L, nParams * sizeof(char *));
		paramLengths = lua_newuserdata(L, nParams * sizeof(int));
		paramFormats = lua_newuserdata(L, nParams * sizeof(int));

		for (n = 0; n < nParams; n++)
			get_param(L, 3 + n, n, NULL, paramValues, paramLengths,
			    paramFormats);
	} else {
		paramValues = NULL;
		paramLengths = NULL;
		paramFormats = NULL;
	}
	lua_pushboolean(L,
	    PQsendQueryPrepared(conn, name, nParams,
	    (const char * const*)paramValues, paramLengths, paramFormats, 0));
	return 1;
}

static int
conn_sendDescribePrepared(lua_State *L)
{
	lua_pushboolean(L,
	    PQsendDescribePrepared(pgsql_conn(L, 1), luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_sendDescribePortal(lua_State *L)
{
	lua_pushboolean(L,
	    PQsendDescribePortal(pgsql_conn(L, 1), luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_getResult(lua_State *L)
{
	PGresult *r, **res;

	r = PQgetResult(pgsql_conn(L, 1));
	if (r == NULL)
		lua_pushnil(L);
	else {
		res = lua_newuserdata(L, sizeof(PGresult *));
		*res = r;
		luaL_setmetatable(L, RES_METATABLE);
	}
	return 1;
}

static int
conn_cancel(lua_State *L)
{
	PGconn *d;
	PGcancel *cancel;
	char errbuf[256];
	int res = 1;

	d = pgsql_conn(L, 1);
	cancel = PQgetCancel(d);
	if (cancel != NULL) {
		res = PQcancel(cancel, errbuf, sizeof errbuf);
		if (!res) {
			lua_pushboolean(L, 0);
			lua_pushstring(L, errbuf);
		} else
			lua_pushboolean(L, 1);
		PQfreeCancel(cancel);
	} else
		lua_pushboolean(L, 0);
	return res == 1 ? 1 : 2;
}

#if PG_VERSION_NUM >= 90200
static int
conn_setSingleRowMode(lua_State *L)
{
	lua_pushboolean(L, PQsetSingleRowMode(pgsql_conn(L, 1)));
	return 1;
}
#endif

/*
 * Asynchronous Notification Functions
 */
static int
conn_notifies(lua_State *L)
{
	PGnotify **notify, *n;

	n = PQnotifies(pgsql_conn(L, 1));
	if (n == NULL)
		lua_pushnil(L);
	else {
		notify = lua_newuserdata(L, sizeof(PGnotify *));
		*notify = n;
		luaL_setmetatable(L, NOTIFY_METATABLE);
	}
	return 1;
}

/*
 * Commands associated with the COPY command
 */
static int
conn_putCopyData(lua_State *L)
{
	const char *data;
	size_t len;
	int r;

	data = luaL_checklstring(L, 2, &len);
	r = PQputCopyData(pgsql_conn(L, 1), data, len);

	if (r != -1)
		lua_pushboolean(L, r);
	else
		lua_pushnil(L);
	return 1;
}

static int
conn_putCopyEnd(lua_State *L)
{
	PGconn *conn;
	int r;

	conn = pgsql_conn(L, 1);
	r = PQputCopyEnd(conn, luaL_optstring(L, 2, NULL));

	if (r != -1)
		lua_pushboolean(L, r);
	else
		lua_pushnil(L);
	return 1;
}

static int
conn_getCopyData(lua_State *L)
{
	PGconn *conn;
	int async, len;
	char **data;

	conn = pgsql_conn(L, 1);
	async = lua_toboolean(L, 2);
	data = gcmalloc(L, sizeof(char *));
	len = PQgetCopyData(conn, data, async);
	if (len > 0)
		lua_pushlstring(L, *data, len);
	else if (len == 0)	/* no data yet */
		lua_pushboolean(L, 0);
	else if (len == -1)	/* copy done */
		lua_pushboolean(L, 1);
	else			/* an error occurred */
		lua_pushnil(L);
	gcfree(data);
	return 1;
}

/*
 * Control functions
 */
static int
conn_clientEncoding(lua_State *L)
{
	lua_pushstring(L,
	    pg_encoding_to_char(PQclientEncoding(pgsql_conn(L, 1))));
	return 1;
}

static int
conn_setClientEncoding(lua_State *L)
{
	if (PQsetClientEncoding(pgsql_conn(L, 1), luaL_checkstring(L, 2)))
		lua_pushboolean(L, 0);
	else
		lua_pushboolean(L, 1);
	return 1;
}

static int
conn_setErrorVerbosity(lua_State *L)
{
	lua_pushinteger(L,
	    PQsetErrorVerbosity(pgsql_conn(L, 1), luaL_checkinteger(L, 2)));
	return 1;
}

static int
closef_untrace(lua_State *L)
{
	PGconn *conn;
	lua_CFunction cf;

	luaL_checkudata(L, 1, LUA_FILEHANDLE);

	/* untrace so libpq doesn't segfault */
	lua_getuservalue(L, 1);
	lua_getfield(L, -1, "PGconn");
	conn = pgsql_conn(L, -1);
	lua_getfield(L, -2, "old_uservalue");
#if LUA_VERSION_NUM >= 502
	lua_getfield(L, -3, "old_closef");
#else
	lua_getfield(L, -1, "__close");
#endif
	cf = lua_tocfunction(L, -1);
	lua_pop(L, 1);
	lua_setuservalue(L, 1);

	PQuntrace(conn);

	/* let go of PGconn's reference to file handle */
	lua_getuservalue(L, -1);
	lua_pushnil(L);
	lua_setfield(L, -2, "trace_file");

	/* pop stream uservalue, PGconn, PGconn uservalue */
	lua_pop(L, 3);

	/* call original close function */
	return (*cf)(L);
}

static int
conn_trace(lua_State *L)
{
	PGconn *conn;
#if LUA_VERSION_NUM >= 502
	luaL_Stream *stream;

	conn = pgsql_conn(L, 1);
	stream = luaL_checkudata(L, 2, LUA_FILEHANDLE);
	luaL_argcheck(L, stream->f != NULL, 2, "invalid file handle");

	/*
	 * Keep a reference to the file object in uservalue of connection
	 * so it doesn't get garbage collected.
	 */
	lua_getuservalue(L, 1);
	lua_pushvalue(L, 2);
	lua_setfield(L, -2, "trace_file");

	/*
	 * Swap out closef luaL_Stream member for our wrapper that will
	 * untrace.
	 */
	lua_createtable(L, 0, 3);
	lua_getuservalue(L, 2);
	lua_setfield(L, -2, "old_uservalue");
	lua_pushcfunction(L, stream->closef);
	lua_setfield(L, -2, "old_closef");
	lua_pushvalue(L, 1);
	lua_setfield(L, -2, "PGconn");
	lua_setuservalue(L, 2);
	stream->closef = closef_untrace;

	PQtrace(conn, stream->f);
#else
	FILE **fp;

	conn = pgsql_conn(L, 1);
	fp = luaL_checkudata(L, 2, LUA_FILEHANDLE);
	luaL_argcheck(L, *fp != NULL, 2, "invalid file handle");

	/*
	 * Keep a reference to the file object in uservalue of connection
	 * so it doesn't get garbage collected.
	 */
	lua_getuservalue(L, 1);
	lua_pushvalue(L, 2);
	lua_setfield(L, -2, "trace_file");

	/*
	 * Swap __close field in file environment for our wrapper that will
	 * untrace keep the old closef under the key of the PGconn.
	 */
	lua_createtable(L, 0, 3);
	lua_pushcfunction(L, closef_untrace);
	lua_setfield(L, -2, "__close");
	lua_getuservalue(L, 2);
	lua_setfield(L, -2, "old_uservalue");
	lua_pushvalue(L, 1);
	lua_setfield(L, -2, "PGconn");
	lua_setuservalue(L, 2);

	PQtrace(conn, *fp);
#endif
	return 0;
}

static int
conn_untrace(lua_State *L)
{
	PQuntrace(pgsql_conn(L, 1));

	/* Let go of PGconn's reference to file handle. */
	lua_getuservalue(L, 1);
	lua_pushnil(L);
	lua_setfield(L, -2, "trace_file");

	return 0;
}

/*
 * Miscellaneous Functions
 */
static int
conn_consumeInput(lua_State *L)
{
	lua_pushboolean(L, PQconsumeInput(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_isBusy(lua_State *L)
{
	lua_pushboolean(L, PQisBusy(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_setnonblocking(lua_State *L)
{
	int r;

	r = PQsetnonblocking(pgsql_conn(L, 1), lua_toboolean(L, 2));
	lua_pushboolean(L, !r);
	return 1;
}

static int
conn_isnonblocking(lua_State *L)
{
	lua_pushboolean(L, PQisnonblocking(pgsql_conn(L, 1)));
	return 1;
}

static int
conn_flush(lua_State *L)
{
	int r;

	r = PQflush(pgsql_conn(L, 1));

	if (r >= 0)
		lua_pushboolean(L, r == 0);
	else
		lua_pushnil(L);
	return 1;
}

#if PG_VERSION_NUM >= 100000
static int
conn_encryptPassword(lua_State *L)
{
	const char *algorithm = NULL;
	char **pw;

	if (lua_isstring(L, 4))
		algorithm = lua_tostring(L, 4);

	pw = gcmalloc(L, sizeof(char *));
	*pw = PQencryptPasswordConn(pgsql_conn(L, 1), luaL_checkstring(L, 2),
	    luaL_checkstring(L, 3), algorithm);
	if (*pw) {
		lua_pushstring(L, *pw);
		gcfree(pw);
	} else
		lua_pushnil(L);
	return 1;
}
#endif

/* Notice processing */
static void
noticeReceiver(void *arg, const PGresult *r)
{
	notice *n = arg;
	PGresult **res;

	lua_rawgeti(n->L, LUA_REGISTRYINDEX, n->f);
	res = lua_newuserdata(n->L, sizeof(PGresult *));

	*res = (PGresult *)r;
	luaL_setmetatable(n->L, RES_METATABLE);

	if (lua_pcall(n->L, 1, 0, 0))
		luaL_error(n->L, "%s", lua_tostring(n->L, -1));
	*res = NULL;	/* avoid double free */
}

static void
noticeProcessor(void *arg, const char *message)
{
	notice *n = arg;

	lua_rawgeti(n->L, LUA_REGISTRYINDEX, n->f);
	lua_pushstring(n->L, message);
	if (lua_pcall(n->L, 1, 0, 0))
		luaL_error(n->L, "%s", lua_tostring(n->L, -1));
}

static int
conn_setNoticeReceiver(lua_State *L)
{
	notice **n;
	PGconn *conn;
	int f;

	if (!lua_isfunction(L, -1))
		return luaL_argerror(L, -1, "function expected");

	f = luaL_ref(L, LUA_REGISTRYINDEX);
	conn = pgsql_conn(L, 1);

	n = gcmalloc(L, sizeof(notice *));
	*n = malloc(sizeof(notice));
	if (*n != NULL) {
		(*n)->L = L;
		(*n)->f = f;
		PQsetNoticeReceiver(conn, noticeReceiver, *n);
	} else
		return luaL_error(L, "out of memory");
	return 0;
}

static int
conn_setNoticeProcessor(lua_State *L)
{
	notice **n;
	PGconn *conn;
	int f;

	if (!lua_isfunction(L, -1))
		return luaL_argerror(L, -1, "function expected");

	f = luaL_ref(L, LUA_REGISTRYINDEX);
	conn = pgsql_conn(L, 1);

	n = gcmalloc(L, sizeof(notice *));
	*n = malloc(sizeof(notice));
	if (*n != NULL) {
		(*n)->L = L;
		(*n)->f = f;
		PQsetNoticeProcessor(conn, noticeProcessor, *n);
	} else
		return luaL_error(L, "out of memory");
	return 0;
}

/* Large objects */
static int
conn_lo_create(lua_State *L)
{
	Oid oid;

	if (lua_gettop(L) == 2)
		oid = luaL_checkinteger(L, 2);
	else
		oid = 0;
	lua_pushinteger(L, lo_create(pgsql_conn(L, 1), oid));
	return 1;
}

static int
conn_lo_import(lua_State *L)
{
	lua_pushinteger(L, lo_import(pgsql_conn(L, 1), luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_lo_import_with_oid(lua_State *L)
{
	lua_pushinteger(L,
	    lo_import_with_oid(pgsql_conn(L, 1), luaL_checkstring(L, 2),
	    luaL_checkinteger(L, 3)));
	return 1;
}

static int
conn_lo_export(lua_State *L)
{
	int r;

	r = lo_export(pgsql_conn(L, 1), luaL_checkinteger(L, 2),
	    luaL_checkstring(L, 3));

	lua_pushboolean(L, r == 1);
	return 1;
}

static int
conn_lo_open(lua_State *L)
{
	int fd;

	fd = lo_open(pgsql_conn(L, 1), luaL_checkinteger(L, 2),
	    luaL_checkinteger(L, 3));
	if (fd == -1)
		lua_pushnil(L);
	else
		lua_pushinteger(L, fd);
	return 1;
}

static int
conn_lo_write(lua_State *L)
{
	const char *s;
	size_t len;

	s = lua_tolstring(L, 3, &len);
	lua_pushinteger(L, lo_write(pgsql_conn(L, 1), luaL_checkinteger(L, 2),
	    s, len));
	return 1;
}

static int
conn_lo_read(lua_State *L)
{
	char *buf;
	size_t len;

	len = luaL_checkinteger(L, 3);
	buf = lua_newuserdata(L, len);
	len = lo_read(pgsql_conn(L, 1), luaL_checkinteger(L, 2), buf, len);
	lua_pushlstring(L, buf, len);
	lua_pushinteger(L, len);
	return 2;
}

static int
conn_lo_lseek(lua_State *L)
{
	lua_pushinteger(L, lo_lseek(pgsql_conn(L, 1), luaL_checkinteger(L, 2),
	    luaL_checkinteger(L, 3), luaL_checkinteger(L, 4)));
	return 1;
}

static int
conn_lo_tell(lua_State *L)
{
	lua_pushinteger(L, lo_tell(pgsql_conn(L, 1), luaL_checkinteger(L, 2)));
	return 1;
}

static int
conn_lo_truncate(lua_State *L)
{
	lua_pushinteger(L, lo_truncate(pgsql_conn(L, 1),
	    luaL_checkinteger(L, 2), luaL_checkinteger(L, 3)));
	return 1;
}

static int
conn_lo_close(lua_State *L)
{
	lua_pushboolean(L,
	    lo_close(pgsql_conn(L, 1), luaL_checkinteger(L, 2)) == 0);
	return 1;
}

static int
conn_lo_unlink(lua_State *L)
{
	lua_pushboolean(L,
	    lo_unlink(pgsql_conn(L, 1), luaL_checkinteger(L, 2)) == 1);
	return 1;
}

#if PG_VERSION_NUM >= 90300
static int
conn_lo_lseek64(lua_State *L)
{
	lua_pushinteger(L, lo_lseek64(pgsql_conn(L, 1), luaL_checkinteger(L, 2),
	    luaL_checkinteger(L, 3), luaL_checkinteger(L, 4)));
	return 1;
}

static int
conn_lo_tell64(lua_State *L)
{
	lua_pushinteger(L,
	    lo_tell64(pgsql_conn(L, 1), luaL_checkinteger(L, 2)));
	return 1;
}

static int
conn_lo_truncate64(lua_State *L)
{
	lua_pushinteger(L, lo_truncate64(pgsql_conn(L, 1),
	    luaL_checkinteger(L, 2), luaL_checkinteger(L, 3)));
	return 1;
}
#endif

/*
 * Result set functions
 */
static int
res_status(lua_State *L)
{
	lua_pushinteger(L,
	    PQresultStatus(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_resStatus(lua_State *L)
{
	lua_pushstring(L, PQresStatus(luaL_checkinteger(L, 2)));
	return 1;
}

static int
res_errorMessage(lua_State *L)
{
	lua_pushstring(L,
	    PQresultErrorMessage(*(PGresult **)luaL_checkudata(L, 1,
	    RES_METATABLE)));
	return 1;
}

static int
res_errorField(lua_State *L)
{
	char *field;

	field = PQresultErrorField(
	    *(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    lua_tointeger(L, 2));
	if (field == NULL)
		lua_pushnil(L);
	else
		lua_pushstring(L, field);
	return 1;
}

static int
res_nfields(lua_State *L)
{
	lua_pushinteger(L,
	    PQnfields(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_ntuples(lua_State *L)
{
	lua_pushinteger(L,
	    PQntuples(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_fname(lua_State *L)
{
	lua_pushstring(L,
	    PQfname(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_fnumber(lua_State *L)
{
	lua_pushinteger(L,
	    PQfnumber(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkstring(L, 2)) + 1);
	return 1;
}

static int
res_ftable(lua_State *L)
{
	lua_pushinteger(L,
	    PQftable(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_ftablecol(lua_State *L)
{
	lua_pushinteger(L,
	    PQftablecol(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_fformat(lua_State *L)
{
	lua_pushinteger(L,
	    PQfformat(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_ftype(lua_State *L)
{
	lua_pushinteger(L,
	    PQftype(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_fmod(lua_State *L)
{
	lua_pushinteger(L,
	    PQfmod(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_fsize(lua_State *L)
{
	lua_pushinteger(L,
	    PQfsize(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1));
	return 1;
}

static int
res_binaryTuples(lua_State *L)
{
	lua_pushboolean(L,
	    PQbinaryTuples(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_getvalue(lua_State *L)
{
	lua_pushstring(L,
	    PQgetvalue(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1, luaL_checkinteger(L, 3) - 1));
	return 1;
}

static int
res_getisnull(lua_State *L)
{
	lua_pushboolean(L,
	    PQgetisnull(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1, luaL_checkinteger(L, 3) - 1));
	return 1;
}

static int
res_getlength(lua_State *L)
{
	lua_pushinteger(L,
	    PQgetlength(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2) - 1, luaL_checkinteger(L, 3) - 1));
	return 1;
}

static int
res_nparams(lua_State *L)
{
	lua_pushinteger(L,
	    PQnparams(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_paramtype(lua_State *L)
{
	lua_pushinteger(L,
	    PQparamtype(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_cmdStatus(lua_State *L)
{
	lua_pushstring(L,
	    PQcmdStatus(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_cmdTuples(lua_State *L)
{
	lua_pushstring(L,
	    PQcmdTuples(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_oidValue(lua_State *L)
{
	lua_pushinteger(L,
	    PQoidValue(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_oidStatus(lua_State *L)
{
	lua_pushstring(L,
	    PQoidStatus(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

/* Lua specific functions */
static int
res_copy(lua_State *L)
{
	PGresult *res = *(PGresult **)luaL_checkudata(L, 1, RES_METATABLE);
	int row, col, convert;

	convert = 0;	/* Do not convert numeric types */

	if (lua_gettop(L) == 2)
		convert = lua_toboolean(L, 2);

	lua_newtable(L);
	for (row = 0; row < PQntuples(res); row++) {
		lua_pushinteger(L, row + 1);
		lua_newtable(L);
		for (col = 0; col < PQnfields(res); col++) {
			if (convert)
				switch (PQftype(res, col)) {
				case BOOLOID:
					lua_pushboolean(L,
					    strcmp(PQgetvalue(res, row, col),
					    "f"));
					break;
				case INT2OID:
				case INT4OID:
				case INT8OID:
					lua_pushinteger(L,
					    atol(PQgetvalue(res, row, col)));
					break;
				case FLOAT4OID:
				case FLOAT8OID:
				case NUMERICOID:
					lua_pushnumber(L,
					    atof(PQgetvalue(res, row, col)));
					break;
				default:
					lua_pushstring(L,
					    PQgetvalue(res, row, col));
				}
			else
				lua_pushstring(L, PQgetvalue(res, row, col));
			lua_setfield(L, -2, PQfname(res, col));
		}
		lua_settable(L, -3);
	}
	return 1;
}

static int
res_fields_iterator(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	int n;

	t->row++;

	luaL_checkstack(L, PQnfields(t->res), "out of stack space");
	if (t->row == PQntuples(t->res))
		for (n = 0; n < PQnfields(t->res); n++)
			lua_pushnil(L);
	else
		for (n = 0; n < PQnfields(t->res); n++)
			lua_pushstring(L, PQgetvalue(t->res, t->row, n));
	return PQnfields(t->res);
}

static int
res_fields(lua_State *L)
{
	tuple *t;

	lua_pushcfunction(L, res_fields_iterator);
	t = lua_newuserdata(L, sizeof(tuple));
	luaL_setmetatable(L, TUPLE_METATABLE);
	t->res = *(PGresult **)luaL_checkudata(L, 1, RES_METATABLE);
	t->row = -1;
	return 2;
}

static int
res_tuples_iterator(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);

	t->row++;

	if (t->row == PQntuples(t->res)) {
		lua_pushnil(L);
		lua_pushnil(L);
	} else {
		lua_pushvalue(L, 1);
		lua_pushinteger(L, t->row + 1);
	}
	return 2;
}

static int
res_tuples(lua_State *L)
{
	PGresult **res;
	tuple *t;

	res = (PGresult **)luaL_checkudata(L, 1, RES_METATABLE);

	lua_pushcfunction(L, res_tuples_iterator);
	t = lua_newuserdata(L, sizeof(tuple));
	luaL_setmetatable(L, TUPLE_METATABLE);
	t->res = *res;
	t->row = -1;
	return 2;
}

static int
res_index(lua_State *L)
{
	if (lua_type(L, -1) == LUA_TNUMBER) {
		tuple *t;
		PGresult *res;
		int row;

		res = *(PGresult **)luaL_checkudata(L, 1, RES_METATABLE);
		row = luaL_checkinteger(L, 2) - 1;

		if (row < 0 || row >= PQntuples(res))
			lua_pushnil(L);
		else {
			t = lua_newuserdata(L, sizeof(tuple));
			t->res = res;
			t->row = row;
			luaL_setmetatable(L, TUPLE_METATABLE);
		}
	} else {
		const char *nam;

		nam = lua_tostring(L, -1);
		if (lua_getmetatable(L, -2)) {
			lua_pushstring(L, nam);
			lua_rawget(L, -2);
		} else
			lua_pushnil(L);
	}
	return 1;
}

static int
res_clear(lua_State *L)
{
	PGresult **r;

	r = luaL_checkudata(L, 1, RES_METATABLE);
	if (*r) {
		PQclear(*r);
		*r = NULL;
	}
	return 0;
}

/*
 * Notifies methods (objects returned by conn:notifies())
 */
static int
notify_relname(lua_State *L)
{
	PGnotify **n;

	n = luaL_checkudata(L, 1, NOTIFY_METATABLE);
	lua_pushstring(L, (*n)->relname);
	return 1;
}

static int
notify_pid(lua_State *L)
{
	PGnotify **n;

	n = luaL_checkudata(L, 1, NOTIFY_METATABLE);
	lua_pushinteger(L, (*n)->be_pid);
	return 1;
}

static int
notify_extra(lua_State *L)
{
	PGnotify **n;

	n = luaL_checkudata(L, 1, NOTIFY_METATABLE);
	lua_pushstring(L, (*n)->extra);
	return 1;
}

static int
notify_clear(lua_State *L)
{
	PGnotify **n;

	n = luaL_checkudata(L, 1, NOTIFY_METATABLE);
	if (*n) {
		PQfreemem(*n);
		*n = NULL;
	}
	return 0;
}

/*
 * Tuple and value functions
 */
static int
tuple_copy(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	int col;

	lua_newtable(L);
	for (col = 0; col < PQnfields(t->res); col++) {
		lua_pushstring(L, PQgetvalue(t->res, t->row, col));
		lua_setfield(L, -2, PQfname(t->res, col));
	}
	return 1;
}

static int
field_iterator(lua_State *L)
{
	field *f = luaL_checkudata(L, 1, FIELD_METATABLE);

	f->col++;

	if (f->col == PQnfields(f->tuple->res)) {
		lua_pushnil(L);
		lua_pushnil(L);
	} else {
		lua_pushstring(L, PQfname(f->tuple->res, f->col));
		lua_pushstring(L,
		    PQgetvalue(f->tuple->res, f->tuple->row, f->col));
	}
	return 2;
}

static int
tuple_getfields(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	field *f;

	lua_pushcfunction(L, field_iterator);
	f = lua_newuserdata(L, sizeof(field));
	f->tuple = t;
	f->col = -1;
	luaL_setmetatable(L, FIELD_METATABLE);
	return 2;
}

static int
tuple_getisnull(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	const char *fnam;
	int fnumber;

	switch (lua_type(L, 2)) {
	case LUA_TNUMBER:
		fnumber = lua_tointeger(L, 2) - 1;
		if (fnumber < 0 || fnumber >= PQnfields(t->res))
			lua_pushnil(L);
		else
			lua_pushboolean(L, PQgetisnull(t->res, t->row,
			    lua_tointeger(L, 2) - 1));
		break;
	case LUA_TSTRING:
		fnam = lua_tostring(L, 2);
		fnumber = PQfnumber(t->res, fnam);

		if (fnumber == -1)
			lua_pushnil(L);
		else
			lua_pushboolean(L, PQgetisnull(t->res, t->row,
			    PQfnumber(t->res, lua_tostring(L, 2))));
		break;
	default:
		lua_pushnil(L);
	}
	return 1;
}

static int
tuple_getlength(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	const char *fnam;
	int fnumber;

	switch (lua_type(L, 2)) {
	case LUA_TNUMBER:
		fnumber = lua_tointeger(L, 2) - 1;
		if (fnumber < 0 || fnumber >= PQnfields(t->res))
			lua_pushnil(L);
		else
			lua_pushinteger(L, PQgetlength(t->res, t->row,
			    lua_tointeger(L, 2) - 1));
		break;
	case LUA_TSTRING:
		fnam = lua_tostring(L, 2);
		fnumber = PQfnumber(t->res, fnam);

		if (fnumber == -1)
			lua_pushnil(L);
		else
			lua_pushinteger(L, PQgetlength(t->res, t->row,
			    PQfnumber(t->res, lua_tostring(L, 2))));
		break;
	default:
		lua_pushnil(L);
	}
	return 1;
}

static int
tuple_index(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);
	const char *fnam;
	int fnumber;

	switch (lua_type(L, 2)) {
	case LUA_TNUMBER:
		fnumber = lua_tointeger(L, 2) - 1;
		if (fnumber < 0 || fnumber >= PQnfields(t->res))
			lua_pushnil(L);
		else
			lua_pushstring(L, PQgetvalue(t->res, t->row, fnumber));
		break;
	case LUA_TSTRING:
		fnam = lua_tostring(L, 2);
		fnumber = PQfnumber(t->res, fnam);

		if (fnumber == -1) {
			if (!strcmp(fnam, "copy"))
				lua_pushcfunction(L, tuple_copy);
			else if (!strcmp(fnam, "getfields"))
				lua_pushcfunction(L, tuple_getfields);
			else if (!strcmp(fnam, "getisnull"))
				lua_pushcfunction(L, tuple_getisnull);
			else if (!strcmp(fnam, "getlength"))
				lua_pushcfunction(L, tuple_getlength);
			else
				lua_pushnil(L);
		} else
			lua_pushstring(L, PQgetvalue(t->res, t->row,
			    PQfnumber(t->res, fnam)));
		break;
	default:
		lua_pushnil(L);
	}
	return 1;
}

static int
tuple_length(lua_State *L)
{
	tuple *t = luaL_checkudata(L, 1, TUPLE_METATABLE);

	lua_pushinteger(L, PQnfields(t->res));
	return 1;
}

/*
 * Module definitions, constants etc.
 */
struct constant {
	char *name;
	int value;
};

static struct constant pgsql_constant[] = {
	/* Connection status */
	{ "CONNECTION_STARTED",		CONNECTION_STARTED },
	{ "CONNECTION_MADE",		CONNECTION_MADE },
	{ "CONNECTION_AWAITING_RESPONSE", CONNECTION_AWAITING_RESPONSE },
	{ "CONNECTION_AUTH_OK",		CONNECTION_AUTH_OK },
	{ "CONNECTION_OK",		CONNECTION_OK },
	{ "CONNECTION_SSL_STARTUP",	CONNECTION_SSL_STARTUP },
	{ "CONNECTION_SETENV",		CONNECTION_SETENV },
	{ "CONNECTION_BAD",		CONNECTION_BAD },
#if PG_VERSION_NUM >= 100000
	{ "CONNECTION_CONSUME",		CONNECTION_CONSUME },
#endif

	/* Resultset status codes */
	{ "PGRES_EMPTY_QUERY",		PGRES_EMPTY_QUERY },
	{ "PGRES_COMMAND_OK",		PGRES_COMMAND_OK },
	{ "PGRES_TUPLES_OK",		PGRES_TUPLES_OK },
#if PG_VERSION_NUM >= 140000
	{ "PGRES_PIPELINE_SYNC",	PGRES_PIPELINE_SYNC },
	{ "PGRES_PIPELINE_ABORTED",	PGRES_PIPELINE_ABORTED },
#endif
#if PG_VERSION_NUM >= 90200
	{ "PGRES_SINGLE_TUPLE",		PGRES_SINGLE_TUPLE },
#endif
	{ "PGRES_COPY_OUT",		PGRES_COPY_OUT },
	{ "PGRES_COPY_IN",		PGRES_COPY_IN },
#if PG_VERSION_NUM >= 90100
	{ "PGRES_COPY_BOTH",		PGRES_COPY_BOTH },
	{ "PGRES_SINGLE_TUPLE",		PGRES_SINGLE_TUPLE },
#endif
	{ "PGRES_BAD_RESPONSE",		PGRES_BAD_RESPONSE },
	{ "PGRES_NONFATAL_ERROR",	PGRES_NONFATAL_ERROR },
	{ "PGRES_FATAL_ERROR",		PGRES_FATAL_ERROR },

	/* Polling status */
	{ "PGRES_POLLING_FAILED",	PGRES_POLLING_FAILED },
	{ "PGRES_POLLING_READING",	PGRES_POLLING_READING },
	{ "PGRES_POLLING_WRITING",	PGRES_POLLING_WRITING },
	{ "PGRES_POLLING_OK",		PGRES_POLLING_OK },

	/* Transaction status */
	{ "PQTRANS_IDLE",		PQTRANS_IDLE },
	{ "PQTRANS_ACTIVE",		PQTRANS_ACTIVE },
	{ "PQTRANS_INTRANS",		PQTRANS_INTRANS },
	{ "PQTRANS_INERROR",		PQTRANS_INERROR },
	{ "PQTRANS_UNKNOWN",		PQTRANS_UNKNOWN },

	/* Diagnostic codes */
	{ "PG_DIAG_SEVERITY",		PG_DIAG_SEVERITY },
	{ "PG_DIAG_SQLSTATE",		PG_DIAG_SQLSTATE },
	{ "PG_DIAG_MESSAGE_PRIMARY",	PG_DIAG_MESSAGE_PRIMARY },
	{ "PG_DIAG_MESSAGE_DETAIL",	PG_DIAG_MESSAGE_DETAIL },
	{ "PG_DIAG_MESSAGE_HINT",	PG_DIAG_MESSAGE_HINT },
	{ "PG_DIAG_STATEMENT_POSITION",	PG_DIAG_STATEMENT_POSITION },
	{ "PG_DIAG_INTERNAL_POSITION",	PG_DIAG_INTERNAL_POSITION },
	{ "PG_DIAG_INTERNAL_QUERY",	PG_DIAG_INTERNAL_QUERY },
	{ "PG_DIAG_CONTEXT",		PG_DIAG_CONTEXT },
	{ "PG_DIAG_SOURCE_FILE",	PG_DIAG_SOURCE_FILE },
	{ "PG_DIAG_SOURCE_LINE",	PG_DIAG_SOURCE_LINE },
	{ "PG_DIAG_SOURCE_FUNCTION",	PG_DIAG_SOURCE_FUNCTION },

	/* Error verbosity */
	{ "PQERRORS_TERSE",		PQERRORS_TERSE },
	{ "PQERRORS_DEFAULT",		PQERRORS_DEFAULT },
	{ "PQERRORS_VERBOSE",		PQERRORS_VERBOSE },

#if PG_VERSION_NUM >= 90100
	/* PQping codes */
	{ "PQPING_OK",			PQPING_OK },
	{ "PQPING_REJECT",		PQPING_REJECT },
	{ "PQPING_NO_RESPONSE",		PQPING_NO_RESPONSE },
	{ "PQPING_NO_ATTEMPT",		PQPING_NO_ATTEMPT },
#endif

	/* Large objects */
	{ "INV_READ",			INV_READ },
	{ "INV_WRITE",			INV_WRITE },
	{ "SEEK_CUR",			SEEK_CUR },
	{ "SEEK_END",			SEEK_END },
	{ "SEEK_SET",			SEEK_SET },

	/* Miscellaneous values */
	{ "InvalidOid",			InvalidOid },

	{ NULL,				0 }
};

static void
pgsql_set_info(lua_State *L)
{
	lua_pushliteral(L, "_COPYRIGHT");
	lua_pushliteral(L, "Copyright (C) 2009 - 2021 by "
	    "micro systems marc balmer");
	lua_settable(L, -3);
	lua_pushliteral(L, "_DESCRIPTION");
	lua_pushliteral(L, "PostgreSQL binding for Lua");
	lua_settable(L, -3);
	lua_pushliteral(L, "_VERSION");
	lua_pushliteral(L, "pgsql 1.7.0");
	lua_settable(L, -3);
}

int
luaopen_pgsql(lua_State *L)
{
	int n;
	struct luaL_Reg luapgsql[] = {
		/* Database Connection Control Functions */
		{ "connectdb", pgsql_connectdb },
		{ "connectStart", pgsql_connectStart },
		{ "libVersion", pgsql_libVersion },
#if PG_VERSION_NUM >= 90100
		{ "ping", pgsql_ping },
#endif
		{ "encryptPassword", pgsql_encryptPassword },
		{ "unescapeBytea", pgsql_unescapeBytea },

		/* SSL support */
		{ "initOpenSSL", pgsql_initOpenSSL },
		{ NULL, NULL }
	};

	struct luaL_Reg conn_methods[] = {
		/* Database Connection Control Functions */
		{ "connectPoll", pgsql_connectPoll },
		{ "finish", conn_finish },
		{ "reset", conn_reset },
		{ "resetStart", conn_resetStart },
		{ "resetPoll", conn_resetPoll },

		/* Connection Status Functions */
		{ "db", conn_db },
		{ "user", conn_user },
		{ "pass", conn_pass },
		{ "host", conn_host },
		{ "port", conn_port },
		{ "tty", conn_tty },
		{ "options", conn_options },
		{ "status", conn_status },
		{ "transactionStatus", conn_transactionStatus },
		{ "parameterStatus", conn_parameterStatus },
		{ "protocolVersion", conn_protocolVersion },
		{ "serverVersion", conn_serverVersion },
		{ "errorMessage", conn_errorMessage },
		{ "socket", conn_socket },
		{ "backendPID", conn_backendPID },
		{ "connectionNeedsPassword", conn_connectionNeedsPassword },
		{ "connectionUsedPassword", conn_connectionUsedPassword },
#if PG_VERSION_NUM >= 90500
		{ "sslInUse", conn_sslInUse },
		{ "sslAttribute", conn_sslAttribute },
		{ "sslAttributeNames", conn_sslAttributeNames },
#endif

		/* Command Execution Functions */
		{ "escapeString", conn_escapeString },
		{ "escapeLiteral", conn_escapeLiteral },
		{ "escapeIdentifier", conn_escapeIdentifier },
		{ "escapeBytea", conn_escapeBytea },
		{ "exec", conn_exec },
		{ "execParams", conn_execParams },
		{ "prepare", conn_prepare },
		{ "execPrepared", conn_execPrepared },
		{ "describePrepared", conn_describePrepared },
		{ "describePortal", conn_describePortal },

		/* Asynchronous command processing */
		{ "sendQuery", conn_sendQuery },
		{ "sendQueryParams", conn_sendQueryParams },
		{ "sendPrepare", conn_sendPrepare },
		{ "sendQueryPrepared", conn_sendQueryPrepared },
		{ "sendDescribePrepared", conn_sendDescribePrepared },
		{ "sendDescribePortal", conn_sendDescribePortal },
		{ "getResult", conn_getResult },
		{ "cancel", conn_cancel },

#if PG_VERSION_NUM >= 90200
		/* Retrieving query results row-by-row */
		{ "setSingleRowMode", conn_setSingleRowMode },
#endif

		/* Asynchronous Notifications Functions */
		{ "notifies", conn_notifies },

		/* Function associated with the COPY command */
		{ "putCopyData", conn_putCopyData },
		{ "putCopyEnd", conn_putCopyEnd },
		{ "getCopyData", conn_getCopyData },

		/* Control Functions */
		{ "clientEncoding", conn_clientEncoding },
		{ "setClientEncoding", conn_setClientEncoding },
		{ "setErrorVerbosity", conn_setErrorVerbosity },
		{ "trace", conn_trace },
		{ "untrace", conn_untrace },

		/* Miscellaneous Functions */
		{ "consumeInput", conn_consumeInput },
		{ "isBusy", conn_isBusy },
		{ "setnonblocking", conn_setnonblocking },
		{ "isnonblocking", conn_isnonblocking },
		{ "flush", conn_flush },
#if PG_VERSION_NUM >= 100000
		{ "encryptPassword", conn_encryptPassword },
#endif
		/* Notice processing */
		{ "setNoticeReceiver", conn_setNoticeReceiver },
		{ "setNoticeProcessor", conn_setNoticeProcessor },

		/* Large Objects */
		{ "lo_create", conn_lo_create },
		{ "lo_import", conn_lo_import },
		{ "lo_import_with_oid", conn_lo_import_with_oid },
		{ "lo_export", conn_lo_export },
		{ "lo_open", conn_lo_open },
		{ "lo_write", conn_lo_write },
		{ "lo_read", conn_lo_read },
		{ "lo_lseek", conn_lo_lseek },
		{ "lo_tell", conn_lo_tell },
		{ "lo_truncate", conn_lo_truncate },
		{ "lo_close", conn_lo_close },
		{ "lo_unlink", conn_lo_unlink },
#if PG_VERSION_NUM >= 90300
		{ "lo_lseek64", conn_lo_lseek64 },
		{ "lo_tell64", conn_lo_tell64 },
		{ "lo_truncate64", conn_lo_truncate64 },
#endif
		{ NULL, NULL }
	};
	struct luaL_Reg res_methods[] = {
		/* Main functions */
		{ "status", res_status },
		{ "resStatus", res_resStatus },
		{ "errorMessage", res_errorMessage },
		{ "errorField", res_errorField },

		/* Retrieving query result information */
		{ "ntuples", res_ntuples },
		{ "nfields", res_nfields },
		{ "fname", res_fname },
		{ "fnumber", res_fnumber },
		{ "ftable", res_ftable },
		{ "ftablecol", res_ftablecol },
		{ "fformat", res_fformat },
		{ "ftype", res_ftype },
		{ "fmod", res_fmod },
		{ "fsize", res_fsize },
		{ "binaryTuples", res_binaryTuples },
		{ "getvalue", res_getvalue },
		{ "getisnull", res_getisnull },
		{ "getlength", res_getlength },
		{ "nparams", res_nparams },
		{ "paramtype", res_paramtype },

		/* Other result information */
		{ "cmdStatus", res_cmdStatus },
		{ "cmdTuples", res_cmdTuples },
		{ "oidValue", res_oidValue },
		{ "oidStatus", res_oidStatus },

		/* Lua specific extension */
		{ "copy", res_copy },
		{ "fields", res_fields },
		{ "tuples", res_tuples },
		{ "clear", res_clear },
		{ NULL, NULL }
	};
	struct luaL_Reg notify_methods[] = {
		{ "relname", notify_relname },
		{ "pid", notify_pid },
		{ "extra", notify_extra },
		{ NULL, NULL }
	};
	if (luaL_newmetatable(L, CONN_METATABLE)) {
#if LUA_VERSION_NUM >= 502
		luaL_setfuncs(L, conn_methods, 0);
#else
		luaL_register(L, NULL, conn_methods);
#endif
		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, conn_finish);
		lua_settable(L, -3);

		lua_pushliteral(L, "__index");
		lua_pushvalue(L, -2);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, RES_METATABLE)) {
#if LUA_VERSION_NUM >= 502
		luaL_setfuncs(L, res_methods, 0);
#else
		luaL_register(L, NULL, res_methods);
#endif
		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, res_clear);
		lua_settable(L, -3);

#if LUA_VERSION_NUM >= 504
		lua_pushliteral(L, "__close");
		lua_pushcfunction(L, res_clear);
		lua_settable(L, -3);
#endif
		lua_pushliteral(L, "__index");
		lua_pushcfunction(L, res_index);
		lua_settable(L, -3);

		lua_pushliteral(L, "__len");
		lua_pushcfunction(L, res_ntuples);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, NOTIFY_METATABLE)) {
#if LUA_VERSION_NUM >= 502
		luaL_setfuncs(L, notify_methods, 0);
#else
		luaL_register(L, NULL, notify_methods);
#endif
		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, notify_clear);
		lua_settable(L, -3);

#if LUA_VERSION_NUM >= 504
		lua_pushliteral(L, "__close");
		lua_pushcfunction(L, notify_clear);
		lua_settable(L, -3);
#endif

		lua_pushliteral(L, "__index");
		lua_pushvalue(L, -2);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, TUPLE_METATABLE)) {
		lua_pushliteral(L, "__index");
		lua_pushcfunction(L, tuple_index);
		lua_settable(L, -3);

		lua_pushliteral(L, "__len");
		lua_pushcfunction(L, tuple_length);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, FIELD_METATABLE)) {
		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, GCMEM_METATABLE)) {
		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, gcmem_clear);
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

#if LUA_VERSION_NUM >= 502
	luaL_newlib(L, luapgsql);
#else
	luaL_register(L, "pgsql", luapgsql);
#endif
	pgsql_set_info(L);
	for (n = 0; pgsql_constant[n].name != NULL; n++) {
		lua_pushinteger(L, pgsql_constant[n].value);
		lua_setfield(L, -2, pgsql_constant[n].name);
	};

	return 1;
}
