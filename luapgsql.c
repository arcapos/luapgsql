/*
 * Copyright (c) 2009 - 2012, Micro Systems Marc Balmer, CH-5073 Gipf-Oberfrick
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <unistd.h>

#include <postgres.h>
#include <libpq-fe.h>
#include <libpq/libpq-fs.h>
#include <pg_config.h>
#include <catalog/pg_type.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#ifdef __linux__
#include <bsd/bsd.h>
#endif

#include "luapgsql.h"

static size_t
PQescape(PGconn *conn, char *dst, const char *from, size_t size)
{
	size_t length, newsiz;
	int error = 0;
	char *buf;

	length = 2 * strlen(from) + 1;
	buf = malloc(length);
	if (buf != NULL) {
		if (conn == NULL)
			newsiz = PQescapeString(buf, from, length);
		else
			newsiz = PQescapeStringConn(conn, buf, from, length,
			    &error);
		if (error) {
			syslog(LOG_ERR, "error escaping string: %s",
				PQerrorMessage(conn));
			free(buf);
			return strlcpy(dst, "", size);
		}
		if (newsiz > size - 1) {
			syslog(LOG_ERR, "target buffer to small for escaped "
			    "string");
			free(buf);
			return strlcpy(dst, "", size);
		}
	} else {
		syslog(LOG_ERR, "memory allocation error");
		return strlcpy(dst, "", size);
	}
	length = strlcpy(dst, buf, size);
	free(buf);
	return length;
}

/*
 * Database Connection Control Functions
 */
static int
pgsql_connectdb(lua_State *L)
{
	PGconn *conn, **data;

	conn = PQconnectdb(luaL_checkstring(L, -1));
	if (conn != NULL) {
		data = (PGconn **)lua_newuserdata(L, sizeof(PGconn *));
		*data = conn;
		luaL_getmetatable(L, CONN_METATABLE);
		lua_setmetatable(L, -2);
	} else
		lua_pushnil(L);
	return 1;
}

static int
pgsql_connectStart(lua_State *L)
{
	PGconn **data;

	data = (PGconn **)lua_newuserdata(L, sizeof(PGconn *));
	*data = PQconnectStart(luaL_checkstring(L, -2));
	luaL_getmetatable(L, CONN_METATABLE);
	lua_setmetatable(L, -2);
	return 1;
}

static int
pgsql_connectPoll(lua_State *L)
{
	lua_pushinteger(L,
	    PQconnectPoll(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
pgsql_libVersion(lua_State *L)
{
	lua_pushinteger(L, PQlibVersion());
	return 1;
}

static int
pgsql_ping(lua_State *L)
{
	lua_pushinteger(L, PQping(luaL_checkstring(L, 1)));
	return 1;
}

static int
pgsql_encryptPassword(lua_State *L)
{
	char *encrypted;

	encrypted = PQencryptPassword(luaL_checkstring(L, 1),
	    luaL_checkstring(L, 2));
	if (encrypted != NULL) {
		lua_pushstring(L, encrypted);
		PQfreemem(encrypted);
	} else
		lua_pushnil(L);
	return 1;
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
		} else
			lua_pop(L, 1);
	}
	return 0;
}

static int
conn_reset(lua_State *L)
{
	PQreset(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE));
	return 0;
}

static int
conn_resetStart(lua_State *L)
{
	lua_pushinteger(L,
	    PQresetStart(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_resetPoll(lua_State *L)
{
	lua_pushinteger(L,
	    PQresetPoll(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

/*
 * Connection status functions
 */
static int
conn_db(lua_State *L)
{
	lua_pushstring(L,
	    PQdb(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_user(lua_State *L)
{
	lua_pushstring(L,
	    PQuser(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_pass(lua_State *L)
{
	lua_pushstring(L,
	    PQpass(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_host(lua_State *L)
{
	lua_pushstring(L,
	    PQhost(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_port(lua_State *L)
{
	lua_pushstring(L,
	    PQport(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_tty(lua_State *L)
{
	lua_pushstring(L,
	    PQtty(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_options(lua_State *L)
{
	lua_pushstring(L,
	    PQoptions(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_status(lua_State *L)
{
	lua_pushinteger(L,
	    PQstatus(*(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

static int
conn_transactionStatus(lua_State *L)
{
	lua_pushinteger(L,
	    PQtransactionStatus(*(PGconn **)luaL_checkudata(L, -1,
	    CONN_METATABLE)));
	return 1;
}

static int
conn_parameterStatus(lua_State *L)
{
	const char *status;

	status = PQparameterStatus(
	    *(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE),
	    luaL_checkstring(L, -2));
	if (status == NULL)
		lua_pushnil(L);
	else
		lua_pushstring(L, status);
	return 1;
}

static int
conn_protocolVersion(lua_State *L)
{
	lua_pushinteger(L,
	    PQprotocolVersion(*(PGconn **)luaL_checkudata(L, -1,
	    CONN_METATABLE)));
	return 1;
}

static int
conn_serverVersion(lua_State *L)
{
	lua_pushinteger(L,
	    PQserverVersion(*(PGconn **)luaL_checkudata(L, -1,
	    CONN_METATABLE)));
	return 1;
}

static int
conn_errorMessage(lua_State *L)
{
	lua_pushstring(L,
	    PQerrorMessage(*(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

static int
conn_socket(lua_State *L)
{
	lua_pushinteger(L,
	    PQsocket(*(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

static int
conn_backendPID(lua_State *L)
{
	lua_pushinteger(L,
	    PQbackendPID(*(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

static int
conn_connectionNeedsPassword(lua_State *L)
{
	lua_pushboolean(L, PQconnectionNeedsPassword(
	    *(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

static int
conn_connectionUsedPassword(lua_State *L)
{
	lua_pushboolean(L, PQconnectionUsedPassword(
	    *(PGconn **)luaL_checkudata(L, -1, CONN_METATABLE)));
	return 1;
}

/*
 * Command Execution Functions
 */
static int
conn_exec(lua_State *L)
{
	PGresult **res;

	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexec(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2));
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	return 1;
}

static int
conn_execParams(lua_State *L)
{
	PGresult **res;
	Oid *paramTypes;
	char **paramValues;
	int n, nParams;

	nParams = lua_gettop(L) - 2;	/* subtract connection and command */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramTypes = calloc(nParams, sizeof(Oid));
		paramValues = calloc(nParams, sizeof(char *));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 3 + n)) {
			case LUA_TBOOLEAN:
				paramTypes[n] = BOOLOID;
				if (lua_toboolean(L, 3 + n))
					paramValues[n] = strdup("true");
				else
					paramValues[n] = strdup("false");
				break;
			case LUA_TNUMBER:
				paramTypes[n] = NUMERICOID;
				asprintf(&paramValues[n], "%f",
				    lua_tonumber(L, 3 + n));
				break;
			case LUA_TSTRING:
				paramTypes[n] = TEXTOID;
				paramValues[n] = strdup(lua_tostring(L, 3 + n));
				break;
			case LUA_TNIL:
				paramValues[n] = NULL;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				for (nParams = 0; nParams < n; nParams++)
					if (paramValues[nParams] != NULL)
						free(paramValues[nParams]);
				free(paramValues);
				free(paramTypes);
				return luaL_argerror(L, 3 + n,
				    "unsupported type");
			}
		}
	} else {
		paramTypes = NULL;
		paramValues = NULL;
	}
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexecParams(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), nParams, paramTypes,
	    (const char * const*)paramValues, NULL, NULL, 0);
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	if (nParams) {
		for (n = 0; n < nParams; n++)
			if (paramValues[n] != NULL)
				free((void *)paramValues[n]);
		free(paramTypes);
		free(paramValues);
	}
	return 1;
}

static int
conn_prepare(lua_State *L)
{
	PGresult **res;
	Oid *paramTypes;
	int n, nParams;

	nParams = lua_gettop(L) - 3;	/* subtract connection, name, command */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramTypes = calloc(nParams, sizeof(Oid));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 4 + n)) {
			case LUA_TBOOLEAN:
				paramTypes[n] = BOOLOID;
				break;
			case LUA_TNUMBER:
				paramTypes[n] = NUMERICOID;
				break;
			case LUA_TSTRING:
				paramTypes[n] = TEXTOID;
				break;
			case LUA_TNIL:
				paramTypes[n] = 0;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				free(paramTypes);
				return luaL_argerror(L, 4 + n,
				    "unsupported type");
			}
		}
	} else
		paramTypes = NULL;
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQprepare(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), luaL_checkstring(L, 3), nParams,
	    paramTypes);
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	if (nParams)
		free(paramTypes);
	return 1;
}

static int
conn_execPrepared(lua_State *L)
{
	PGresult **res;
	char **paramValues;
	int n, nParams;

	nParams = lua_gettop(L) - 2;	/* subtract connection and name */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramValues = calloc(nParams, sizeof(char *));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 3 + n)) {
			case LUA_TBOOLEAN:
				if (lua_toboolean(L, 3 + n))
					paramValues[n] = strdup("true");
				else
					paramValues[n] = strdup("false");
				break;
			case LUA_TNUMBER:
				asprintf(&paramValues[n], "%f",
				    lua_tonumber(L, 3 + n));
				break;
			case LUA_TSTRING:
				paramValues[n] = strdup(lua_tostring(L, 3 + n));
				break;
			case LUA_TNIL:
				paramValues[n] = NULL;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				for (nParams = 0; nParams < n; nParams++)
					if (paramValues[nParams] != NULL)
						free(paramValues[nParams]);
				free(paramValues);
				return luaL_argerror(L, 3 + n,
				    "unsupported type");
			}
		}
	} else
		paramValues = NULL;
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQexecPrepared(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), nParams, (const char * const*)paramValues,
	    NULL, NULL, 0);
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	if (nParams) {
		for (n = 0; n < nParams; n++)
			if (paramValues[n] != NULL)
				free((void *)paramValues[n]);
		free(paramValues);
	}
	return 1;
}

static int
conn_describePrepared(lua_State *L)
{
	PGresult **res;
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQdescribePrepared(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2));
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	return 1;
}

static int
conn_describePortal(lua_State *L)
{
	PGresult **res;
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = PQdescribePortal(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2));
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);
	return 1;
}

static int
conn_escape(lua_State *L)
{
	char buf[1024];
	const char *str;
	PGconn **d;

	d = luaL_checkudata(L, 1, CONN_METATABLE);

	str = lua_tostring(L, 2);
	if (str != NULL) {
		PQescape(*d, buf, str, sizeof(buf));
		lua_pushstring(L, buf);
	} else
		lua_pushnil(L);
	return 1;
}

static int
conn_escapeLiteral(lua_State *L)
{
	const char *s;
	char *p;
	PGconn **d;

	d = luaL_checkudata(L, 1, CONN_METATABLE);	
	s = lua_tostring(L, 2);
	p = PQescapeLiteral(*d, s, strlen(s));
	lua_pushstring(L, p);
	PQfreemem(p);
	return 1;
}

static int
conn_escapeIdentifier(lua_State *L)
{
	const char *s;
	char *p;
	PGconn **d;

	d = luaL_checkudata(L, 1, CONN_METATABLE);	
	s = lua_tostring(L, 2);
	p = PQescapeIdentifier(*d, s, strlen(s));
	lua_pushstring(L, p);
	PQfreemem(p);
	return 1;
}

/*
 * Asynchronous Command Execution Functions
 */
static int
conn_sendQuery(lua_State *L)
{
	lua_pushinteger(L,
	    PQsendQuery(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_sendQueryParams(lua_State *L)
{
	Oid *paramTypes;
	char **paramValues;
	int n, nParams;

	nParams = lua_gettop(L) - 2;	/* subtract connection and command */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramTypes = calloc(nParams, sizeof(Oid));
		paramValues = calloc(nParams, sizeof(char *));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 3 + n)) {
			case LUA_TBOOLEAN:
				paramTypes[n] = BOOLOID;
				if (lua_toboolean(L, 3 + n))
					paramValues[n] = strdup("true");
				else
					paramValues[n] = strdup("false");
				break;
			case LUA_TNUMBER:
				paramTypes[n] = NUMERICOID;
				asprintf(&paramValues[n], "%f",
				    lua_tonumber(L, 3 + n));
				break;
			case LUA_TSTRING:
				paramTypes[n] = TEXTOID;
				paramValues[n] = strdup(lua_tostring(L, 3 + n));
				break;
			case LUA_TNIL:
				paramValues[n] = NULL;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				for (nParams = 0; nParams < n; nParams++)
					if (paramValues[nParams] != NULL)
						free(paramValues[nParams]);
				free(paramValues);
				free(paramTypes);
				return luaL_argerror(L, 3 + n,
				    "unsupported type");
			}
		}
	} else {
		paramTypes = NULL;
		paramValues = NULL;
	}
	lua_pushinteger(L,
	    PQsendQueryParams(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), nParams, paramTypes,
	    (const char * const*)paramValues, NULL, NULL, 0));
	if (nParams) {
		for (n = 0; n < nParams; n++)
			if (paramValues[n] != NULL)
				free((void *)paramValues[n]);
		free(paramTypes);
		free(paramValues);
	}
	return 1;
}

static int
conn_sendPrepare(lua_State *L)
{
	Oid *paramTypes;
	int n, nParams;

	nParams = lua_gettop(L) - 3;	/* subtract connection, name, command */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramTypes = calloc(nParams, sizeof(Oid));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 4 + n)) {
			case LUA_TBOOLEAN:
				paramTypes[n] = BOOLOID;
				break;
			case LUA_TNUMBER:
				paramTypes[n] = NUMERICOID;
				break;
			case LUA_TSTRING:
				paramTypes[n] = TEXTOID;
				break;
			case LUA_TNIL:
				paramTypes[n] = 0;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				free(paramTypes);
				return luaL_argerror(L, 4 + n,
				    "unsupported type");
			}
		}
	} else
		paramTypes = NULL;
	lua_pushinteger(L,
	    PQsendPrepare(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), luaL_checkstring(L, 3), nParams,
	    paramTypes));
	if (nParams)
		free(paramTypes);
	return 1;
}

static int
conn_sendQueryPrepared(lua_State *L)
{
	char **paramValues;
	int n, nParams;

	nParams = lua_gettop(L) - 2;	/* subtract connection and name */
	if (nParams < 0)
		nParams = 0;
	if (nParams) {
		paramValues = calloc(nParams, sizeof(char *));

		for (n = 0; n < nParams; n++) {
			switch (lua_type(L, 3 + n)) {
			case LUA_TBOOLEAN:
				if (lua_toboolean(L, 3 + n))
					paramValues[n] = strdup("true");
				else
					paramValues[n] = strdup("false");
				break;
			case LUA_TNUMBER:
				asprintf(&paramValues[n], "%f",
				    lua_tonumber(L, 3 + n));
				break;
			case LUA_TSTRING:
				paramValues[n] = strdup(lua_tostring(L, 3 + n));
				break;
			case LUA_TNIL:
				paramValues[n] = NULL;
				break;
			case LUA_TFUNCTION:
			case LUA_TTABLE:
			case LUA_TUSERDATA:
			case LUA_TTHREAD:
			case LUA_TLIGHTUSERDATA:
				for (nParams = 0; nParams < n; nParams++)
					if (paramValues[nParams] != NULL)
						free(paramValues[nParams]);
				free(paramValues);
				return luaL_argerror(L, 3 + n,
				    "unsupported type");
			}
		}
	} else
		paramValues = NULL;
	lua_pushinteger(L,
	    PQsendQueryPrepared(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), nParams, (const char * const*)paramValues,
	    NULL, NULL, 0));
	if (nParams) {
		for (n = 0; n < nParams; n++)
			if (paramValues[n] != NULL)
				free((void *)paramValues[n]);
		free(paramValues);
	}
	return 1;
}

static int
conn_sendDescribePrepared(lua_State *L)
{
	lua_pushinteger(L,
	    PQsendDescribePrepared(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_sendDescribePortal(lua_State *L)
{
	lua_pushinteger(L,
	    PQsendDescribePortal(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_getResult(lua_State *L)
{
	PGresult *r, **res;

	r = PQgetResult(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE));
	if (r == NULL)
		lua_pushnil(L);
	else {
		res = lua_newuserdata(L, sizeof(PGresult *));
		*res = r;
		luaL_getmetatable(L, RES_METATABLE);
		lua_setmetatable(L, -2);
	}
	return 1;
}

static int
conn_cancel(lua_State *L)
{
	PGconn **d;
	PGcancel *cancel;
	char errbuf[256];
	int res = 1;

	d = luaL_checkudata(L, 1, CONN_METATABLE);	
	cancel = PQgetCancel(*d);
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

/*
 * Asynchronous Notification Functions
 */
static int
conn_notifies(lua_State *L)
{
	PGnotify **notify, *n;

	n = PQnotifies(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE));
	if (n == NULL)
		lua_pushnil(L);
	else {
		notify = lua_newuserdata(L, sizeof(PGnotify *));
		*notify = n;
		luaL_getmetatable(L, NOTIFY_METATABLE);
		lua_setmetatable(L, -2);
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

	data = luaL_checkstring(L, 2);
	lua_pushinteger(L,
	    PQputCopyData(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    data, strlen(data)));
	return 1;
}

static int
conn_putCopyEnd(lua_State *L)
{
	lua_pushinteger(L,
	    PQputCopyEnd(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    NULL));
	return 1;
}

static int
conn_getCopyData(lua_State *L)
{
	int res;
	char *data;

	res = PQgetCopyData(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    &data, 0);
	if (res > 0)
		lua_pushstring(L, data);
	else
		lua_pushnil(L);
	if (data)
		PQfreemem(data);
	return 1;
}

/*
 * Control functions
 */
static int
conn_clientEncoding(lua_State *L)
{
	lua_pushstring(L,
	    pg_encoding_to_char(PQclientEncoding(
		*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE))));
	return 1;
}

static int
conn_setClientEncoding(lua_State *L)
{
	if (PQsetClientEncoding(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2)))
		lua_pushboolean(L, 0);
	else
		lua_pushboolean(L, 1);
	return 1;
}

static int
conn_setErrorVerbosity(lua_State *L)
{
	lua_pushinteger(L,
	    PQsetErrorVerbosity(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkinteger(L, 2)));
	return 1;
}

/*
 * Miscellaneous Functions
 */
static int
conn_consumeInput(lua_State *L)
{
	lua_pushboolean(L,
	    PQconsumeInput(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_isBusy(lua_State *L)
{
	lua_pushboolean(L,
	    PQisBusy(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_setnonblocking(lua_State *L)
{
	lua_pushinteger(L,
	    PQsetnonblocking(*(PGconn **)luaL_checkudata(L, 1,
	    CONN_METATABLE), lua_toboolean(L, 2)));
	return 1;
}

static int
conn_isnonblocking(lua_State *L)
{
	lua_pushboolean(L,
	    PQisnonblocking(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

static int
conn_flush(lua_State *L)
{
	lua_pushinteger(L,
	    PQflush(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE)));
	return 1;
}

/* Notice processing */
static void
noticeReceiver(void *arg, const PGresult *r)
{
	lua_State *L = (lua_State *)arg;
	PGresult **res;

	lua_pushstring(L, "__pgsqlNoticeReceiver");
	lua_rawget(L, LUA_REGISTRYINDEX);
	res = lua_newuserdata(L, sizeof(PGresult *));
	*res = (PGresult *)r;
	luaL_getmetatable(L, RES_METATABLE);
	lua_setmetatable(L, -2);

	if (lua_pcall(L, 1, 0, 0))
		luaL_error(L, "%s", lua_tostring(L, -1));
	*res = NULL;	/* avoid double free */
}

static void
noticeProcessor(void *arg, const char *message)
{
	lua_State *L = (lua_State *)arg;

	lua_pushstring(L, "__pgsqlNoticeProcessor");
	lua_rawget(L, LUA_REGISTRYINDEX);
	lua_pushstring(L, message);
	if (lua_pcall(L, 1, 0, 0))
		luaL_error(L, "%s", lua_tostring(L, -1));
}

static int
conn_setNoticeReceiver(lua_State *L)
{
	lua_pushstring(L, "__pgsqlNoticeReceiver");
	lua_pushvalue(L, -2);
	lua_rawset(L, LUA_REGISTRYINDEX);
	PQsetNoticeReceiver(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    noticeReceiver, L);
	return 0;
}

static int
conn_setNoticeProcessor(lua_State *L)
{
	lua_pushstring(L, "__pgsqlNoticeProcessor");
	lua_pushvalue(L, -2);
	lua_rawset(L, LUA_REGISTRYINDEX);
	PQsetNoticeProcessor(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    noticeProcessor, L);
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
	lua_pushinteger(L,
	    lo_create(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE), oid));
	return 1;
}

static int
conn_lo_import(lua_State *L)
{
	lua_pushinteger(L,
	    lo_import(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2)));
	return 1;
}

static int
conn_lo_import_with_oid(lua_State *L)
{
	lua_pushinteger(L,
	    lo_import_with_oid(
	    *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkstring(L, 2), luaL_checkinteger(L, 3)));
	return 1;
}

static int
conn_lo_export(lua_State *L)
{
	lua_pushinteger(L,
	    lo_export(*(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE),
	    luaL_checkinteger(L, 2), luaL_checkstring(L, 3)));
	return 1;
}

static int
conn_lo_open(lua_State *L)
{
	largeObject **o;

	o = lua_newuserdata(L, sizeof(largeObject *));
	(*o)->conn = *(PGconn **)luaL_checkudata(L, 1, CONN_METATABLE);
	(*o)->fd = lo_open((*o)->conn, luaL_checkinteger(L, 2),
	    luaL_checkinteger(L, 3));
	luaL_getmetatable(L, LO_METATABLE);
	lua_setmetatable(L, -2);
	return 1;
}

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
	lua_pushstring(L,
	    PQresStatus(luaL_checkinteger(L, 2)));
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
	    luaL_checkint(L, 2) - 1));
	return 1;
}

static int
res_fnumber(lua_State *L)
{
	lua_pushinteger(L,
	    PQfnumber(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkstring(L, 2)) - 1);
	return 1;
}

static int
res_ftable(lua_State *L)
{
	lua_pushinteger(L,
	    PQftable(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_ftablecol(lua_State *L)
{
	lua_pushinteger(L,
	    PQftablecol(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_fformat(lua_State *L)
{
	lua_pushinteger(L,
	    PQfformat(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_ftype(lua_State *L)
{
	lua_pushinteger(L,
	    PQftype(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_fmod(lua_State *L)
{
	lua_pushinteger(L,
	    PQfmod(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_fsize(lua_State *L)
{
	lua_pushinteger(L,
	    PQfsize(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkinteger(L, 2)) - 1);
	return 1;
}

static int
res_binaryTuples(lua_State *L)
{
	lua_pushinteger(L,
	    PQbinaryTuples(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_getvalue(lua_State *L)
{
	lua_pushstring(L,
	    PQgetvalue(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkint(L, 2) - 1, luaL_checkint(L, 3) - 1));
	return 1;
}

static int
res_getisnull(lua_State *L)
{
	lua_pushinteger(L,
	    PQgetisnull(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkint(L, 2) - 1, luaL_checkint(L, 3) - 1));
	return 1;
}

static int
res_getlength(lua_State *L)
{
	lua_pushinteger(L,
	    PQgetlength(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkint(L, 2) - 1, luaL_checkint(L, 3) - 1));
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

static int
res_clear(lua_State *L)
{
	PGresult **r;

	r = luaL_checkudata(L, 1, RES_METATABLE);
	if (r && *r)  {
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
	if (*n)  {
		PQfreemem(*n);
		*n = NULL;
	}
	return 0;
}

/*
 * Large object functions
 */
static int
pgsql_lo_write(lua_State *L)
{
	largeObject **o;
	const char *s;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	s = lua_tostring(L, 2);
	lua_pushinteger(L, lo_write((*o)->conn, (*o)->fd, s, strlen(s)));
	return 1;
}

static int
pgsql_lo_read(lua_State *L)
{
	largeObject **o;
	int res;
	char buf[256];	/* arbitrary size */

	o = luaL_checkudata(L, 1, LO_METATABLE);
	res = lo_read((*o)->conn, (*o)->fd, buf, sizeof buf);
	lua_pushstring(L, buf);
	lua_pushinteger(L, res);
	return 2;
}

static int
pgsql_lo_lseek(lua_State *L)
{
	largeObject **o;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	lua_pushinteger(L, lo_lseek((*o)->conn, (*o)->fd,
	    luaL_checkinteger(L, 2), luaL_checkinteger(L, 3)));
	return 1;
}

static int
pgsql_lo_tell(lua_State *L)
{
	largeObject **o;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	lua_pushinteger(L, lo_tell((*o)->conn, (*o)->fd));
	return 1;
}

static int
pgsql_lo_truncate(lua_State *L)
{
	largeObject **o;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	lua_pushinteger(L, lo_truncate((*o)->conn, (*o)->fd,
	    luaL_checkinteger(L, 2)));
	return 1;
}

static int
pgsql_lo_close(lua_State *L)
{
	largeObject **o;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	lua_pushinteger(L, lo_close((*o)->conn, (*o)->fd));
	*o = NULL;	/* prevent close during garbage collection time */
	return 1;
}

static int
pgsql_lo_clear(lua_State *L)
{
	largeObject **o;

	o = luaL_checkudata(L, 1, LO_METATABLE);
	if (*o)  {
		lo_close((*o)->conn, (*o)->fd);
		*o = NULL;
	}
	return 0;
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

	/* Resultset status codes */
	{ "PGRES_EMPTY_QUERY",		PGRES_EMPTY_QUERY },
	{ "PGRES_COMMAND_OK",		PGRES_COMMAND_OK },
	{ "PGRES_TUPLES_OK",		PGRES_TUPLES_OK },
	{ "PGRES_COPY_OUT",		PGRES_COPY_OUT },
	{ "PGRES_COPY_IN",		PGRES_COPY_IN },
#if PG_VERSION_NUM >= 90100
	{ "PGRES_COPY_BOTH",		PGRES_COPY_BOTH },
#endif
	{ "PGRES_BAD_RESPONSE",		PGRES_BAD_RESPONSE },
	{ "PGRES_NONFATAL_ERROR",	PGRES_NONFATAL_ERROR },
	{ "PGRES_FATAL_ERROR",		PGRES_FATAL_ERROR },

	/* Transaction Status */
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
	{ "PQERRORS_TESRE",		PQERRORS_TERSE },
	{ "PQERRORS_DEFAULT",		PQERRORS_DEFAULT },
	{ "PQERRORS_VERBOSE",		PQERRORS_VERBOSE },

	/* PQping codes */
	{ "PQPING_OK",			PQPING_OK },
	{ "PQPING_REJECT",		PQPING_REJECT },
	{ "PQPING_NO_RESPONSE",		PQPING_NO_RESPONSE },
	{ "PQPING_NO_ATTEMPT",		PQPING_NO_ATTEMPT },

	/* Large objects */
	{ "INV_READ",			INV_READ },
	{ "INV_WRITE",			INV_WRITE },
	{ "SEEK_CUR",			SEEK_CUR },
	{ "SEEK_END",			SEEK_END },
	{ "SEEK_SET",			SEEK_SET },

	{ NULL,				0 }
};

static void
pgsql_set_info(lua_State *L)
{
	lua_pushliteral(L, "_COPYRIGHT");
	lua_pushliteral(L, "Copyright (C) 2011, 2012 by "
	    "micro systems marc balmer");
	lua_settable(L, -3);
	lua_pushliteral(L, "_DESCRIPTION");
	lua_pushliteral(L, "PostgreSQL binding for Lua");
	lua_settable(L, -3);
	lua_pushliteral(L, "_VERSION");
	lua_pushliteral(L, "pgsql 1.2.0");
	lua_settable(L, -3);
}

int
luaopen_pgsql(lua_State *L)
{
	int n;
	struct luaL_reg luapgsql[] = {
		/* Database Connection Control Functions */
		{ "connectdb", pgsql_connectdb },
		{ "connectStart", pgsql_connectStart },
		{ "connectPoll", pgsql_connectPoll },
		{ "libVersion", pgsql_libVersion },
		{ "ping", pgsql_ping },
		{ "encryptPassword", pgsql_encryptPassword },
		{ NULL, NULL }
	};

	struct luaL_reg conn_methods[] = {
		/* Database Connection Control Functions */
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

		/* Command Execution Functions */
		{ "escape", conn_escape },
		{ "escapeLiteral", conn_escapeLiteral },
		{ "escapeIdentifier", conn_escapeIdentifier },
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

		/* Asynchronous Notifications Funcions */
		{ "notifies", conn_notifies },

		/* Function associated with the COPY command */
		{ "putCopyData", conn_putCopyData },
		{ "putCopyEnd", conn_putCopyEnd },
		{ "getCopyData", conn_getCopyData },

		/* Control Functions */
		{ "clientEncoding", conn_clientEncoding },
		{ "setClientEncoding", conn_setClientEncoding },
		{ "setErrorVerbosity", conn_setErrorVerbosity },

		/* Miscellaneous Functions */
		{ "consumeInput", conn_consumeInput },
		{ "isBusy", conn_isBusy },
		{ "setnonblocking", conn_setnonblocking },
		{ "isnonblocking", conn_isnonblocking },
		{ "flush", conn_flush },

		/* Notice processing */
		{ "setNoticeReceiver", conn_setNoticeReceiver },
		{ "setNoticeProcessor", conn_setNoticeProcessor },
		
		/* Large Objects */
		{ "lo_create", conn_lo_create },
		{ "lo_import", conn_lo_import },
		{ "lo_import_with_oid", conn_lo_import_with_oid },
		{ "lo_export", conn_lo_export },
		{ "lo_open", conn_lo_open },
		{ NULL, NULL }
	};
	struct luaL_reg res_methods[] = {
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
		{ NULL, NULL }
	};
	struct luaL_reg notify_methods[] = {
		{ "relname", notify_relname },
		{ "pid", notify_pid },
		{ "extra", notify_extra },
		{ NULL, NULL }
	};
	struct luaL_reg lo_methods[] = {
		{ "write", pgsql_lo_write },
		{ "read", pgsql_lo_read },
		{ "lseek", pgsql_lo_lseek },
		{ "tell", pgsql_lo_tell },
		{ "truncate", pgsql_lo_truncate },
		{ "close", pgsql_lo_close },
		{ NULL, NULL }
	};

	if (luaL_newmetatable(L, CONN_METATABLE)) {
		luaL_register(L, NULL, conn_methods);

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
		luaL_register(L, NULL, res_methods);

		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, res_clear);
		lua_settable(L, -3);

		lua_pushliteral(L, "__index");
		lua_pushvalue(L, -2);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, NOTIFY_METATABLE)) {
		luaL_register(L, NULL, notify_methods);

		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, notify_clear);
		lua_settable(L, -3);

		lua_pushliteral(L, "__index");
		lua_pushvalue(L, -2);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	if (luaL_newmetatable(L, LO_METATABLE)) {
		luaL_register(L, NULL, lo_methods);

		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, pgsql_lo_clear);
		lua_settable(L, -3);

		lua_pushliteral(L, "__index");
		lua_pushvalue(L, -2);
		lua_settable(L, -3);

		lua_pushliteral(L, "__metatable");
		lua_pushliteral(L, "must not access this metatable");
		lua_settable(L, -3);
	}
	lua_pop(L, 1);

	luaL_register(L, "pgsql", luapgsql);
	pgsql_set_info(L);

	for (n = 0; pgsql_constant[n].name != NULL; n++) {
		lua_pushinteger(L, pgsql_constant[n].value);
		lua_setfield(L, -2, pgsql_constant[n].name);
	};
	return 1;
}
