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

/* POstgreSQL extension module (using Lua) */

#include <libpq-fe.h>
#include <pg_config.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

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
	PGconn **data;

	data = (PGconn **)lua_newuserdata(L, sizeof(PGconn *));
	*data = PQconnectdb(luaL_checkstring(L, -2));
	luaL_getmetatable(L, CONN_METATABLE);
	lua_setmetatable(L, -2);
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
conn_finish(lua_State *L)
{
	PGconn **conn;

	conn = luaL_checkudata(L, 1, CONN_METATABLE);
	if (*conn) {
		PQfinish(*conn);
		*conn = NULL;
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

/*
 * Result set functions
 */
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
res_nfields(lua_State *L)
{
	lua_pushinteger(L,
	    PQnfields(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
	return 1;
}

static int
res_status(lua_State *L)
{
	lua_pushinteger(L,
	    PQresultStatus(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
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
res_getvalue(lua_State *L)
{
	lua_pushstring(L,
	    PQgetvalue(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE),
	    luaL_checkint(L, 2) - 1, luaL_checkint(L, 3) - 1));
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
res_ntuples(lua_State *L)
{
	lua_pushinteger(L,
	    PQntuples(*(PGresult **)luaL_checkudata(L, 1, RES_METATABLE)));
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
res_clear(lua_State *L)
{
	PGresult **r;

	r = luaL_checkudata(L, 1, RES_METATABLE);
	if (*r)  {
		PQclear(*r);
		*r = NULL;
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
	{ NULL,				0 }
};

static void
pgsql_set_info(lua_State *L)
{
	lua_pushliteral(L, "_COPYRIGHT");
	lua_pushliteral(L, "Copyright (C) 2011 micro systems marc balmer");
	lua_settable(L, -3);
	lua_pushliteral(L, "_DESCRIPTION");
	lua_pushliteral(L, "PostgreSQL binding for Lua");
	lua_settable(L, -3);
	lua_pushliteral(L, "_VERSION");
	lua_pushliteral(L, "pgsql 1.0.0");
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
		{ "exec", conn_exec },

		{ NULL, NULL }
	};
	struct luaL_reg res_methods[] = {
		{ "errorField", res_errorField },
		{ "errorMessage", res_errorMessage },
		{ "getisnull", res_getisnull },
		{ "getvalue", res_getvalue },
		{ "ntuples", res_ntuples },
		{ "fname", res_fname },
		{ "fnumber", res_fnumber },
		{ "nfields", res_nfields },
		{ "status", res_status },
		{ NULL, NULL }
	};

	if (luaL_newmetatable(L, CONN_METATABLE)) {
		luaL_register(L, NULL, conn_methods);

#if 0
		lua_pushliteral(L, "__gc");
		lua_pushcfunction(L, conn_finish);
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

	luaL_register(L, "pgsql", luapgsql);
	pgsql_set_info(L);

	for (n = 0; pgsql_constant[n].name != NULL; n++) {
		lua_pushinteger(L, pgsql_constant[n].value);
		lua_setfield(L, -2, pgsql_constant[n].name);
	};
	return 1;
}
