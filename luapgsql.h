/*
 * Copyright (c) 2009 - 2017, Micro Systems Marc Balmer, CH-5073 Gipf-Oberfrick
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

/* Lua binding for PostgreSQL */

#ifndef __LUAPGSQL_H__
#define __LUAPGSQL_H__

#define CONN_METATABLE		"pgsql connection"
#define RES_METATABLE		"pgsql result"
#define TUPLE_METATABLE		"pgsql tuple"
#define FIELD_METATABLE		"pgsql tuple field"
#define NOTIFY_METATABLE	"pgsql asynchronous notification"
#define GCMEM_METATABLE		"pgsql garbage collected memory"

/* OIDs from server/pg_type.h */
#define BOOLOID			16
#define INT8OID			20
#define INT2OID			21
#define INT4OID			23
#define TEXTOID			25
#define FLOAT4OID		700
#define FLOAT8OID		701
#define NUMERICOID		1700

typedef struct tuple {
	PGresult	*res;
	int		 row;
} tuple;

typedef struct field {
	tuple		*tuple;
	int		 col;
} field;

typedef struct notice {
	lua_State	*L;
	int		 f;
} notice;

#endif /* __LUAPGSQL_H__ */
