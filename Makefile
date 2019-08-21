LIB=		pgsql
SHLIB_MAJOR=	1
SHLIB_MINOR=	0

SRCS=		luapgsql.c

LUA_VERSION?=	5.2
OS!=		uname

.if ${OS} == "NetBSD"
LOCALBASE?=	/usr/pkg
LUA_INCDIR?=	${LOCALBASE}/include/lua-${LUA_VERSION}
.else
LOCALBASE?=	/usr/local
LUA_INCDIR?=	${LOCALBASE}/include/lua${LUA_VERSION:S/.//}
.endif

PQ_LIBDIR?=	${LOCALBASE}/lib

CFLAGS+=	-I${LOCALBASE}/include -I${LUA_INCDIR}
LDADD+=		-L${PQ_LIBDIR} -lpq
NOLINT=		1

LUA_MODLIBDIR?=	${LOCALBASE}/lib/lua/${LUA_VERSION}

libinstall:

install:
	${INSTALL} -d ${DESTDIR}${LUA_MODLIBDIR}
	${INSTALL} lib${LIB}.so ${DESTDIR}${LUA_MODLIBDIR}/${LIB}.so

.include <bsd.lib.mk>
