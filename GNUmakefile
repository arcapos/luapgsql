LIB=		pgsql

SRCS=		luapgsql.c

LUA_VERSION?=	$(shell lua -v 2>&1 | cut -c 5-7)

CFLAGS+=	-O3 -Wall -fPIC -I/usr/include -I/usr/include/lua${LUA_VERSION} \
		-I/usr/include/postgresql

LDADD+=		-L/usr/lib -lpq

LIBDIR=		/usr/lib
LUADIR=		/usr/lib/lua/${LUA_VERSION}

${LIB}.so:	${SRCS:.c=.o}
		cc -shared -o ${LIB}.so ${CFLAGS} ${SRCS:.c=.o} ${LDADD}

clean:
		rm -f *.o *.so
install:
	install -d ${DESTDIR}${LIBDIR}
	install -m 755 ${LIB}.so ${DESTDIR}${LUADIR}/${LIB}.so
