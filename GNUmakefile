SRCS=		luapgsql.c
LIB=		pgsql

LUAVER=		$(shell lua -v 2>&1 | cut -c 5-7)
PGVER=		$(shell psql -V | cut -c 19-21)

CFLAGS+=	-O3 -Wall -fPIC -I/usr/include -I/usr/include/lua${LUAVER} \
		-I/usr/include/postgresql \
		-I/usr/include/postgresql/${PGVER}/server \
		-D_GNU_SOURCE

LDADD+=		-L/usr/lib -lpq -lbsd

LIBDIR=		/usr/lib
LUADIR=		/usr/lib/lua/${LUAVER}

${LIB}.so:	${SRCS:.c=.o}
		cc -shared -o ${LIB}.so ${CFLAGS} ${SRCS:.c=.o} ${LDADD}

clean:
		rm -f *.o *.so
install:
	install -d ${DESTDIR}${LIBDIR}
	install -m 755 ${LIB}.so ${DESTDIR}${LUADIR}/${LIB}.so
