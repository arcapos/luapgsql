SRCS=		luapgsql.c
LIB=		pgsql

CFLAGS+=	-O3 -Wall -fPIC -I/usr/include -I/usr/include/lua5.1 \
		-I/usr/include/postgresql -D_GNU_SOURCE
LDADD+=		-L/usr/lib -lpq -lbsd

LIBDIR=		/usr/lib/lua/5.1

${LIB}.so:	${SRCS:.c=.o}
		cc -shared -o ${LIB}.so ${CFLAGS} ${SRCS:.c=.o} ${LDADD}

clean:
		rm -f *.o *.so
install:
	install -d ${DESTDIR}${LIBDIR}
	install ${LIB}.so ${DESTDIR}${LIBDIR}
