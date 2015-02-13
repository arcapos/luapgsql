SRCS=		luapgsql.c
LIB=		pgsql

OS!=		uname

.if ${OS} == "NetBSD"
LOCALBASE=	/usr/pkg
LDADD+=		-R/usr/lib -R${XDIR}/lib -R${LOCALBASE}/lib
.else
LOCALBASE=	/usr/local
.endif

NOLINT=	1
CFLAGS+=	-I${LOCALBASE}/include
LDADD+=		-L${LOCALBASE}/lib -lpq

LIBDIR=		${LOCALBASE}/lib/lua/5.2

libinstall:

install:
	${INSTALL} -d ${DESTDIR}${LIBDIR}
	${INSTALL} lib${LIB}.so ${DESTDIR}${LIBDIR}/${LIB}.so

.include <bsd.lib.mk>
