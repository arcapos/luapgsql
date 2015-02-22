package = "luapgsql"
version = "1.4.2-0"

source = {
	url = "http://github.com/mbalmer/luapgsql/archive/1.4.2.zip";
	md5 = "044cf06a9917099b740445fceeb08fc4";
	dir = "luapgsql-1.4.2";
}

description = {
	summary = "A Lua Binding for PostgreSQL";
	homepage = "http://github.com/mbalmer/luapgsql";
	license = "3-clause BSD";
}

dependencies = {
	"lua >= 5.1, lua < 5.3";
}

external_dependencies = {
	PQ = {
		header = "libpq-fe.h";
		library = "pq";
	};
}

build = {
	type = "builtin";
	modules = {
		pgsql = {
			sources = "luapgsql.c";
			incdirs = { "$(PQ_INCDIR)" };
			libdirs = { "$(PQ_LIBDIR)" };
			libraries = { "pq" };
		};
	};
}
