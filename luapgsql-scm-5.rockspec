package = "luapgsql"
version = "scm-5"
source = {
   url = "git://github.com/mbalmer/luapgsql";
}
description = {
   summary = "A Lua Binding for PostgreSQL";
   homepage = "http://github.com/mbalmer/luapgsql";
   license = "3-clause BSD";
}
dependencies = {
   "lua >= 5.1, < 5.3";
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
