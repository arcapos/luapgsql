package = "luapgsql"
version = "scm-3"
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
   POSTGRESQL = {
      header = "postgres_fe.h";
   };
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
         incdirs = { "$(POSTGRESQL_INCDIR)" };
         libdirs = { "$(POSTGRESQL_LIBDIR)" };
         libraries = { "pq" };
      };
   };
   platforms = {
      linux = {
         modules = {
            pgsql = {
               defines = { "_GNU_SOURCE" }; -- for asprintf
            };
         };
      };
   };
}
