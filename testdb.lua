local pgsql = require 'pgsql'

local conn = pgsql.connectdb('')
if conn:status()  == pgsql.CONNECTION_OK then
	print('connection is ok')
else
	print('connection is not ok')
	print(conn:errorMessage())
end

local res = conn:exec([[
select rolname, rolsuper from pg_roles order by rolname limit 2
]])

print(#res, 'roles')

print(res:ntuples(), res['ntuples'](res), res[1].rolname, res[1][1])

local res2 = conn:exec("select 'abc' as name, 'def' as surname")

print(res2[1][1], res2[1][2], res2[1][3])

print(res2[3])

print(res2[1].getisnull, res2[1][1], res2[1]:getisnull('name'))

for tuple, row in res:tuples() do
	print(row, #tuple, tuple[1], tuple.rolname)
end

for k, v in pairs(res:copy()) do
	for k2, v2 in pairs(v) do
		print(k, k2, v2)
	end
end

for rolname, rolsuper in res:fields() do
	print(rolname, rolsuper)
end

print(conn:errorMessage())
