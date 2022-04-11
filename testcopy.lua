local pgsql = require 'pgsql'
local proxy = require 'proxy'

local conn = pgsql.connectdb('')
if conn:status()  == pgsql.CONNECTION_OK then
	print('connection is ok')
else
	print('connection is not ok')
	print(conn:errorMessage())
end

local res <close> = conn:exec('select * from pg_roles')

local t = {}

res[1]:copy(t)

print 'populated table'
for k, v in pairs(t) do
	print(k, v)
end
print ''


t = res[1]:copy()

print 'returned table'
for k, v in pairs(t) do
	print(k, v)
end
print ''

local p = proxy.new()

res[1]:copy(p)

print 'value from populated proxy'
print(p.rolname)

res[1]:copy(p)

print 'value from populated proxy'
print(p.rolname)

-- provoke an error
res[1]:copy(x)