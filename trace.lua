local pgsql = require 'pgsql'

conn = pgsql.connectdb('')
print(conn:errorMessage())
if conn:status()  == pgsql.CONNECTION_OK then
	print('connection is ok')
else
	print('connection is not ok')
end

local f = io.open('trace', 'w')
conn:trace(f)
res = conn:execParams('insert into test (d) values ($1)', true)

local f2 = io.open('trace2', 'w')
conn:trace(f2)

print('closing first trace file')
f:close()
print('done')

print('close second trace file')
f2:close()
print('done')

res = conn:execParams('insert into test (d) values ($1)', true)

print('finish connection')

conn:finish()
