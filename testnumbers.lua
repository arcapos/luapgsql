local pgsql = require 'pgsql'

conn = pgsql.connectdb('')
print(conn:errorMessage())
if conn:status()  == pgsql.CONNECTION_OK then
	print('connection is ok')
else
	print('connection is not ok')
end

local n = 3.1415986
print(n)

local res = conn:exec('create table test (a integer, b numeric(8, 2), c float, d boolean)')

if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to create table')
	print(res:errorMessage())
end
 
res = conn:execParams('insert into test (a, b, c) values ($1::integer, $1::numeric, $1)', n, n, n)
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to insert data')
	print(res:errorMessage())
end

res = conn:execParams('insert into test (c) values ($1)', n)
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to insert data')
	print(res:errorMessage())
end

-- Infinity
res = conn:execParams('insert into test (c) values ($1)', 1/0)
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to insert data')
	print(res:errorMessage())
end

-- -Infinity
res = conn:execParams('insert into test (c) values ($1)', -1/0)
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to insert data')
	print(res:errorMessage())
end

-- NaN
res = conn:execParams('insert into test (c) values ($1)', 0/0)
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print('failed to insert data')
	print(res:errorMessage())
end

res = conn:execParams('insert into test (d) values ($1)', true)
res = conn:execParams('insert into test (d) values ($1)', false)

conn:finish()
