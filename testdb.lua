require 'pgsql'

conn = pgsql.connectdb('dbname=arcapos user=lua password=42')
print(conn:errorMessage())
if conn:status()  == pgsql.CONNECTION_OK then
	print('connection is ok')
else
	print('connection is not ok')
end

print(conn:errorMessage())
res = conn:exec('select name from syt_pos')
print(res:ntuples())
print(conn:errorMessage())
