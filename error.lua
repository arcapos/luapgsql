require 'pgsql'

conn = pgsql.connectdb('dbname=arcapos user=lua password=lua2011!')

print("errorMessage: " .. conn:errorMessage())
conn:exec("this will not work")
print("errorMessage: " .. conn:errorMessage())
conn:exec("select now()")
print("errorMessage: " .. conn:errorMessage())
