local pgsql = require 'pgsql'

if pgsql.libVersion() < 100000 then
	print('conn:encryptPassword() requires at least PosgreSQL 10')
	os.exit(1)
end

conn = pgsql.connectdb('')
print(conn:errorMessage())
if conn:status() ~= pgsql.CONNECTION_OK then
	print('database connection failed')
	os.exit(1)
end


local pw = conn:encryptPassword('secret', 'postgres')
local pw_md5 = conn:encryptPassword('secret', 'postgres', 'md5')
local pw_scram_sha_256 = conn:encryptPassword('secret', 'postgres',
    'scram-sha-256')

print(string.format([[
password with default algorithm:
%s

password with md5 algorithm:
%s

password with scram-sha-256 algorithm:
%s
]], pw, pw_md5, pw_scram_sha_256))

conn:finish()
