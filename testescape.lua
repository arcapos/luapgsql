local pgsql = require 'pgsql'

conn = pgsql.connectdb('')
print(conn:errorMessage())
if conn:status() ~= pgsql.CONNECTION_OK then
	print('database connection failed')
	os.exit(1)
end

local original = "abc'def"

local escaped = conn:escapeBytea(original)
local unescaped = pgsql.unescapeBytea(escaped)

print(string.format([[
original string: %s
escaped string: %s
escaped string length: %d
unescaped string: %s
]], original, escaped, #escaped, unescaped))

if original == unescaped then
	print('unescaped string matches original string')
else
	print('unescaped string does not match original string')
end

conn:finish()
