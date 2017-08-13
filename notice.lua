-- Testing notice processor and notice receiver functions

local pgsql = require 'pgsql'

local conn1 = pgsql.connectdb('')
if conn1:status() == pgsql.CONNECTION_OK then
	print('connection 1 is ok')
else
	print('connection 1 is not ok')
	print(conn1:errorMessage())
end

local conn2 = pgsql.connectdb('')
if conn2:status() == pgsql.CONNECTION_OK then
	print('connection 2 is ok')
else
	print('connection 2 is not ok')
	print(conn2:errorMessage())
end

local function noticeProcessor1(msg)
	print('This is notice processor 1')
	print(msg)
end

local function noticeProcessor2(msg)
	print('This is notice processor 2')
	print(msg)
end

conn1:setNoticeProcessor(noticeProcessor1)
conn2:setNoticeProcessor(noticeProcessor2)

local res = conn1:exec("do $$ begin raise notice '1st notice on conn1'; end $$")
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print(conn1:errorMessage())
end

res = conn2:exec("do $$ begin raise notice '1st notice on conn2'; end $$")
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print(conn2:errorMessage())
end

local function noticeReceiver1(res)
	print('This is notice receiver 1')
	print(res:errorMessage())
end

local function noticeReceiver2(res)
	print('This is notice receiver 2')
	print(res:errorMessage())
end

conn1:setNoticeReceiver(noticeReceiver1)
conn2:setNoticeReceiver(noticeReceiver2)

res = conn1:exec("do $$ begin raise notice '2nd notice on conn1'; end $$")
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print(conn1:errorMessage())
end

res = conn2:exec("do $$ begin raise notice '2nd notice on conn2'; end $$")
if res:status() ~= pgsql.PGRES_COMMAND_OK then
	print(conn2:errorMessage())
end


print('finish connections')

conn1:finish()
conn2:finish()
