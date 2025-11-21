local timeResult = redis.call("TIME")
local timestamp = timeResult[1]

local seqKey = "seqid:" .. timestamp

local setResult = redis.call("SETNX", seqKey, "0")
if setResult == 1 then
    redis.call("EXPIRE", seqKey, 5)
end
local seqid = redis.call("INCR", seqKey)

return {timestamp, seqid}