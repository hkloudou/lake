local catlog = KEYS[1]
local filePath = ARGV[1]
local uuidString = ARGV[2]
local prefix = ARGV[3]
local keyTask = ARGV[4]

-- 设置哈希表
redis.call("HSET", prefix .. catlog, filePath, "")
redis.call("HSET", prefix .. catlog, "meta-last-uuid", '"' .. uuidString .. '"')

-- 添加到集合
return redis.call("SADD", keyTask, catlog .. "," .. uuidString)