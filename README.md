# lake

# 时间复杂性
1. 不同的Lambda函数计算有不同的容器时间；
2. 不同客户端有不同的客户端；
3. 不同的Oss/Hdfs 有不同的分布式时间；

# 时间复杂性解决方案
1. 利用mysql/redis等全局锁，所得全局seqID，利用seqID来确保顺序，这种时间一致性是最确定性的；
2. 利用客户端时间和客户端的seqID，在Lambda函数计算里验证Unix时间相差不超过一个阈值，比如15秒；缺点是客户端必须提供准确的Unix和不重复的SeqID；
3. 利用文件锁，一致性也是没问题的，但是效率极差；

# snap操作
1、取一个相对比较稳定的时间作为快照时间，样本时间至少比当前时间小1个小时
2、对样本时间之前的所有数据进行计算后快照保存