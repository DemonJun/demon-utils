# demon-utils

常用工具集， 通过 shell 命令交互方式, 支持 tab 补全, 可通过 help 查看支持的命令

# kafka

- getOffsetByTime 根据时间戳获取 topic - partition 对应的 offset
    - "-b", "--bootstrap-servers": 指定 bootstrap-servers
    - "-t", "--topic": 指定 topic
    - "-p", "--partitions": 指定 partitions, 多个已','分割, 默认为空, 查询所有的 partitions
    - "-time": 指定时间戳