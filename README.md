# quick-dfs
quick-dfs灵感来源于Hadoop hdfs,主要的架构机制类似于hdfs。

quick-dfs的目标是构架一个高可用、高性能的分布式文件系统，并且尽量使整体架构简化。

--------------------------------------------------------------------------------------------------------
项目结构:
    backupnode: 元数据备份节点,一直拉取nameNode的editLog并定时进行checkPoint后上报给nameNode,减轻nameNode
    文件目录树管理的压力。提高nameNode启动时恢复文件目录树的速度。
    
    client: 客户端相关操作。可以进行文件的上传、下载、删除。目录的创建、删除、重命名等操作。
    
    common: 各个模块需要用的公用代码，主要包括配置项以及工具类。
    
    datanode: 数据保存节点，上传的文件真实保存的节点。主要涉及接收client端上报的文件、需要向nameNode进行注册、
    上报文件存储相关信息等。
    
    namenode: 元数据管理节点。管理整个文件系统的文件目录树元数据，保存在内存中，提供高效、高并发的服务。主要涉及
    管理datanode上报的文件存储信息、接收editLog维护文件目录树、接收datanode心跳，监控datanode状态、响应client
    端的相关请求。
    
    rpc: 整个系统各模块之间的通信协议，在一些简单的数据交互场景下使用。包括心跳、创建目录、shutdown、拉取editlog
    、上报checkPointTxid、上报文件存储信息等。
--------------------------------------------------------------------------------------------------------

项目还在持续开发中，如果您有好的建议，或者发现有任何bug,欢迎提issue。