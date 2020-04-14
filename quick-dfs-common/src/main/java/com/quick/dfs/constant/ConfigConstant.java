package com.quick.dfs.constant;

/**
 * @项目名称: quick-dfs
 * @描述: 配置常量 通用
 * @作者: fansy
 * @日期: 2020/03/25 21:51
 **/
public interface ConfigConstant {

    /**
     * namenode fsImage 文件存放路径
     */
//    String FS_IMAGE_PATH = "/home/quick-dfs/fsimage/";
    String NAME_NODE_FS_IMAGE_PATH = "G:\\quick-dfs\\namenode\\fsimage\\";

    /**
     * backup node fsImage 文件存放路径
     */
//    String BACKUP_NODE_FS_IMAGE_PATH = "/home/quick-dfs/fsimage/";
    String BACKUP_NODE_FS_IMAGE_PATH = "G:\\quick-dfs\\backupnode\\fsimage\\";

    /**
     * fsimage前缀
     */
    String FS_IMAGE_PREFIX = "fsiamge-";

    /**
     * fsimage后缀
     */
    String FS_IMAGE_SUFFIX = ".meta";

    /**
     * checkpoint操作的时间间隔
     */
    Long CHECKPOINT_INTERVAL = 60 * 60 * 1000L;
//    Long CHECKPOINT_INTERVAL = 60 * 1000L;

    /**
     * 上报fsimage的端口号
     */
    Integer FS_IMAGE_UPLOAD_PORT = 9000;

    /**
     * namenode 主机名
     */
    String NAME_NODE_HOST_NAME = "localhost";

    /**
     * namenode 默认通信端口
     */
    Integer NAME_NODE_DEFAULT_PORT = 50070;

    /**
     * namenode清理editlog 时间间隔
     */
    Long NAME_NODE_EDIT_LOG_CLEAN_INTERVAL = 30 *1000l;

    /**
     * namenode editLog 文件存放路径
     */
//    String NAME_NODE_EDIT_LOG_PATH = "/home/quick-dfs/editlog/";
    String NAME_NODE_EDIT_LOG_PATH = "G:\\quick-dfs\\namenode\\editlog\\";

    /**
     * namenode editLog 文件后缀名
     */
    String NAME_NODE_EDIT_LOG_SUFFIX = ".log";

    /**
     * checkpont 元数据文件
     */
    String CHECKPOINT_META = "checkpoint.meta";

    /**
     * datanode  上报心跳时间间隔
     */
    Long DATA_NODE_HEARTBEAT_INTERVAL = 30 * 1000l;

    /**
     * 数据存储副本数量
     */
    Integer DATA_STORE_REPLICA = 2;

    /**
     * datanode 接收文件上传的端口
     */
    Integer DATA_NODE_UPLOAD_PORT = 9000;

    /**
     * datanode 处理网络连接的线程数量
     */
    Integer DATA_NODE_NIO_PROCESSOR_COUNT = 10;

    /**
     * datanode 处理文件读写的线程数量
     */
    Integer DATA_NODE_IO_THREAD_COUNT = 10;

    /**
     * datanode 数据存放目录
     */
//    String DATA_NODE_DATA_PATH = "/home/quick-dfs/data";
    String DATA_NODE_DATA_PATH = "G:\\quick-dfs\\datanode\\data";

    /**
     * datanode 主机名
     */
    String DATA_NODE_HOST_NAME = "";

    /**
     * datanode ip地址
     */
    String DATA_NODE_IP = "";

    /**
     * datanode  文件复制 线程数量
     */
    Integer DATA_NODE_REPLICATE_THREAD_COUNT = 3;

    /**
     * 重平衡 删除迁移副本的延时时间
     */
    Long REBALANCE_REMOVE_REPLICATE_DELAY = 24 * 60 * 60 * 1000l;
}
