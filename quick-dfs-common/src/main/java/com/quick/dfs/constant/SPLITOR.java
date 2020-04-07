package com.quick.dfs.constant;

/**
 * @项目名称: quick-dfs
 * @描述: 分隔符
 * @作者: fansy
 * @日期: 2020/4/7 14:39
 **/
public interface SPLITOR {

    /**
     * 文件名 长度 分隔符
     */
    String FILE_NAME_LENGTH = "@#";

    /**
     * dataNode 信息中  ip 和  host的分隔符
     */
    String DATA_NODE_IP_HOST = "-";

    /**
     * editlog 中txid  起始  结束  分隔符
     */
    String TX_ID_START_END = "-";

    /**
     * 文件名 分隔符
     */
    String FILE_NAME ="[.]";
}
