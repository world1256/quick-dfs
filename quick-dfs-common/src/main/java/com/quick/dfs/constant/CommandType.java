package com.quick.dfs.constant;

/**
 * @项目名称: quick-dfs
 * @描述: nameNode下发命令
 * @作者: fansy
 * @日期: 2020/04/04 14:20
 **/
public interface CommandType {

    /**
     * 分隔符
     */
    String SPLIT = ",";

    /**
     * 重新进行注册
     */
    String REGISTER = "1";

    /**
     * 全量上报文件存储信息
     */
    String REPORT_COMPLETE_STORAGE_INFO = "2";
}
