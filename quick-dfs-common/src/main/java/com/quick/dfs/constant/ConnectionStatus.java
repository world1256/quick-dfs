package com.quick.dfs.constant;

/**
 * @项目名称: quick-dfs
 * @描述: 网络连接状态
 * @作者: fansy
 * @日期: 2020/4/16 9:36
 **/
public interface ConnectionStatus {

    /**
     * 正在连接
     */
    Integer CONNECTING = 1;

    /**
     * 完成连接
     */
    Integer CONNECTED = 2;

    /**
     * 连接中断
     */
    Integer DIS_CONNECTED = 3;
}
