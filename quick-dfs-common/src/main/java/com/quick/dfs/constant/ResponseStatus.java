package com.quick.dfs.constant;

/**
 * @项目名称: quick-dfs
 * @描述:  状态值
 * @作者: fansy
 * @日期: 2020/03/29 21:21
 **/
public interface ResponseStatus {
    /**
     * 成功
     */
    Integer STATUS_SUCCESS = 1;

    /**
     * 失败
     */
    Integer STATUS_FAILURE = 2;

    /**
     * 已停机
     */
    Integer STATUS_SHUTDOWN = 3;

    /**
     * 文件重复
     */
    Integer STATUS_DUPLICATE = 4;
}
