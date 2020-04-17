package com.quick.dfs.client;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求响应回调函数
 * @作者: fansy
 * @日期: 2020/4/17 15:23
 **/
public interface NetworkResponseCallback {

    /**
     * @方法名: process
     * @描述:   处理响应结果
     * @param response
     * @return void
     * @作者: fansy
     * @日期: 2020/4/17 15:42
    */
    void process(NetworkResponse response);

}
