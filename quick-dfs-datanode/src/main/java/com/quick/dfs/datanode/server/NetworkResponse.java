package com.quick.dfs.datanode.server;

import java.nio.ByteBuffer;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求响应
 * @作者: fansy
 * @日期: 2020/04/14 19:55
 **/
public class NetworkResponse {
    private ByteBuffer buffer;

    private String client;

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }
}
