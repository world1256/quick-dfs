package com.quick.dfs.client;

import java.nio.ByteBuffer;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求响应
 * @作者: fansy
 * @日期: 2020/4/16 16:35
 **/
public class NetworkResponse {

    private String hostname;

    private String requestId;

    private ByteBuffer buffer;

    private Integer status;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
