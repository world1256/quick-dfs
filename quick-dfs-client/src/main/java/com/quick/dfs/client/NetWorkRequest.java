package com.quick.dfs.client;

import java.nio.ByteBuffer;

/**
 * @项目名称: quick-dfs
 * @描述: 网络连接请求
 * @作者: fansy
 * @日期: 2020/4/16 10:28
 **/
public class NetWorkRequest {

    private String id;

    private String hostname;

    private boolean needResponse;

    private Integer requestType;

    private ByteBuffer buffer;

    private long sendTime;

    private NetworkResponseCallback callback;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public boolean isNeedResponse() {
        return needResponse;
    }

    public void setNeedResponse(boolean needResponse) {
        this.needResponse = needResponse;
    }

    public Integer getRequestType() {
        return requestType;
    }

    public void setRequestType(Integer requestType) {
        this.requestType = requestType;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public NetworkResponseCallback getCallback() {
        return callback;
    }

    public void setCallback(NetworkResponseCallback callback) {
        this.callback = callback;
    }
}
