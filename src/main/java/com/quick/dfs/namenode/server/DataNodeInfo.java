package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: DataNode相关信息
 * @作者: fansy
 * @日期: 2020/3/18 15:38
 **/
public class DataNodeInfo {

    private String ip;

    private String hostName;

    public DataNodeInfo(String ip,String hostName){
        this.ip = ip;
        this.hostName = hostName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}
