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

    /**
     * 最后上报心跳时间
     */
    private long lastHeartbeatTime;

    /**
     * dataNode 上存储的文件总大小
     */
    private long storedDataSize;

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

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public void setLastHeartbeatTime(long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    /**
     * @方法名: addStoredDataSize
     * @描述:  累加当前dataNode存储的数据量大小
     * @param fileSize  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/30 9:45 
    */  
    public void addStoredDataSize(long fileSize){
        this.storedDataSize += fileSize;
    }
}
