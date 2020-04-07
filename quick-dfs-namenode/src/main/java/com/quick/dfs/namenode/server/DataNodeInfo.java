package com.quick.dfs.namenode.server;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    private ConcurrentLinkedQueue<ReplicateTask> replicateTaskQueue = new ConcurrentLinkedQueue<>();

    public DataNodeInfo(String ip,String hostName){
        this.ip = ip;
        this.hostName = hostName;
        this.lastHeartbeatTime = System.currentTimeMillis();
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

    /**
     * @方法名: addReplicateTask
     * @描述:   将复制任务加入队列
     * @param task
     * @return void
     * @作者: fansy
     * @日期: 2020/4/7 16:00
    */
    public void addReplicateTask(ReplicateTask task){
        this.replicateTaskQueue.offer(task);
    }

    /**  
     * @方法名: getReplicateTask
     * @描述:   获取复制任务队列中的一个复制任务
     * @param   
     * @return com.quick.dfs.namenode.server.ReplicateTask  
     * @作者: fansy
     * @日期: 2020/4/7 16:14 
    */  
    public ReplicateTask getReplicateTask(){
        if(!replicateTaskQueue.isEmpty()){
            return replicateTaskQueue.poll();
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataNodeInfo that = (DataNodeInfo) o;
        return ip.equals(that.ip) &&
                hostName.equals(that.hostName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, hostName);
    }
}
