package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: 元数据管理组件
 * @作者: fansy
 * @日期: 2020/3/18 15:12
 **/
public class FSNameSystem {

    /**
     * 内存文件目录管理组件
     */
    private FSDirectory directory;

    /**
     * 负责edit log 写入磁盘的组件
     */
    private FSEditLog editLog;

    /**
     * 最近一次checkpoint更新到的txid
     */
    private long checkpointTxid;

    public FSNameSystem(){
        this.directory = new FSDirectory();
        this.editLog = new FSEditLog();
    }

    /**
     * @方法名: mkDir
     * @描述: 创建目录
     * @param path  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public boolean mkDir(String path){
        this.directory.mkDir(path);
        this.editLog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");
        return true;
    }

    /**  
     * @方法名: flush   
     * @描述:  将内存中缓存的edit log强制刷入磁盘
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public void flush(){
        this.editLog.flush();
    }

    public FSEditLog getEditLog() {
        return editLog;
    }

    public long getCheckpointTxid() {
        return checkpointTxid;
    }

    public void setCheckpointTxid(long checkpointTxid) {
        System.out.println("接收到的checkpoint txid:" + checkpointTxid);
        this.checkpointTxid = checkpointTxid;
    }
}
