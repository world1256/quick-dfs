package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: 对外提供rpc请求响应组件
 * @作者: fansy
 * @日期: 2020/3/18 15:15
 **/
public class NameNodeRpcServer {

    /**
     * 负责管理元数据的组件
     */
    private FSNameSystem nameSystem;

    /**
     * 负责管理DataNode的组件
     */
    private DataNodeManager dataNodeManager;

    public NameNodeRpcServer(FSNameSystem nameSystem,DataNodeManager dataNodeManager){
        this.nameSystem = nameSystem;
        this.dataNodeManager = dataNodeManager;
    }

    /**
     * 创建目录
     * @param path
     * @return
     */
    public boolean mkDir(String path){
        return  this.nameSystem.mkDir(path);
    }

    /**
     * DataNode注册
     * @param ip
     * @param hostName
     * @return
     */
    public boolean register(String ip,String hostName){
        return this.dataNodeManager.register(ip,hostName);
    }

    /***  
     * @方法名: start
     * @描述:   启动rpcServer
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/18 15:53 
    */  
    public void start(){

    }
}

