package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: NameNode启动入口
 * @作者: fansy
 * @日期: 2020/3/18 15:09
 **/
public class NameNode {

    /**
     * NameNode 是否应该运行
     */
    private volatile  boolean shouldRun;

    /**
     * 元数据管理组件
     */
    private FSNameSystem fsNameSystem;

    /**
     * DataNode管理组件
     */
    private DataNodeManager dataNodeManager;

    /**
     * 对外提供rpc请求响应组件
     */
    private NameNodeRpcServer rpcServer;

    public NameNode(){
        this.shouldRun = true;
    }

    /***  
     * @方法名: init
     * @描述:  NameNode初始化  核心组件初始化
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/18 15:21 
    */  
    private void init(){
        this.fsNameSystem = new FSNameSystem();
        this.dataNodeManager = new DataNodeManager();
        this.rpcServer = new NameNodeRpcServer(fsNameSystem,dataNodeManager);
    }

    /**
     * @方法名: start
     * @描述:   启动NameNode
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 10:58
    */
    private void start() throws Exception{
        this.rpcServer.start();
        this.rpcServer.blockUntilShutdown();
    }

    public static void main(String[] args) throws Exception{
        NameNode nameNode = new NameNode();
        nameNode.init();
        nameNode.start();
    }
}
