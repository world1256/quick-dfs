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
        this.rpcServer.start();
    }

    /**
     * @方法名: run
     * @描述:   NameNode 正式运行
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/18 15:28
    */
    private void run(){
        try{
            while (shouldRun){
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NameNode nameNode = new NameNode();
        nameNode.init();
        nameNode.run();
    }
}
