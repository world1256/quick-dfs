package com.quick.dfs.datanode.server;

/**
 * @项目名称: quick-dfs
 * @描述: DataNode启动入口
 * @作者: fansy
 * @日期: 2020/3/19 10:52
 **/
public class DataNode {

    /**
     * 是否应该运行
     */
    private volatile boolean shouldRun;

    /**
     * 和一组NameNode的通信组件
     */
    private NameNodeRpcClient nameNode;

    /**
     * 磁盘存储管理组件
     */
    private StorageManager storageManager;

    /**
     * 文件复制管理组件
     */
    private ReplicateManager replicateManager;

    /**
     * 心跳管理组件
     */
    private HeartbeatManager heartbeatManager;

    /**
     * @方法名: init
     * @描述: 初始化一些组件
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/19 11:29
    */
    public void init(){
        this.shouldRun = true;
        this.nameNode = new NameNodeRpcClient();
        boolean registerSuccess = this.nameNode.register();

        this.storageManager = new StorageManager();
        //注册成功
        if(registerSuccess){
            //全量上报文件存储信息
            StorageInfo storageInfo = this.storageManager.getStorageInfo();
            this.nameNode.reportCompleteStorageInfo(storageInfo);
        }else{
            System.out.println("不需要全量上报文件存储信息...");
        }

        this.replicateManager = new ReplicateManager();
        //启动心跳
        this.heartbeatManager = new HeartbeatManager(nameNode,storageManager,replicateManager);
        this.heartbeatManager.start();

        //启动文件处理服务组件
        new DataNodeNIOServer(nameNode).start();
    }

    /**
     * @方法名: run
     * @描述:   运行dataNode
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/19 11:29
    */
    public void run(){
        try{
            while(shouldRun){
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @方法名: main
     * @描述:   DataNode启动主入口
     * @param args
     * @return void
     * @作者: fansy
     * @日期: 2020/3/19 11:29
    */
    public static void main(String[] args) {
        DataNode dataNode = new DataNode();
        dataNode.init();
        dataNode.run();
    }

}
