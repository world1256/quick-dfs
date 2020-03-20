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
    private NameNodeOfferService nameNodeOfferService;

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
        this.nameNodeOfferService = new NameNodeOfferService();
        nameNodeOfferService.start();
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
