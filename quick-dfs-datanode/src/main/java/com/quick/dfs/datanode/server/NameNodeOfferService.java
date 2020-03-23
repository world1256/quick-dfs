package com.quick.dfs.datanode.server;

import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * @项目名称: quick-dfs
 * @描述: 负责跟一组NameNode通信的组件
 * @作者: fansy
 * @日期: 2020/3/19 10:56
 **/
public class NameNodeOfferService {

    /**
     * NameNode主节点通信组件
     */
    private NameNodeServiceActor activeServiceActor;

    /**
     * NameNode备节点通信组件
     */
    private NameNodeServiceActor standbyServiceActor;

    /**
     * dataNode上保存的serviceActor列表
     */
    private CopyOnWriteArrayList<NameNodeServiceActor> serviceActors;

    public NameNodeOfferService(){
        this.activeServiceActor = new NameNodeServiceActor();
        this.standbyServiceActor = new NameNodeServiceActor();

        this.serviceActors = new CopyOnWriteArrayList<NameNodeServiceActor>();
        this.serviceActors.add(this.activeServiceActor);
        this.serviceActors.add(this.standbyServiceActor);
    }

    /**  
     * @方法名: start
     * @描述:   启动
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 11:15 
    */  
    public void start(){
        register();
        startHearbeat();
    }

    /***  
     * @方法名: register   
     * @描述:   在主备NameNode上分别注册该DataNode
     *       这里需要等主备节点上都注册成功才能进行下一步操作
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 11:15 
    */  
    public void register(){
        try{
            CountDownLatch latch = new CountDownLatch(2);
            this.activeServiceActor.register(latch);
            this.standbyServiceActor.register(latch);
            latch.await();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**  
     * @方法名: startHearbeat
     * @描述:   开始发送心跳
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/20 16:20 
    */  
    public void startHearbeat(){
        this.activeServiceActor.startHearbeat();
        this.standbyServiceActor.startHearbeat();
    }

    /**  
     * @方法名: shutDown
     * @描述:   关闭指定的NameNode通信组件
     * @param serviceActor  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 11:19 
    */  
    public void shutDown(NameNodeServiceActor serviceActor){
        this.serviceActors.remove(serviceActor);
    }

    /**
     * @方法名: iterateServiceActors
     * @描述:   迭代遍历ServiceActor
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 11:20 
    */  
    public void iterateServiceActors() {
        Iterator<NameNodeServiceActor> iterator = serviceActors.iterator();
        while(iterator.hasNext()) {
            iterator.next();
        }
    }

}
