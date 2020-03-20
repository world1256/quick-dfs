package com.quick.dfs.datanode.server;

import com.quick.dfs.thread.Demo;

import java.util.concurrent.CountDownLatch;

/**
 * @项目名称: quick-dfs
 * @描述: 负责跟一组NameNode中的一个进行通信的组件
 * @作者: fansy
 * @日期: 2020/3/19 10:56
 **/
public class NameNodeServiceActor {

    /**
     * 发送心跳间隔
     */
    private static  long HEARTBEAT_INTERVAL = 30 *1000L;

    /**
     * @方法名: register
     * @描述:   向nameNode发起注册请求
     * @param latch  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 11:02 
    */  
    public void register(CountDownLatch latch){
        new RegisterThread(latch).start();
    }

    /***  
     * @方法名: startHearbeat
     * @描述:   开始发送心跳
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/20 16:18 
    */  
    public void startHearbeat(){
        new HeartbeatThread().start();
    }

    /**
     * 注册线程
     */
    class RegisterThread extends Thread{

        CountDownLatch latch;

        public RegisterThread(CountDownLatch latch){
            this.latch = latch;
        }

        //这里进行注册操作
        @Override
        public void run() {

            latch.countDown();

        }
    }

    /**
     * 心跳线程
     */
    class HeartbeatThread extends Demo {

        @Override
        public void run() {
            while (true){
                //TODO  send heartbeat
                try {
                    wait(HEARTBEAT_INTERVAL);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
