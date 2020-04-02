package com.quick.dfs.datanode.server;

import com.quick.dfs.namenode.rpc.model.HeartbeatRequest;
import com.quick.dfs.namenode.rpc.model.HeartbeatResponse;
import com.quick.dfs.namenode.rpc.model.RegisterRequest;
import com.quick.dfs.namenode.rpc.model.RegisterResponse;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import com.quick.dfs.thread.Daemon;
import com.quick.dfs.constant.ConfigConstant;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.CountDownLatch;

/**
 * @项目名称: quick-dfs
 * @描述: 负责跟一组NameNode中的一个进行通信的组件
 * @作者: fansy
 * @日期: 2020/3/19 10:56
 **/
public class NameNodeServiceActor {

    /**
     * namenode通信组件
     */
    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeServiceActor(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(ConfigConstant.NAME_NODE_HOST_NAME,ConfigConstant.NAME_NODE_DEFAULT_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }


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
            try{
                RegisterRequest registerRequest = RegisterRequest.newBuilder()
                        .setIp(ConfigConstant.DATA_NODE_IP).setHostname(ConfigConstant.DATA_NODE_HOST_NAME).build();
                RegisterResponse registerResponse = namenode.register(registerRequest);
                latch.countDown();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    /**
     * 心跳线程
     */
    class HeartbeatThread extends Daemon {

        @Override
        public void run() {
            while (true){
                //TODO  send heartbeat
                try {
                    HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
                            .setIp(ConfigConstant.DATA_NODE_IP).setHostname(ConfigConstant.DATA_NODE_HOST_NAME).build();
                    HeartbeatResponse heartbeatResponse = namenode.heartbeat(heartbeatRequest);
                    Thread.sleep(ConfigConstant.DATA_NODE_HEARTBEAT_INTERVAL);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
