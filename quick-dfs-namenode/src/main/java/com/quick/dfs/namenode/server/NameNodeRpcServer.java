package com.quick.dfs.namenode.server;

import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

/**
 * @项目名称: quick-dfs
 * @描述: 对外提供rpc请求响应组件
 * @作者: fansy
 * @日期: 2020/3/18 15:15
 **/
public class NameNodeRpcServer {

    /**
     * namenode 默认监听的端口号
     */
    private static int DEFAULT_POTR = 50070;

    /**
     * 负责管理元数据的组件
     */
    private FSNameSystem nameSystem;

    /**
     * 负责管理DataNode的组件
     */
    private DataNodeManager dataNodeManager;

    private Server server;

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

    /**
     * 方法名: heartbeat
     * 描述:   处理DataNode心跳请求
     * @param ip
     * @param hostName
     * @return boolean
     * 作者: fansy
     * 日期: 2020/3/20 20:06
     */
    public boolean heartbeat(String ip,String hostName){
        return this.dataNodeManager.heatbeat(ip,hostName);
    }

    /***  
     * @方法名: start
     * @描述:   启动rpcServer
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/18 15:53 
    */  
    public void start() throws IOException {
        this.server = ServerBuilder.forPort(DEFAULT_POTR)
                .addService(NameNodeServiceGrpc.bindService(new NameNodeServiceImpl(this.nameSystem,this.dataNodeManager)))
                .build()
                .start();

        System.out.println("NameNodeRpcServer 启动成功，监听端口号："+DEFAULT_POTR);

        //添加停机钩子  关闭rpcServer
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                NameNodeRpcServer.this.stop();
            }
        });
    }

    /**  
     * @方法名: stop
     * @描述:   停止rpc  server
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/23 10:40 
    */  
    public void stop (){
        if(this.server!=null){
            this.server.shutdown();
        }
    }

    /**  
     * @方法名: blockUntilShutdown
     * @描述:   停止之前阻塞
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/23 10:40 
    */  
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}

