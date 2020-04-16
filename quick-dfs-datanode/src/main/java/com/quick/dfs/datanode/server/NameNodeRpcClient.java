package com.quick.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.quick.dfs.constant.ResponseStatus;
import com.quick.dfs.namenode.rpc.model.*;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import com.quick.dfs.constant.ConfigConstant;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @项目名称: quick-dfs
 * @描述: 负责跟一组NameNode中的一个进行通信的组件
 * @作者: fansy
 * @日期: 2020/3/19 10:56
 **/
public class NameNodeRpcClient {

    /**
     * namenode通信组件
     */
    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(ConfigConstant.NAME_NODE_HOST_NAME,ConfigConstant.NAME_NODE_DEFAULT_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }


    /**
     * @方法名: register
     * @描述:   向nameNode发起注册请求
     * @return boolean
     * @作者: fansy
     * @日期: 2020/3/19 11:02 
    */  
    public boolean register(){

        RegisterRequest registerRequest = RegisterRequest.newBuilder()
                .setIp(ConfigConstant.DATA_NODE_IP).setHostname(ConfigConstant.DATA_NODE_HOST_NAME).build();
        RegisterResponse registerResponse = namenode.register(registerRequest);

        if(registerResponse.getStatus() == ResponseStatus.STATUS_SUCCESS){
            return  true;
        }
        return false;
    }

    /**
     * 方法名: heartbeat
     * 描述:   向nameNode发送心跳
     * @param
     * @return com.quick.dfs.namenode.rpc.model.HeartbeatResponse
     * 作者: fansy
     * 日期: 2020/4/4 14:18
     */
    public HeartbeatResponse heartbeat(){
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.newBuilder()
                .setIp(ConfigConstant.DATA_NODE_IP).setHostname(ConfigConstant.DATA_NODE_HOST_NAME).build();
        HeartbeatResponse heartbeatResponse = namenode.heartbeat(heartbeatRequest);
        return heartbeatResponse;
    }

    /**
     * 方法名: informReplicaReceived
     * 描述:   向nameNode上报自己接收到的文件
     * @param fileName  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/4 12:37 
     */  
    public void informReplicaReceived(String fileName,long fileLength){
        InformReplicaReceivedRequest request = InformReplicaReceivedRequest.newBuilder()
                .setFileName(fileName)
                .setHostname(ConfigConstant.DATA_NODE_HOST_NAME)
                .setIp(ConfigConstant.DATA_NODE_IP)
                .setFileLength(fileLength)
                .build();
        namenode.informReplicaReceived(request);
    }

    /**
     * 方法名: reportCompleteStorageInfo
     * 描述:   上报全量文件存储信息
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/4/4 13:46
     */
    public void reportCompleteStorageInfo(StorageInfo storageInfo){
        ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest
                .newBuilder()
                .setIp(ConfigConstant.DATA_NODE_IP)
                .setHostname(ConfigConstant.DATA_NODE_HOST_NAME)
                .setFileNames(JSONArray.toJSONString(storageInfo.getFiles()))
                .setStoredDataSize(storageInfo.getStoredDataSize())
                .build();
        this.namenode.reportCompleteStorageInfo(request);
    }

}
