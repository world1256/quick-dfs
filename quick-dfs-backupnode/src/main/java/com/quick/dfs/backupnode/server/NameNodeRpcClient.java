package com.quick.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.quick.dfs.namenode.rpc.model.FetchEditLogRequest;
import com.quick.dfs.namenode.rpc.model.FetchEditLogResponse;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @项目名称: quick-dfs
 * @描述: 和namenode通信的组件
 * @作者: fansy
 * @日期: 2020/3/24 9:37
 **/
public class NameNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";

    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME,NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**  
     * @方法名: fetchEditLog
     * @描述:  拉取eidtLog数据
     * @param   
     * @return com.alibaba.fastjson.JSONArray  
     * @作者: fansy
     * @日期: 2020/3/24 9:52 
    */  
    public JSONArray fetchEditLog(){
        FetchEditLogRequest fetchEditLogRequest = FetchEditLogRequest.newBuilder()
                .setCode(1).build();

        FetchEditLogResponse fetchEditLogResponse = this.namenode.fetchEditLog(fetchEditLogRequest);
        String editLogJson = fetchEditLogResponse.getEditLogs();

        return JSONArray.parseArray(editLogJson);
    }

}
