package com.quick.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.quick.dfs.namenode.rpc.model.FetchEditLogRequest;
import com.quick.dfs.namenode.rpc.model.FetchEditLogResponse;
import com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest;
import com.quick.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import com.quick.dfs.util.ConfigConstant;
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

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    private Boolean isNamenodeRunning = true;

    public NameNodeRpcClient(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(ConfigConstant.NAME_NODE_HOST_NAME,ConfigConstant.NAME_NODE_DEFAULT_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**  
     * @方法名: fetchEditLog
     * @描述:  拉取eidtLog数据
     * @param   syncedTxid  已经拉取过的txid
     * @return com.alibaba.fastjson.JSONArray
     * @作者: fansy
     * @日期: 2020/3/24 9:52 
    */  
    public JSONArray fetchEditLog(long syncedTxid){
        FetchEditLogRequest fetchEditLogRequest = FetchEditLogRequest.newBuilder()
                .setCode(1).setSyncedTxid(syncedTxid).build();

        FetchEditLogResponse fetchEditLogResponse = this.namenode.fetchEditLog(fetchEditLogRequest);
        String editLogJson = fetchEditLogResponse.getEditLogs();

        return JSONArray.parseArray(editLogJson);
    }

    /**  
     * 方法名: updateCheckpointTxid
     * 描述:   上报checkpoint  txid
     * @param txid  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 16:46 
     */  
    public void updateCheckpointTxid(long txid){
        UpdateCheckpointTxidRequest request = UpdateCheckpointTxidRequest.newBuilder()
                .setTxid(txid).build();
        UpdateCheckpointTxidResponse response  = this.namenode.updateCheckpointTxid(request);
        System.out.println("上报checkpoint txid完成，响应状态："+response.getStatus());
    }

    public Boolean isNamenodeRunning() {
        return isNamenodeRunning;
    }

    public void setIsNamenodeRunning(Boolean isNamenodeRunning) {
        this.isNamenodeRunning = isNamenodeRunning;
    }
}
