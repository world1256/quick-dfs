package com.quick.dfs.namenode.server;

import com.quick.dfs.namenode.rpc.model.HeartbeatRequest;
import com.quick.dfs.namenode.rpc.model.HeartbeatResponse;
import com.quick.dfs.namenode.rpc.model.RegisterRequest;
import com.quick.dfs.namenode.rpc.model.RegisterResponse;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/23 10:44
 **/
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;

    /**
     * 负责管理元数据的核心组件
     */
    private FSNameSystem nameSystem;

    /**
     * 负责管理datanode的核心组件
     */
    private DataNodeManager dataNodeManager;

    public NameNodeServiceImpl(FSNameSystem nameSystem,DataNodeManager dataNodeManager){
        this.nameSystem = nameSystem;
        this.dataNodeManager = dataNodeManager;
    }

    /**
     * @方法名: mkDir
     * @描述:  创建目录
     * @param path
     * @return boolean
     * @作者: fansy
     * @日期: 2020/3/23 10:50
    */
    public boolean mkDir(String path){
        return this.nameSystem.mkDir(path);
    }

    /**
     * @方法名: register
     * @描述:   响应注册请求
     * @param request
     * @param responseObserver
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 10:51
    */
    @Override
    public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
        this.dataNodeManager.register(request.getIp(),request.getHostname());
        RegisterResponse response = RegisterResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @方法名: heartbeat
     * @描述:   响应心跳请求
     * @param request
     * @param responseObserver
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 10:51
    */
    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        this.dataNodeManager.heatbeat(request.getIp(),request.getHostname());
        HeartbeatResponse response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
