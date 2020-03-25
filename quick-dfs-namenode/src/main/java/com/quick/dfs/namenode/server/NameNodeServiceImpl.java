package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.namenode.rpc.model.*;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/23 10:44
 **/
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;
    public static final Integer STATUS_SHUTDOWN = 3;

    /**
     * editLog 文件存放路径
     */
    private static final String EDIT_LOG_PATH = "/home/quick-dfs/editlog/";

    /**
     * backup node 每次拉取editlog的数量
     */
    public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

    /**
     * 负责管理元数据的核心组件
     */
    private FSNameSystem nameSystem;

    /**
     * 负责管理datanode的核心组件
     */
    private DataNodeManager dataNodeManager;

    /**
     * 是否正在运行
     */
    private volatile  Boolean isRunning;

    /**
     * 当前backupNode 同步到哪一条txid的 editLog
     */
    private long backupSyncTxid = 0l;

    /**
     * 当前缓冲的一小部分editLog
     */
    private JSONArray currentBufferedEditLog = new JSONArray();

    /**
     * 当前内存里缓冲了哪个磁盘文件的数据
     */
    private String bufferedFlushedTxid;

    /**
     * 当前内存缓冲里保存的最大的txid
     */
    private long currentBufferedMaxTxid;

    public NameNodeServiceImpl(FSNameSystem nameSystem,DataNodeManager dataNodeManager){
        this.nameSystem = nameSystem;
        this.dataNodeManager = dataNodeManager;
        this.isRunning = true;
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
        HeartbeatResponse response = null;
        if(isRunning){
            this.dataNodeManager.heatbeat(request.getIp(),request.getHostname());
            response = HeartbeatResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        }else{
            response = HeartbeatResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @方法名: mkDir
     * @描述:   创建目录
     * @param request
     * @param responseObserver
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 14:46
     */
    @Override
    public void mkDir(MkDirRequest request, StreamObserver<MkDirResponse> responseObserver) {
        this.nameSystem.mkDir(request.getPath());
        MkDirResponse response = MkDirResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**  
     * @方法名: shutdown
     * @描述:   关闭namenode   停止服务
     * @param request
     * @param responseObserver  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 9:05 
    */  
    @Override
    public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
        this.isRunning = false;
        this.nameSystem.flush();
    }

    /**  
     * @方法名: fetchEditLog
     * @描述:   backup node  拉取namenode editLog
     * @param request
     * @param responseObserver  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 11:49 
    */  
    @Override
    public void fetchEditLog(FetchEditLogRequest request, StreamObserver<FetchEditLogResponse> responseObserver) {
        FetchEditLogResponse response = null;
        JSONArray fetchedEditLog = new JSONArray();

        List<String> flushedTxids = this.nameSystem.getEditLog().getFlushedTxid();

        //刚开始拉取日志  磁盘中还没有已经刷入了的editLog
        //此时数据只存在于内存缓冲中
        if(flushedTxids.size() == 0){
            System.out.println("暂时没有任何磁盘文件，直接从namenode 内存缓冲中拉取editLog");
            fetchFromBufferedEditsLog(fetchedEditLog);
        }
        //已经有editLog刷入了磁盘文件
        //此时需要扫描所有的磁盘文件的索引范围
        else{
            //当前backup node 已经拉取过磁盘文件了
            //那么该磁盘文件数据会被缓存在内存中  尝试读取
            if(bufferedFlushedTxid != null){
                if(existInFlushedFile(bufferedFlushedTxid)){
                    System.out.println("上一次已经缓存过磁盘文件的数据，直接从磁盘文件缓存中拉取editslog......");
                    fetchFromCurrentBuffer(fetchedEditLog);
                }
                //判断是否需要读取下一个磁盘文件
                else{
                    String nextFlushedTxid = null;
                    for(int i = 0;i < flushedTxids.size();i++ ){
                        if(bufferedFlushedTxid.equals(flushedTxids.get(i)) && i+1 < flushedTxids.size()){
                            nextFlushedTxid = flushedTxids.get(i+1);
                        }
                    }
                    //下一个磁盘文件不为空  从这个文件里拉取editLog
                    if(nextFlushedTxid != null){
                        System.out.println("上一次缓存过的磁盘文件找不到需要拉取的数据，从下一个磁盘文件拉取...");
                        fetchFromFlushedFile(nextFlushedTxid,fetchedEditLog);
                    }
                    //如果没有找到下一个磁盘文件  那么此时就需要到内存缓冲中拉取
                    else{
                        System.out.println("上一次缓存过的磁盘文件找不到需要拉取的数据，并且没有下一个磁盘文件，直接从namenode 内存缓冲中拉取...");
                        fetchFromBufferedEditsLog(fetchedEditLog);
                    }
                }
            }
            //当前backup node  没有拉取过磁盘文件  第一次尝试读取磁盘文件
            else{
                //遍历所有的磁盘文件
                for(String flushedTxid : flushedTxids){
                    //如果需要拉取的下一条数据在该磁盘文件中  那么就从该磁盘中读取数据
                    if(existInFlushedFile(flushedTxid)){
                        System.out.println("尝试从磁盘文件中拉取数据，flushedTxid:"+flushedTxid);
                        fetchFromFlushedFile(flushedTxid,fetchedEditLog);
                        break;
                    }
                }
                //如果当前拉取的日志已经比所有的磁盘文件都新了  就从内存缓冲中去读取
                if(bufferedFlushedTxid == null){
                    System.out.println("所有磁盘文件里都没有需要拉取的数据，尝试直接从namenode 内存缓冲中拉取数据...");
                    fetchFromBufferedEditsLog(fetchedEditLog);
                }
            }
        }
        response = FetchEditLogResponse.newBuilder()
                .setEditLogs(fetchedEditLog.toJSONString())
                .setStatus(STATUS_SUCCESS)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    /**  
     * @方法名: fetchFromBufferedEditsLog
     * @描述:   从namenode内存缓冲中拉取editLog
     * @param fetchedEditLog  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 16:53 
    */  
    private void fetchFromBufferedEditsLog(JSONArray fetchedEditLog){
        if(backupSyncTxid < currentBufferedMaxTxid){
            System.out.println("尝试从namenode 内存缓冲拉取数据的时候，发现上次内存缓存里面还有数据可以拉取，" +
                    "尝试从内存缓存里拉取数据...");
            fetchFromCurrentBuffer(fetchedEditLog);
            return;
        }
        currentBufferedEditLog.clear();

        int fetchCount = 0;
        String[] bufferedEditLog = this.nameSystem.getEditLog().getBufferedEditLog();

        for(String eidtlogRawData : bufferedEditLog){
            JSONObject editLogJson = JSON.parseObject(eidtlogRawData);
            currentBufferedEditLog.add(editLogJson);
            long txId = editLogJson.getLong("txId");
            currentBufferedMaxTxid = txId;
            if(txId == backupSyncTxid + 1 && fetchCount <= BACKUP_NODE_FETCH_SIZE){
                fetchedEditLog.add(editLogJson);
                backupSyncTxid = txId;
                fetchCount++;
            }
        }

        bufferedFlushedTxid = null;
    }

    /**  
     * @方法名: existInFlushedFile
     * @描述:   判断当前应该读取的数据是否在给定的磁盘文件中
     * @param bufferedFlushedTxid  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 17:03 
    */  
    private boolean existInFlushedFile(String bufferedFlushedTxid){
        String[] flushedTxidArr = bufferedFlushedTxid.split("-");
        long startTxid = Long.parseLong(flushedTxidArr[0]);
        long endTxid = Long.parseLong(flushedTxidArr[1]);
        long fetchBeginTxId = backupSyncTxid + 1;

        //当前需要拉取的日志在该磁盘文件中
        if(startTxid <= fetchBeginTxId && endTxid >= fetchBeginTxId){
            return true;
        }
        return false;
    }

    /**
     * @方法名: fetchFromCurrentBuffer
     * @描述:   从当前的内存缓冲中读取数据
     * @param fetchedEditLog
     * @return void
     * @作者: fansy
     * @日期: 2020/3/24 17:07
    */
    private void fetchFromCurrentBuffer(JSONArray fetchedEditLog){
        int fetchCount = 0;
        for(int i = 0;i < currentBufferedEditLog.size();i++){
            JSONObject editLogJson = currentBufferedEditLog.getJSONObject(i);
            long txId = editLogJson.getLong("txId");
            if(txId == backupSyncTxid + 1){
                fetchedEditLog.add(editLogJson);
                backupSyncTxid = txId;
                fetchCount++;
            }
            if(fetchCount == BACKUP_NODE_FETCH_SIZE){
                break;
            }
        }
    }

    /**  
     * @方法名: fetchFromFlushedFile
     * @描述:   从指定的磁盘文件中拉取editLog数据
     * @param flushedTxid
     * @param fetchedEditLog  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 17:13 
    */  
    private void fetchFromFlushedFile(String flushedTxid,JSONArray fetchedEditLog){
        String currentEditLogFile = EDIT_LOG_PATH + flushedTxid + ".log";
        try {
            currentBufferedEditLog.clear();
            List<String> editLogs = Files.readAllLines(Paths.get(currentEditLogFile));
            int fetchCount = 0;
            for(int i = 0; i < editLogs.size(); i++){
                JSONObject editLogJson = JSON.parseObject(editLogs.get(i));
                currentBufferedEditLog.add(editLogJson);
                long txId = editLogJson.getLong("txId");
                currentBufferedMaxTxid = txId;
                if(txId == backupSyncTxid + 1 && fetchCount <= BACKUP_NODE_FETCH_SIZE){
                    fetchedEditLog.add(editLogJson);
                    backupSyncTxid = txId;
                }
            }
            bufferedFlushedTxid = flushedTxid;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
