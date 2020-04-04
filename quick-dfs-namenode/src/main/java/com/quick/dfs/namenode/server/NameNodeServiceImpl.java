package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.constant.CommandType;
import com.quick.dfs.constant.StatusCode;
import com.quick.dfs.namenode.rpc.model.*;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import com.quick.dfs.constant.ConfigConstant;
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
        RegisterResponse response = null;
        boolean success = this.dataNodeManager.register(request.getIp(),request.getHostname());
        if(success){
            response = RegisterResponse.newBuilder()
                    .setStatus(StatusCode.STATUS_SUCCESS).build();
        }else{
            response = RegisterResponse.newBuilder()
                    .setStatus(StatusCode.STATUS_FAILURE).build();
        }
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
        boolean success = this.dataNodeManager.heatbeat(request.getIp(),request.getHostname());
        if(success){
            response = HeartbeatResponse.newBuilder()
                    .setStatus(StatusCode.STATUS_SUCCESS)
                    .build();
        }else{
            //心跳失败 需要重新注册并且上报全量文件存储信息
            String commands = CommandType.REGISTER + CommandType.SPLIT + CommandType.REPORT_COMPLETE_STORAGE_INFO;
            response = HeartbeatResponse.newBuilder()
                    .setStatus(StatusCode.STATUS_FAILURE)
                    .setCommands(commands)
                    .build();
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
        MkDirResponse response = MkDirResponse.newBuilder()
                .setStatus(StatusCode.STATUS_SUCCESS).build();
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
        this.nameSystem.saveCheckpointTxid();
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

        long syncedTxid = request.getSyncedTxid();
        List<String> flushedTxids = this.nameSystem.getEditLog().getFlushedTxid();

        //刚开始拉取日志  磁盘中还没有已经刷入了的editLog
        //此时数据只存在于内存缓冲中
        if(flushedTxids.size() == 0){
            System.out.println("暂时没有任何磁盘文件，直接从namenode 内存缓冲中拉取editLog");
            fetchFromBufferedEditsLog(syncedTxid,fetchedEditLog);
        }
        //已经有editLog刷入了磁盘文件
        //此时需要扫描所有的磁盘文件的索引范围
        else{
            //当前backup node 已经拉取过磁盘文件了
            //那么该磁盘文件数据会被缓存在内存中  尝试读取
            if(bufferedFlushedTxid != null){
                if(existInFlushedFile(syncedTxid,bufferedFlushedTxid)){
                    System.out.println("上一次已经缓存过磁盘文件的数据，直接从磁盘文件缓存中拉取editslog......");
                    fetchFromCurrentBuffer(syncedTxid,fetchedEditLog);
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
                        System.out.println("上一次缓存过的磁盘文件找不到需要拉取的数据，从下一个磁盘文件拉取,flushedTxid:"+nextFlushedTxid);
                        fetchFromFlushedFile(syncedTxid,nextFlushedTxid,fetchedEditLog);
                    }
                    //如果没有找到下一个磁盘文件  那么此时就需要到内存缓冲中拉取
                    else{
                        System.out.println("上一次缓存过的磁盘文件找不到需要拉取的数据，并且没有下一个磁盘文件，直接从namenode 内存缓冲中拉取...");
                        fetchFromBufferedEditsLog(syncedTxid,fetchedEditLog);
                    }
                }
            }
            //当前backup node  没有拉取过磁盘文件  第一次尝试读取磁盘文件
            else{
                //遍历所有的磁盘文件
                for(String flushedTxid : flushedTxids){
                    //如果需要拉取的下一条数据在该磁盘文件中  那么就从该磁盘中读取数据
                    if(existInFlushedFile(syncedTxid,flushedTxid)){
                        System.out.println("尝试从磁盘文件中拉取数据，flushedTxid:"+flushedTxid);
                        fetchFromFlushedFile(syncedTxid,flushedTxid,fetchedEditLog);
                        break;
                    }
                }
                //如果当前拉取的日志已经比所有的磁盘文件都新了  就从内存缓冲中去读取
                if(bufferedFlushedTxid == null){
                    System.out.println("所有磁盘文件里都没有需要拉取的数据，尝试直接从namenode 内存缓冲中拉取数据...");
                    fetchFromBufferedEditsLog(syncedTxid,fetchedEditLog);
                }
            }
        }
        response = FetchEditLogResponse.newBuilder()
                .setEditLogs(fetchedEditLog.toJSONString())
                .setStatus(StatusCode.STATUS_SUCCESS)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    /**  
     * @方法名: fetchFromBufferedEditsLog
     * @描述:   从namenode内存缓冲中拉取editLog
     * @param syncedTxid
     * @param fetchedEditLog  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 16:53 
    */  
    private void fetchFromBufferedEditsLog(long syncedTxid,JSONArray fetchedEditLog){
        if(syncedTxid < currentBufferedMaxTxid){
            System.out.println("尝试从namenode 内存缓冲拉取数据的时候，发现上次内存缓存里面还有数据可以拉取，" +
                    "尝试从内存缓存里拉取数据...");
            fetchFromCurrentBuffer(syncedTxid,fetchedEditLog);
            return;
        }
        currentBufferedEditLog.clear();

        int fetchCount = 0;
        String[] bufferedEditLog = this.nameSystem.getEditLog().getBufferedEditLog();

        if(bufferedEditLog != null){
            for(String eidtlogRawData : bufferedEditLog){
                JSONObject editLogJson = JSON.parseObject(eidtlogRawData);
                currentBufferedEditLog.add(editLogJson);
                long txId = editLogJson.getLong("txid");
                currentBufferedMaxTxid = txId;
                if(txId == syncedTxid + 1 && fetchCount <= BACKUP_NODE_FETCH_SIZE){
                    fetchedEditLog.add(editLogJson);
                    syncedTxid = txId;
                    fetchCount++;
                }
            }
        }

        bufferedFlushedTxid = null;
    }

    /**  
     * @方法名: existInFlushedFile
     * @描述:   判断当前应该读取的数据是否在给定的磁盘文件中
     * @param syncedTxid
     * @param bufferedFlushedTxid  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 17:03 
    */  
    private boolean existInFlushedFile(long syncedTxid,String bufferedFlushedTxid){
        String[] flushedTxidArr = bufferedFlushedTxid.split("-");
        long startTxid = Long.parseLong(flushedTxidArr[0]);
        long endTxid = Long.parseLong(flushedTxidArr[1]);
        long fetchBeginTxId = syncedTxid + 1;

        //当前需要拉取的日志在该磁盘文件中
        if(startTxid <= fetchBeginTxId && endTxid >= fetchBeginTxId){
            return true;
        }
        return false;
    }

    /**
     * @方法名: fetchFromCurrentBuffer
     * @描述:   从当前的内存缓冲中读取数据
     * @param syncedTxid
     * @param fetchedEditLog
     * @return void
     * @作者: fansy
     * @日期: 2020/3/24 17:07
    */
    private void fetchFromCurrentBuffer(long syncedTxid,JSONArray fetchedEditLog){
        int fetchCount = 0;
        for(int i = 0;i < currentBufferedEditLog.size();i++){
            JSONObject editLogJson = currentBufferedEditLog.getJSONObject(i);
            long txId = editLogJson.getLong("txid");
            if(txId == syncedTxid + 1){
                fetchedEditLog.add(editLogJson);
                syncedTxid = txId;
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
     * @param syncedTxid
     * @param flushedTxid
     * @param fetchedEditLog  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 17:13 
    */  
    private void fetchFromFlushedFile(long syncedTxid,String flushedTxid,JSONArray fetchedEditLog){
        String currentEditLogFile = ConfigConstant.NAME_NODE_EDIT_LOG_PATH + flushedTxid + ".log";
        try {
            currentBufferedEditLog.clear();
            List<String> editLogs = Files.readAllLines(Paths.get(currentEditLogFile));
            int fetchCount = 0;
            for(int i = 0; i < editLogs.size(); i++){
                JSONObject editLogJson = JSON.parseObject(editLogs.get(i));
                currentBufferedEditLog.add(editLogJson);
                long txId = editLogJson.getLong("txid");
                currentBufferedMaxTxid = txId;
                if(txId == syncedTxid + 1 && fetchCount <= BACKUP_NODE_FETCH_SIZE){
                    fetchedEditLog.add(editLogJson);
                    syncedTxid = txId;
                }
            }
            bufferedFlushedTxid = flushedTxid;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**  
     * 方法名: updateCheckpointTxid
     * 描述:  接收backup node checkpoint后最新的txid
     * @param request
     * @param responseObserver  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 16:40 
     */  
    @Override
    public void updateCheckpointTxid(UpdateCheckpointTxidRequest request, StreamObserver<UpdateCheckpointTxidResponse> responseObserver) {
        long txid = request.getTxid();
        this.nameSystem.setCheckpointTxid(txid);

        UpdateCheckpointTxidResponse response = UpdateCheckpointTxidResponse.newBuilder()
                .setStatus(StatusCode.STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**  
     * 方法名: createFile
     * 描述:   创建文件
     * @param request
     * @param responseObserver  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/29 21:32 
     */  
    @Override
    public void createFile(CreateFileRequest request, StreamObserver<CreateFileResponse> responseObserver) {
        CreateFileResponse response = null;
        String fileName = request.getFileName();
        if(isRunning){
            boolean success = this.nameSystem.createFile(fileName);
            if(success){
                response = CreateFileResponse.newBuilder()
                        .setStatus(StatusCode.STATUS_SUCCESS).build();
            }else{
                response = CreateFileResponse.newBuilder()
                        .setStatus(StatusCode.STATUS_DUPLICATE).build();
            }
        }else{
            response = CreateFileResponse.newBuilder()
                    .setStatus(StatusCode.STATUS_SHUTDOWN).build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @方法名: allocateDataNodes
     * @描述:   定位文件需要上传到哪些dataNode上
     * @param request
     * @param responseObserver
     * @return void
     * @作者: fansy
     * @日期: 2020/3/30 9:32
    */
    @Override
    public void allocateDataNodes(AllocateDataNodesRequest request, StreamObserver<AllocateDataNodesResponse> responseObserver) {
        long fileSize = request.getFileSize();
        List<DataNodeInfo> dataNodes = this.dataNodeManager.allocateDataNodes(fileSize);
        String dataNodesJson = JSONArray.toJSONString(dataNodes);

        AllocateDataNodesResponse response = AllocateDataNodesResponse.newBuilder()
                .setDataNodes(dataNodesJson).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 方法名: informReplicaReceived
     * 描述:   dataNode上报接收到的文件信息
     * @param request
     * @param responseObserver
     * @return void
     * 作者: fansy
     * 日期: 2020/4/4 12:15
     */
    @Override
    public void informReplicaReceived(InformReplicaReceivedRequest request, StreamObserver<InformReplicaReceivedResponse> responseObserver) {
        String ip = request.getIp();
        String hostname = request.getHostname();
        String fileName = request.getFileName();
        InformReplicaReceivedResponse response = null;
        try{
            this.nameSystem.addReceivedReplica(ip,hostname,fileName);
            response = InformReplicaReceivedResponse.newBuilder().setStatus(StatusCode.STATUS_SUCCESS).build();
        }catch (Exception e){
            e.printStackTrace();
            response = InformReplicaReceivedResponse.newBuilder().setStatus(StatusCode.STATUS_FAILURE).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**  
     * 方法名: reportCompleteStorageInfo
     * 描述:   dataNode上报全量存储信息
     * @param request
     * @param responseObserver  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/4 13:29 
     */  
    @Override
    public void reportCompleteStorageInfo(ReportCompleteStorageInfoRequest request, StreamObserver<ReportCompleteStorageInfoResponse> responseObserver) {
        String ip = request.getIp();
        String hostname = request.getHostname();
        String fileNamesJson = request.getFileNames();
        long storedDataSize = request.getStoredDataSize();

        this.dataNodeManager.setStoredDataSize(ip,hostname,storedDataSize);

        JSONArray fileNames = JSONArray.parseArray(fileNamesJson);
        for(int i = 0;i < fileNames.size();i++){
            String fileName = fileNames.getString(i);
            this.nameSystem.addReceivedReplica(ip,hostname,fileName);
        }

        ReportCompleteStorageInfoResponse response = ReportCompleteStorageInfoResponse
                .newBuilder().setStatus(StatusCode.STATUS_SUCCESS).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
