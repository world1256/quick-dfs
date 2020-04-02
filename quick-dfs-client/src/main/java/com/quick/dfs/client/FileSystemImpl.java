package com.quick.dfs.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.StatusCode;
import com.quick.dfs.namenode.rpc.model.*;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @项目名称: quick-dfs
 * @描述: 客户端文件系统操作实现类
 * @作者: fansy
 * @日期: 2020/3/23 14:23
 **/
public class FileSystemImpl implements FileSystem{

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(ConfigConstant.NAME_NODE_HOST_NAME,ConfigConstant.NAME_NODE_DEFAULT_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * @方法名: mkDir
     * @描述:   创建目录
     * @param path
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 14:42
    */
    @Override
    public void mkDir(String path) throws Exception {
        MkDirRequest mkDirRequest = MkDirRequest.newBuilder()
                .setPath(path).build();

        MkDirResponse mkDirResponse = this.namenode.mkDir(mkDirRequest);

        System.out.println("创建目录的响应：" +mkDirResponse.getStatus());
    }

    /**  
     * @方法名: shutdown
     * @描述:   关闭namenode  停止服务
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 8:51 
    */  
    @Override
    public void shutdown() throws Exception {
        ShutdownRequest shutdownRequest = ShutdownRequest.newBuilder()
                .setCode(1).build();

       this.namenode.shutdown(shutdownRequest);
    }

    /**  
     * 方法名: upload
     * 描述:   上传文件
     * @param file
     * @param fileName  
     * @return boolean
     * 作者: fansy 
     * 日期: 2020/3/29 19:31 
     */  
    @Override
    public boolean upload(byte[] file, String fileName) throws Exception {
        //先在文件目录树中创建该文件
        //如果文件已存在  则返回false  不能上传
        if(!createFile(fileName)){
            return false;
        }


        String dataNodesJson = allocateDataNodes(fileName,file.length);

        JSONArray dataNodes = JSONArray.parseArray(dataNodesJson);
        for(int i = 0;i < dataNodes.size();i++){
            JSONObject dataNode = dataNodes.getJSONObject(i);
            String hostName = dataNode.getString("hostName");
            NIOClient.sendFile(hostName,fileName,file,file.length);
        }

        return true;
    }

    /**  
     * 方法名: createFile
     * 描述:   在目录树中创建文件
     * @param fileName  
     * @return boolean  
     * 作者: fansy 
     * 日期: 2020/3/29 21:24 
     */  
    private boolean createFile(String fileName){
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFileName(fileName).build();

        CreateFileResponse response = this.namenode.createFile(request);

        if(response.getStatus() == StatusCode.STATUS_SUCCESS){
            return true;
        }
        return false;
    }

    /**
     * @方法名: allocateDataNodes
     * @描述:  获取文件上报到的dataNode节点
     * @param fileName
     * @param fileSize
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/3/30 11:09
    */
    private String allocateDataNodes(String fileName,long fileSize){
        AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder()
                .setFileName(fileName).setFileSize(fileSize).build();
        AllocateDataNodesResponse response = this.namenode.allocateDataNodes(request);
        return response.getDataNodes();
    }

}
