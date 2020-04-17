package com.quick.dfs.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.ResponseStatus;
import com.quick.dfs.namenode.rpc.model.*;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import com.quick.dfs.util.StringUtil;
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
    private NIOClient nioClient;

    public FileSystemImpl(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(ConfigConstant.NAME_NODE_HOST_NAME,ConfigConstant.NAME_NODE_DEFAULT_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.nioClient = new NIOClient();
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
    public boolean upload(byte[] file, String fileName,NetworkResponseCallback callback) throws Exception {
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

            if(!nioClient.sendFile(hostName,fileName,file,callback)){
                //如果文件上传失败  重新上传到另外一台数据节点
                hostName = relocateDataNode(fileName,file.length,dataNodesJson);
                if(hostName != null){
                    //再次上传失败  则抛出上传失败异常
                    if(!nioClient.sendFile(hostName,fileName,file,callback)){
                        throw new Exception("file upload failed......");
                    }
                }
            }
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

        if(response.getStatus() == ResponseStatus.STATUS_SUCCESS){
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

    /**
     * 重新分配一个上传文件的dataNode节点
     * @param fileName
     * @param fileSize
     * @param excludeDataNodes 之前分配的节点不再分配
     * @return
     */
    private String relocateDataNode(String fileName,long fileSize,String excludeDataNodes){
        RelocateDataNodeRequest request = RelocateDataNodeRequest.newBuilder()
                .setFileSize(fileSize)
                .setExcludeDataNodes(excludeDataNodes)
                .build();
        RelocateDataNodeResponse response = this.namenode.relocateDataNode(request);
        String dataNodeJson = response.getDataNode();
        if(StringUtil.isNotEmpty(dataNodeJson)){
            JSONObject dataNode = JSONObject.parseObject(dataNodeJson);
            return dataNode.getString("hostName");
        }
        return null;
    }

    /**
     * 方法名: download
     * 描述:   下载文件
     * @param fileName
     * @return byte[]
     * 作者: fansy
     * 日期: 2020/4/6 13:34
     */
    @Override
    public byte[] download(String fileName) throws Exception {
        byte[] fileBytes = null;
        String dataNodeJson = getDataNodeHostNameForFile(fileName,"");
        if(StringUtil.isEmpty(dataNodeJson)){
            return null;
        }
        JSONObject dataNode = JSONObject.parseObject(dataNodeJson);
        //获取文件所在dataNode hostname
        String hostname = dataNode.getString("hostName");
        if(StringUtil.isNotEmpty(hostname)){
            try{
                fileBytes = nioClient.readFile(hostname,fileName,true);
            }catch (Exception e){
                e.printStackTrace();
                dataNodeJson = getDataNodeHostNameForFile(fileName,dataNodeJson);
                if(StringUtil.isEmpty(dataNodeJson)){
                    return null;
                }
                dataNode = JSONObject.parseObject(dataNodeJson);
                hostname = dataNode.getString("hostName");
                try{
                    //这里只重试一次   如果第二次还无法下载成功  抛出异常
                    fileBytes = nioClient.readFile(hostname,fileName,false);
                }catch (Exception e2){
                    throw e2;
                }
            }
        }
        return fileBytes;
    }

    /**  
     * 方法名: getDataNodeHostNameForFile
     * 描述:   获取文件所在dataNode hostname
     * @param fileName
     * @param excludeDataNode 失败的节点
     * @return java.lang.String  
     * 作者: fansy 
     * 日期: 2020/4/6 13:41 
     */  
    private String getDataNodeHostNameForFile(String fileName,String excludeDataNode){
        GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                .setFileName(fileName)
                .setExcludeDataNode(excludeDataNode)
                .build();
        GetDataNodeForFileResponse response = this.namenode.getDataNodeForFile(request);
        if(response.getStatus() == ResponseStatus.STATUS_SUCCESS){
            return response.getDataNodeInfo();
        }
        return null;
    }

    /**
     * 方法名: rebalance
     * 描述:   数据节点重平衡
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/4/12 14:24
     */
    @Override
    public void rebalance() throws Exception {
        RebalanceRequest request = RebalanceRequest.newBuilder().build();
        RebalanceResponse response = this.namenode.rebalance(request);
        System.out.println("重平衡的响应:"+response.getStatus());
    }
}
