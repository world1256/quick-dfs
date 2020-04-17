package com.quick.dfs.client;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.ResponseStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.UUID;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 10:40
 **/
public class NIOClient {

    private NetworkManager networkManager;

    public NIOClient(){
        this.networkManager = new NetworkManager();
    }

    /**
     * @方法名: sendFile
     * @描述:   上传文件
     * @param hostName
     * @param fileName
     * @param file
     * @param callback
     * @return boolean
     * @作者: fansy
     * @日期: 2020/4/17 16:35
    */
    public boolean sendFile(String hostName,String fileName,byte[] file,NetworkResponseCallback callback){
        if(!networkManager.maybeConnect(hostName)){
            return false;
        }

        NetWorkRequest request  = createSendFileRequest(hostName,fileName,file,callback);
        networkManager.sendRequest(request);
        return true;
    }

    /**
     * @方法名: createSendFileRequest
     * @描述:   创建上传文件请求
     * @param hostName
     * @param fileName
     * @param file
     * @param callback
     * @return com.quick.dfs.client.NetWorkRequest
     * @作者: fansy
     * @日期: 2020/4/17 16:35
    */
    private NetWorkRequest createSendFileRequest(String hostName,String fileName,byte[] file,NetworkResponseCallback callback){

        NetWorkRequest request = new NetWorkRequest();
        request.setId(UUID.randomUUID().toString());
        request.setHostname(hostName);
        request.setCallback(callback);
        request.setRequestType(ClientRequestType.SEND_FILE);
        request.setNeedResponse(false);

        byte[] fileNameBytes = fileName.getBytes();

        //依次存放请求类型+文件名长度+文件名+文件长度+文件内容
        int totalLength = 4 + 4 + fileNameBytes.length + 8 + file.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(ClientRequestType.SEND_FILE);
        buffer.putInt(fileNameBytes.length);
        buffer.put(fileNameBytes);
        buffer.putLong(file.length);
        buffer.put(file);
        buffer.rewind();
        request.setBuffer(buffer);

        return request;
    }

    /**
     * @方法名: readFile
     * @描述:   下载文件
     * @param hostName
     * @param fileName
     * @param retry 是否重试
     * @return byte[]
     * @作者: fansy
     * @日期: 2020/4/17 16:56
    */
    public  byte[] readFile(String hostName,String fileName,boolean retry) throws Exception{
        if(!networkManager.maybeConnect(hostName)){
            if(retry){
                throw new Exception();
            }
        }

        NetWorkRequest request = createReadFileRequest(hostName,fileName);
        networkManager.sendRequest(request);

        NetworkResponse response = networkManager.waitResponse(request.getId());
        if(response.getStatus() == ResponseStatus.STATUS_FAILURE){
            if(retry){
                throw new Exception();
            }
        }

        return response.getBuffer().array();
    }

    /**
     * @方法名: createReadFileRequest
     * @描述:   创建下载文件请求
     * @param hostName
     * @param fileName
     * @return com.quick.dfs.client.NetWorkRequest
     * @作者: fansy
     * @日期: 2020/4/17 16:56
    */
    public NetWorkRequest createReadFileRequest(String hostName,String fileName){
        NetWorkRequest request = new NetWorkRequest();
        request.setId(UUID.randomUUID().toString());
        request.setHostname(hostName);
        request.setRequestType(ClientRequestType.SEND_FILE);
        request.setNeedResponse(true);

        //依次存放请求类型+文件名长度+文件名
        int totalLength = 4 + 4 + fileName.getBytes().length;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.putInt(ClientRequestType.READ_FILE);
        buffer.putInt(fileName.getBytes().length);
        buffer.put(fileName.getBytes());
        buffer.rewind();
        request.setBuffer(buffer);

        return request;
    }

}
