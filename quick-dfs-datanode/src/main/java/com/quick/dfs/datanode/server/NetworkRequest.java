package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.util.FileUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/4/14 15:21
 **/
public class NetworkRequest {

    /**
     * 本次网络请求对应的连接
     */
    private SelectionKey key;

    /**
     * 本次网络请求对应的连接
     */
    private SocketChannel channel;

    /**
     * 缓存没处理完的请求信息
     */
    private Map<String, CachedRequest> cachedRequestByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的请求类型
     */
    private Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件名长度
     */
    private Map<String,ByteBuffer> fileNameLengthByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件名
     */
    private Map<String,ByteBuffer> fileNameByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件长度
     */
    private Map<String,ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的文件内容
     */
    private Map<String,ByteBuffer> fileByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没发送完的文件内容
     */
    private Map<String,ByteBuffer> sendFileByClient = new ConcurrentHashMap<>();

    public NetworkRequest(SelectionKey key,SocketChannel channel){
        this.key = key;
        this.channel = channel;
    }


    public void read(){
        try {
            Integer requestType = null;
            String client = channel.getRemoteAddress().toString();
            if(cachedRequestByClient.containsKey(client)){
                requestType = cachedRequestByClient.get(client).requestType;
            }else{
                requestType = getRequestType();
            }

            if(requestType == null){
                return;
            }

            if(requestType == ClientRequestType.SEND_FILE){
                handleSendFileRequest(channel,key);
            }else if(requestType == ClientRequestType.READ_FILE){
                handleReadFileRequest(channel,key);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**  
     * @方法名: hasCompletedRead
     * @描述:   是否完成了一个请求的读取
     * @param   
     * @return java.lang.Boolean  
     * @作者: fansy
     * @日期: 2020/4/14 15:36 
    */  
    public Boolean hasCompletedRead() {
        return false;
    }

    /**
     * 方法名: handleRequest
     * 描述:   处理客户端请求
     * @param key
     * @param channel
     * @return void
     * 作者: fansy
     * 日期: 2020/4/6 15:35
     */
    private void handleRequest(SelectionKey key,SocketChannel channel) throws Exception {
        if(!channel.isOpen()){
            channel.close();
            return;
        }

        String remoteAddr = channel.getRemoteAddress().toString();
        System.out.println("接收到客户端请求："+remoteAddr);

        if(cachedRequestByClient.containsKey(remoteAddr)){
            handleSendFileRequest(channel,key);
        }else{
            Integer requestType = getRequestType();

            if(requestType == null){
                return;
            }

            if(requestType == ClientRequestType.SEND_FILE){
                handleSendFileRequest(channel,key);
            }else if(requestType == ClientRequestType.READ_FILE){
                handleReadFileRequest(channel,key);
            }
        }

    }

    /**
     * 方法名: getRequestType
     * 描述:   获取请求类型
     * @return long
     * 作者: fansy
     * 日期: 2020/4/6 14:54
     */
    private Integer getRequestType() throws IOException {
        Integer requestType = null;

        String client = channel.getRemoteAddress().toString();
        if(cachedRequestByClient.get(client)!=null){
            return cachedRequestByClient.get(client).requestType;
        }

        ByteBuffer requestTypeBuffer = null;
        if(requestTypeByClient.containsKey(client)){
            requestTypeBuffer = requestTypeByClient.get(client);
        }else{
            requestTypeBuffer = ByteBuffer.allocate(4);
        }

        channel.read(requestTypeBuffer);

        //请求类型读取完毕  从未读取完毕缓存中删除  将该请求加入未处理完成请求缓存中
        if(!requestTypeBuffer.hasRemaining()){
            requestTypeBuffer.rewind();
            requestType = requestTypeBuffer.getInt();

            requestTypeByClient.remove(client);
            CachedRequest cachedRequest = getCachedRequest(client);
            cachedRequest.requestType = requestType;
        }else{
            requestTypeByClient.put(client,requestTypeBuffer);
        }
        return requestType;
    }

    /**
     * 方法名: getCachedRequest
     * 描述:   获取缓存的请求
     * @param client
     * @return com.quick.dfs.datanode.server.DataNodeNIOServer.CachedRequest
     * 作者: fansy
     * 日期: 2020/4/6 16:32
     */
    public CachedRequest getCachedRequest(String client){
        CachedRequest cachedRequest = cachedRequestByClient.get(client);
        if(cachedRequest == null){
            cachedRequest = new CachedRequest();
            cachedRequestByClient.put(client,cachedRequest);
        }
        return cachedRequest;
    }

    /**
     * @方法名: getFileName
     * @描述:   获取文件名  顺便创建文件目录
     * @param channel
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/4/2 15:25
     */
    private FileName getFileName(SocketChannel channel) throws Exception {
        FileName fileName = new FileName();
        String client = channel.getRemoteAddress().toString();
        if(getCachedRequest(client).fileName != null){
            return getCachedRequest(client).fileName;
        }

        String relativeFileName = getRelativeFileName(channel);
        if(relativeFileName == null){
            return null;
        }

        String absoluteFileName = FileUtil.getAbsoluteFileName(relativeFileName);

        //将文件名保存到未完成请求缓存中
        fileName.relativeFileName = relativeFileName;
        fileName.absoluteFileName = absoluteFileName;
        getCachedRequest(client).fileName = fileName;

        return fileName;
    }

    /**
     * @方法名: getRelativeFileName
     * @描述:   从channel中读取文件名
     * @param channel
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/4/2 15:13
     */
    private String getRelativeFileName(SocketChannel channel) throws Exception {
        String client = channel.getRemoteAddress().toString();
        Integer fileNameLength = null;
        String fileName = null;
        ByteBuffer fileNameLengthBuffer = null;

        //首先读取文件名长度  如果文件名在未读取完成缓存中  那么不需要读取文件名长度
        if(!fileNameByClient.containsKey(client)){
            if(fileNameLengthByClient.containsKey(client)){
                fileNameLengthBuffer = fileNameLengthByClient.get(client);
            }else{
                fileNameLengthBuffer = ByteBuffer.allocate(4);
            }
            channel.read(fileNameLengthBuffer);
            if(!fileNameLengthBuffer.hasRemaining()){
                fileNameLengthBuffer.rewind();
                fileNameLength = fileNameLengthBuffer.getInt();
                fileNameLengthByClient.remove(client);
            }else{
                fileNameLengthByClient.put(client,fileNameLengthBuffer);
                return null;
            }
        }

        ByteBuffer fileNameBuffer = null;
        if(fileNameByClient.containsKey(client)){
            fileNameBuffer = fileNameByClient.get(client);
        }else{
            fileNameBuffer = ByteBuffer.allocate(fileNameLength);
        }
        channel.read(fileNameBuffer);
        if(!fileNameBuffer.hasRemaining()){
            fileNameBuffer.rewind();
            fileName = new String(fileNameBuffer.array());
            fileNameByClient.remove(client);
        }else{
            fileNameByClient.put(client,fileNameBuffer);
        }
        return fileName;
    }

    /**
     * @方法名: getFileLength
     * @描述:   获取文件的长度
     * @param channel
     * @return long
     * @作者: fansy
     * @日期: 2020/4/2 15:38
     */
    private Long getFileLength(SocketChannel channel) throws Exception {
        Long fileLength = null;
        String client = channel.getRemoteAddress().toString();
        if(cachedRequestByClient.get(client)!=null){
            return cachedRequestByClient.get(client).fileLength;
        }

        ByteBuffer fileLengthBuffer = null;

        if(fileLengthByClient.containsKey(client)){
            fileLengthBuffer = fileLengthByClient.get(client);
        }else{
            fileLengthBuffer = ByteBuffer.allocate(8);
        }

        channel.read(fileLengthBuffer);

        if(!fileLengthBuffer.hasRemaining()){
            fileLengthBuffer.rewind();
            fileLength = fileLengthBuffer.getLong();
            fileLengthByClient.remove(client);
            cachedRequestByClient.get(client).fileLength = fileLength;
        }else{
            fileLengthByClient.put(client,fileLengthBuffer);
        }
        return fileLength;
    }

    /**
     * 方法名: handleSendFileRequest
     * 描述:   处理客户端的上传文件请求
     * @param channel
     * @return void
     * 作者: fansy
     * 日期: 2020/4/6 14:47
     */
    private void handleSendFileRequest(SocketChannel channel,SelectionKey key) throws Exception {
        String client = channel.getRemoteAddress().toString();
        FileName fileName = getFileName(channel);
        if(fileName == null){
            return;
        }
        Long fileLength = getFileLength(channel);
        if(fileLength == null){
            return;
        }

        ByteBuffer fileBuffer = null;
        if(fileByClient.containsKey(client)){
            fileBuffer = fileByClient.get(client);
        }else{
            fileBuffer = ByteBuffer.allocate(fileLength.intValue());
        }

        channel.read(fileBuffer);

        if(!fileBuffer.hasRemaining()){
            FileOutputStream out = null;
            FileChannel fileChannel = null;
            try {
                out = new FileOutputStream(fileName.absoluteFileName);
                fileChannel = out.getChannel();

                fileBuffer.rewind();
                fileChannel.write(fileBuffer);
                fileByClient.remove(client);
                System.out.println("文件上传完毕，写入磁盘...");

                ByteBuffer responseBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
                channel.write(responseBuffer);
                cachedRequestByClient.remove(client);
                System.out.println("文件读取完毕，返回响应给客户端：" + client);

                //向nameNode上报接收到的文件信息
//                nameNode.informReplicaReceived(fileName.relativeFileName,fileLength);
//                System.out.println("上报接收到文件信息给nameNode...");

                //不再响应该channel的读请求
                key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            }finally {
                fileChannel.close();
                out.close();
            }
        }else{
            fileByClient.put(client, fileBuffer);
        }

    }

    /**
     * 方法名: handleReadFileRequest
     * 描述:   处理客户端的下载文件请求
     * @param channel
     * @param key
     * @return void
     * 作者: fansy
     * 日期: 2020/4/6 15:26
     */
    private void handleReadFileRequest(SocketChannel channel,SelectionKey key) throws Exception {
        String client = channel.getRemoteAddress().toString();
        ByteBuffer buffer = null;
        if(sendFileByClient.containsKey(client)){
            buffer = sendFileByClient.get(client);
        }else{
            FileName fileName = getFileName(channel);
            if(fileName == null){
                channel.close();
                return;
            }
            FileInputStream in = new FileInputStream(fileName.absoluteFileName);
            FileChannel fileChannel = in.getChannel();

            buffer = ByteBuffer.allocate( 8 + (int)fileChannel.size());
            buffer.putLong(fileChannel.size());
            fileChannel.read(buffer);
            fileChannel.close();
            in.close();
            buffer.rewind();
        }

        channel.write(buffer);
        if(buffer.hasRemaining()){
            sendFileByClient.put(client,buffer);
            System.out.println("文件发送没有完成，下次继续发送...");
            key.interestOps(key.interestOps() &~ SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }else{
            sendFileByClient.remove(client);
            cachedRequestByClient.remove(client);
            System.out.println("文件发送给client "+client+" 完毕...");
            key.interestOps(key.interestOps() &~ SelectionKey.OP_READ &~ SelectionKey.OP_WRITE);
        }
    }


    /**
     * 文件名
     */
    class FileName{
        /**
         * 相对路径文件名
         */
        private String relativeFileName;

        /**
         * 绝对路径文件名
         */
        private String absoluteFileName;

    }

    /**
     * 处理的请求缓存信息
     */
    class CachedRequest{

        private Integer requestType;

        private FileName fileName;

        private long fileLength;
    }

}
