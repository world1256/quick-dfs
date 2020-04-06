package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.util.FileUtil;
import com.sun.beans.editors.ByteEditor;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 11:20
 **/
public class DataNodeNIOServer extends Thread{

    private static Integer BUFFER_SIZE = 10 * 1024;

    private Selector selector;

    private List<LinkedBlockingDeque<SelectionKey>> queues = new ArrayList<>();

    /**
     * 缓存没处理完的请求信息
     */
    private Map<String,CachedRequest> cachedRequestByClient = new ConcurrentHashMap<>();

    /**
     * 缓存没读取完的请求类型
     */
    private Map<String,ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();

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

    private NameNodeRpcClient nameNode;

    public DataNodeNIOServer(NameNodeRpcClient nameNode){
        this.nameNode = nameNode;
        ServerSocketChannel channel = null;
        try {
            selector = Selector.open();
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(ConfigConstant.DATA_NODE_UPLOAD_PORT),100);
            channel.register(selector, SelectionKey.OP_ACCEPT);

            //启动指定数量的worker  来具体处理文件的上传
            for(int i = 0;i < ConfigConstant.DATA_NODE_UPLOAD_THREAD_COUNT;i++){
                LinkedBlockingDeque<SelectionKey> queue = new LinkedBlockingDeque<>();
                queues.add(queue);
                new Worker(queue).start();
            }
            System.out.println("DataNodeNIOServer启动成功，开始监听端口："+ConfigConstant.DATA_NODE_UPLOAD_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true){
            try {
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    handEvent(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**  
     * 方法名: handEvent
     * 描述:   请求分发到各个线程上
     * @param key  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/6 15:11 
     */  
    private void handEvent(SelectionKey key) throws IOException {
        SocketChannel channel = null;
        try {
            if (key.isAcceptable()) {
                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                channel = serverSocketChannel.accept();
                if(channel != null){
                    channel.configureBlocking(false);
                    channel.register(selector,SelectionKey.OP_READ);
                }
            }else if(key.isReadable()){
                //上报文件放到队列里交给 线程去处理
                channel = (SocketChannel) key.channel();
                String client = channel.getRemoteAddress().toString();
                //对上传的客户端地址取模，让请求尽量均匀的分布在各个线程上
                int queuesIndex = client.hashCode() % ConfigConstant.DATA_NODE_UPLOAD_THREAD_COUNT;
                queues.get(queuesIndex).put(key);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }
    }


    /**
     * 处理请求的工作线程
     */
    class Worker extends Thread{

        private LinkedBlockingDeque<SelectionKey> queue;

        public Worker(LinkedBlockingDeque<SelectionKey> queue){
            this.queue = queue;
        }

        @Override
        public void run() {
            while(true){
                SocketChannel channel = null;
                try {
                    SelectionKey key = queue.take();
                    channel = (SocketChannel) key.channel();
                    handleRequest(key,channel);
                } catch (Exception e) {
                    e.printStackTrace();
                    if(channel != null){
                        try {
                            channel.close();
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        }
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
            Integer requestType = getRequestType(channel);

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
     * @param channel
     * @return long
     * 作者: fansy
     * 日期: 2020/4/6 14:54
     */
    private Integer getRequestType(SocketChannel channel) throws IOException {
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

        //如果文件目录不存在  先创建文件目录
        String dirPath = ConfigConstant.DATA_NODE_DATA_PATH + relativeFileName.substring(0,relativeFileName.lastIndexOf("/")+1);
        File dir = new File(dirPath);
        if(!dir.exists()){
            dir.mkdirs();
        }
        String absoluteFileName = ConfigConstant.DATA_NODE_DATA_PATH + relativeFileName;

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
     * @方法名: getReadFileLength
     * @描述:   获取已经读取了的文件长度
     * @param channel
     * @return long
     * @作者: fansy
     * @日期: 2020/4/2 15:42
    */
    private long getReadFileLength(SocketChannel channel) throws Exception {
        long readFileLength = 0;
        String client = channel.getRemoteAddress().toString();
        if(cachedRequestByClient.containsKey(client)){
            readFileLength = cachedRequestByClient.get(client).readFileLength;
        }
        return readFileLength;
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
        long readFileLength = getReadFileLength(channel);

        FileOutputStream out = null;
        FileChannel fileChannel = null;
        try{
            out = new FileOutputStream(fileName.absoluteFileName);
            fileChannel = out.getChannel();
            fileChannel.position(fileChannel.size());

            ByteBuffer fileBuffer = null;
            if(fileByClient.containsKey(client)){
                fileBuffer = fileByClient.get(client);
            }else{
                fileBuffer = ByteBuffer.allocate(fileLength.intValue());
            }

            readFileLength += channel.read(fileBuffer);

            if(!fileBuffer.hasRemaining()){
                fileBuffer.rewind();
                fileChannel.write(fileBuffer);
                fileByClient.remove(client);
            }else{
                cachedRequestByClient.get(client).readFileLength = readFileLength;
                fileByClient.put(client, fileBuffer);
                return;
            }
        }finally {
            fileChannel.close();
            out.close();
        }

        if(fileLength == readFileLength){
            ByteBuffer responseBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
            channel.write(responseBuffer);
            cachedRequestByClient.remove(client);
            System.out.println("文件读取完毕，返回响应给客户端："+client);

            //向nameNode上报接收到的文件信息
            nameNode.informReplicaReceived(fileName.relativeFileName);
            System.out.println("上报接收到文件信息给nameNode...");

            //不再响应该channel的读请求
            key.interestOps(key.interestOps() &~ SelectionKey.OP_READ);
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
        FileName fileName = getFileName(channel);
        if(fileName == null){
            channel.close();
            return;
        }

        String client = channel.getRemoteAddress().toString();

        FileInputStream in = new FileInputStream(fileName.absoluteFileName);
        FileChannel fileChannel = in.getChannel();

        ByteBuffer buffer = ByteBuffer.allocate( 8 + (int)fileChannel.size());
        buffer.putLong(fileChannel.size());
        fileChannel.read(buffer);

        buffer.rewind();
        channel.write(buffer);

        fileChannel.close();
        in.close();

        System.out.println("文件发送给client "+client+" 完毕...");
        key.interestOps(key.interestOps() &~ SelectionKey.OP_READ);
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

        private long readFileLength;
    }


}
