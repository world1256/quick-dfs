package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ConfigConstant;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 11:20
 **/
public class DataNodeNIOServer extends Thread{

    private Selector selector;

    private List<LinkedBlockingDeque<SelectionKey>> queues = new ArrayList<>();

    private Map<String,CachedFile> cachedFiles = new HashMap<>();

    public DataNodeNIOServer(){
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
                    handRequest(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void handRequest(SelectionKey key) throws IOException {
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
                String remoteAddr = channel.getRemoteAddress().toString();
                //对上传的客户端地址取模，让请求尽量均匀的分布在各个线程上
                int queuesIndex = remoteAddr.hashCode() % ConfigConstant.DATA_NODE_UPLOAD_THREAD_COUNT;
                queues.get(queuesIndex).put(key);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }
    }



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
                    if(!channel.isOpen()){
                        channel.close();
                        continue;
                    }

                    String remoteAddr = channel.getRemoteAddress().toString();

                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
                    int length = -1;



                    String fileName = getFileName(channel,buffer);
                    if(fileName == null){
                        continue;
                    }
                    long fileLength = getFileLength(channel,buffer);
                    long readFileLength = getReadFileLength(channel,buffer);

                    FileOutputStream out = new FileOutputStream(fileName);
                    FileChannel fileChannel = out.getChannel();
                    fileChannel.position(fileChannel.size());

                    //首次读取文件
                    if(!cachedFiles.containsKey(remoteAddr)){
                        readFileLength += fileChannel.write(buffer);
                        buffer.clear();
                    }

                    //读取channel中的全部数据
                    while ((length = channel.read(buffer)) > 0){
                        buffer.flip();
                        fileChannel.write(buffer);
                        buffer.clear();
                        readFileLength += length;
                    }

                    if(cachedFiles.containsKey(remoteAddr)){
                        if(readFileLength == cachedFiles.get(remoteAddr).fileLength){
                            channel.close();
                            continue;
                        }
                    }

                    fileChannel.close();
                    out.close();

                    if(fileLength == readFileLength){
                        ByteBuffer responseBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
                        channel.write(responseBuffer);
                        cachedFiles.remove(remoteAddr);
                    }else{
                        CachedFile cachedFile = new CachedFile(fileName,fileLength,readFileLength);
                        cachedFiles.put(remoteAddr,cachedFile);
                        //应该不需要吧
                        key.interestOps(SelectionKey.OP_READ);
                    }

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
     * @方法名: getFileName
     * @描述:   获取文件名  顺便创建文件目录
     * @param channel
     * @param buffer
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/4/2 15:25
    */
    private String getFileName(SocketChannel channel,ByteBuffer buffer) throws Exception {
        String fileName = null;
        String remoteAddr = channel.getRemoteAddress().toString();
        if(cachedFiles.containsKey(remoteAddr)){
            fileName = cachedFiles.get(remoteAddr).fileName;
            return fileName;
        }

        fileName = getFileNameFromChannel(channel,buffer);
        if(fileName == null){
            return fileName;
        }

        //如果文件目录不存在  先创建文件目录
        String dirPath = ConfigConstant.DATA_NODE_DATA_PATH + fileName.substring(0,fileName.lastIndexOf("/")+1);
        File dir = new File(dirPath);
        if(!dir.exists()){
            dir.mkdirs();
        }
        fileName = ConfigConstant.DATA_NODE_DATA_PATH + fileName;
        return fileName;
    }

    /**  
     * @方法名: getFileNameFromChannel
     * @描述:   从channel中读取文件名
     * @param channel
     * @param buffer  
     * @return java.lang.String  
     * @作者: fansy
     * @日期: 2020/4/2 15:13 
    */  
    private String getFileNameFromChannel(SocketChannel channel,ByteBuffer buffer) throws Exception {
        int length = channel.read(buffer);
        if(length > 0){
            byte[] fileNameLengthBytes = new byte[4];
            buffer.get(fileNameLengthBytes,0,4);

            ByteBuffer fileNameLengthbuffer = ByteBuffer.wrap(fileNameLengthBytes);
            int fileNameLength = fileNameLengthbuffer.getInt();

            byte[] fileNameBytes = new byte[fileNameLength];
            buffer.get(fileNameBytes,0,fileNameLength);
            String fileName = new String(fileNameBytes);

            return fileName;
        }
        return null;
    }

    /**  
     * @方法名: getFileLength
     * @描述:   获取文件的长度
     * @param channel
     * @param buffer  
     * @return long  
     * @作者: fansy
     * @日期: 2020/4/2 15:38 
    */  
    private long getFileLength(SocketChannel channel,ByteBuffer buffer) throws Exception {
        long fileLength = 0;
        String remoteAddr = channel.getRemoteAddress().toString();
        if(cachedFiles.containsKey(remoteAddr)){
            fileLength = cachedFiles.get(remoteAddr).fileLength;
        }else {
            byte[] fileLengthBytes = new byte[8];
            buffer.get(fileLengthBytes,0,8);

            ByteBuffer fileLengthBuffer = ByteBuffer.wrap(fileLengthBytes);
            fileLength = fileLengthBuffer.getLong();
        }
        return fileLength;
    }

    /**
     * @方法名: getReadFileLength
     * @描述:   获取已经读取了的文件长度
     * @param channel
     * @param buffer
     * @return long
     * @作者: fansy
     * @日期: 2020/4/2 15:42
    */
    private long getReadFileLength(SocketChannel channel,ByteBuffer buffer) throws Exception {
        long readFileLength = 0;
        String remoteAddr = channel.getRemoteAddress().toString();
        if(cachedFiles.containsKey(remoteAddr)){
            readFileLength = cachedFiles.get(remoteAddr).readFileLength;
        }
        return readFileLength;
    }



    class CachedFile{

        private String fileName;

        private long fileLength;

        private long readFileLength;

        public CachedFile(String fileName,long fileLength,long readFileLength){
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.readFileLength = readFileLength;
        }

    }

}
