package com.quick.dfs.namenode.server;

import com.quick.dfs.util.ConfigConstant;
import com.quick.dfs.util.FileUtil;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * @项目名称: quick-dfs
 * @描述: 响应fsImage上传  服务端
 * @作者: fansy
 * @日期: 2020/3/25 17:20
 **/
public class FSImageUploadServer extends Thread{

    private Selector selector;

    public FSImageUploadServer(){
        init();
    }

    private void init(){
        ServerSocketChannel serverSocketChannel = null;
        try{
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(ConfigConstant.FS_IMAGE_UPLOAD_PORT),100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("FSImageUploadServer启动，监听"+ConfigConstant.FS_IMAGE_UPLOAD_PORT+"端口...");
        while (true){
            try {
                selector.select();
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()){
                    SelectionKey key = keyIterator.next();
                    handleRequest(key);
                    keyIterator.remove();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRequest(SelectionKey key) throws IOException {
        if(key.isAcceptable()){
            handleConnectRequest(key);
        }else if(key.isReadable()){
            handleReadableRequest(key);
        }else if(key.isWritable()){
            handleWritableRequest(key);
        }
    }

    /**
     * 方法名: handleConnectRequest
     * 描述:  处理backup node 连接请求
     * @param key
     * @return void
     * 作者: fansy
     * 日期: 2020/3/25 21:56
     */
    private void handleConnectRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;
        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            channel = serverSocketChannel.accept();
            if(channel != null){
                channel.configureBlocking(false);
                channel.register(selector,SelectionKey.OP_READ);
            }
        } catch (IOException e) {
            e.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }
    }

    /**  
     * 方法名: handleReadableRequest
     * 描述:   从backup node 发送的请求中读取元数据快照
     * @param key  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/25 21:56 
     */  
    private void handleReadableRequest(SelectionKey key) throws IOException {

        SocketChannel channel = null;
        try {
            String fsImagePath = ConfigConstant.FS_IMAGE_PATH + "fsimage" + ConfigConstant.FS_IMAGE_SUFFIX;
            File fsImageFile = new File(fsImagePath);
            if(fsImageFile.exists()){
                fsImageFile.delete();
            }

            RandomAccessFile file = null;
            FileOutputStream out = null;
            FileChannel fileChannel = null;
            try{

                channel = (SocketChannel) key.channel();
                ByteBuffer buffer = ByteBuffer.allocate(1024*1024);

                int total = 0;
                int length = -1;
                if((length = channel.read(buffer)) > 0){
                    file = new RandomAccessFile(fsImagePath,"rw");
                    out = new FileOutputStream(file.getFD());
                    fileChannel = out.getChannel();

                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();

                    total += length;
                }else{
                    //没有读取到任何数据  说明该channel 上报数据已经完成
                    channel.close();
                }

                while (channel.read(buffer)>0){
                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();
                }

                if(total > 0){
                    System.out.println("接收fsimage并写入磁盘成功...");
                    fileChannel.force(false);
                    key.interestOps(key.interestOps() &~ SelectionKey.OP_READ | SelectionKey.OP_WRITE);
//                    channel.register(selector,SelectionKey.OP_WRITE);
                }
            }finally {
                FileUtil.closeOutputFile(file,out,fileChannel);
            }

        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }

    }

    /**  
     * 方法名: handleWritableRequest
     * 描述:  给backup node 返回响应
     * @param key  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/25 22:11 
     */  
    private void handleWritableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            ByteBuffer buffer = ByteBuffer.wrap("SUCESS".getBytes());
            channel = (SocketChannel) key.channel();
            channel.write(buffer);

            System.out.println("fsiamge上传完毕，返回响应给backup node...");

            key.interestOps(key.interestOps() &~ SelectionKey.OP_WRITE | SelectionKey.OP_READ);
//            channel.register(selector,SelectionKey.OP_WRITE);
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }

    }

}
