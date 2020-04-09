package com.quick.dfs.client;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.constant.ConfigConstant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 10:40
 **/
public class NIOClient {

    /**
     * @方法名: sendFile
     * @描述:   上报文件到dataNode
     * @param hostName
     * @param fileName
     * @param file
     * @param fileSize  
     * @return boolean
     * @作者: fansy
     * @日期: 2020/3/30 11:02 
    */  
    public boolean sendFile(String hostName,String fileName,byte[] file,long fileSize){
        //建立一个短连接  发送完一个文件后就关闭连接
        SocketChannel channel = null;
        Selector selector = null;
        ByteBuffer buffer = null;
        try{
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostName, ConfigConstant.DATA_NODE_UPLOAD_PORT));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while (sending){
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while(iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    if(key.isConnectable()){
                        channel = (SocketChannel) key.channel();

                        //完成连接建立
                        if(channel.isConnectionPending()){
                          channel.finishConnect();
                        }

                        byte[] fileNameBytes = fileName.getBytes();

                        //依次存放请求类型+文件名长度+文件名+文件长度+文件内容
                        int totalLength = 4 + 4 + fileNameBytes.length + 8 + (int)fileSize;

                        buffer = ByteBuffer.allocate(totalLength);

                        buffer.putInt(ClientRequestType.SEND_FILE);
                        buffer.putInt(fileNameBytes.length);
                        buffer.put(fileNameBytes);
                        buffer.putLong(fileSize);
                        buffer.put(file);

                        buffer.rewind();

                        channel.write(buffer);

                        if(buffer.hasRemaining()){
                            System.out.println("文件没有发送完毕,下次继续发送...");
                            key.interestOps(key.interestOps() &~ SelectionKey.OP_CONNECT | SelectionKey.OP_WRITE);
                        }else{
                            System.out.println("client文件上传完毕,准备读取服务端的响应...");
                            //不再监听连接事件   监听读取事件
                            key.interestOps(key.interestOps() &~ SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        }
                    }else if(key.isWritable()){
                        channel = (SocketChannel) key.channel();
                        channel.write(buffer);
                        if(!buffer.hasRemaining()){
                            System.out.println("client文件上传完毕,准备读取服务端的响应...");
                            key.interestOps(key.interestOps() &~ SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                        }
                    } else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        buffer = ByteBuffer.allocate(1024);

                        int length = channel.read(buffer);
                        if(length > 0){
                            String response = new String(buffer.array(),0,length);
                            System.out.println("["+Thread.currentThread().getName()+"] 发送文件完成,收到响应："+response);
                            sending = false;
                        }
                    }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            if(channel != null){
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(selector != null){
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    /**
     * 方法名: readFile
     * 描述:   从dataNode下载文件
     * @param hostName
     * @param fileName
     * @return byte[]
     * 作者: fansy
     * 日期: 2020/4/6 16:04
     */
    public  byte[] readFile(String hostName,String fileName){
        byte[] fileBytes = null;

        ByteBuffer fileLengthBuffer = null;
        Long fileLength = null;
        ByteBuffer fileBuffer = null;

        SocketChannel channel = null;
        Selector selector = null;
        try{
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostName,ConfigConstant.DATA_NODE_UPLOAD_PORT));
            selector = Selector.open();
            channel.register(selector,SelectionKey.OP_CONNECT);

            boolean reading = true;

            while (reading){
                selector.select();
                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if(key.isConnectable()){
                        channel = (SocketChannel) key.channel();
                        if(channel.isConnectionPending()){
                            channel.finishConnect();
                        }

                        //依次存放请求类型+文件名长度+文件名
                        int totalLength = 4 + 4 + fileName.getBytes().length;
                        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
                        buffer.putInt(ClientRequestType.READ_FILE);
                        buffer.putInt(fileName.getBytes().length);
                        buffer.put(fileName.getBytes());
                        buffer.rewind();

                        channel.write(buffer);
                        System.out.println("发送文件读取请求到 "+hostName+" 完成...");
                        key.interestOps(key.interestOps() &~ SelectionKey.OP_CONNECT | SelectionKey.OP_READ);

                    }else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        if(fileLength == null){
                            if(fileBuffer == null){
                                fileLengthBuffer = ByteBuffer.allocate(8);
                            }
                            channel.read(fileLengthBuffer);
                            if(!fileBuffer.hasRemaining()){
                                fileLengthBuffer.rewind();
                                fileLength = fileLengthBuffer.getLong();
                            }
                        }else{
                            if(fileBuffer == null){
                                fileBuffer = ByteBuffer.allocate(fileLength.intValue());
                            }
                            channel.read(fileBuffer);
                            if(!fileBuffer.hasRemaining()){
                                fileBytes = fileBuffer.array();
                                reading = false;
                                System.out.println("dataNode发送过来的文件数据接收完毕...");
                            }
                        }
                    }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(channel != null){
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(selector != null){
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return  fileBytes;
    }




}
