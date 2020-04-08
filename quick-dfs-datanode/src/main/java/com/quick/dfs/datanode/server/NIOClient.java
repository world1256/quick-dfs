package com.quick.dfs.datanode.server;

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
