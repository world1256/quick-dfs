package com.quick.dfs.client;

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
     * @param file
     * @param fileSize  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/30 11:02 
    */  
    public static void sendFile(String hostName,byte[] file,long fileSize){
        SocketChannel channel = null;
        Selector selector = null;
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

                        if(channel.isConnectionPending()){
                            channel.finishConnect();

                            int totalLength = 8 + (int)fileSize;
                            ByteBuffer buffer = ByteBuffer.allocate(totalLength);
                            buffer.putLong(fileSize);
                            buffer.put(file);
                            buffer.flip();
                            channel.write(buffer);

                            key.interestOps(key.interestOps() &~ SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                        }
                    }else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);

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
    }
}
