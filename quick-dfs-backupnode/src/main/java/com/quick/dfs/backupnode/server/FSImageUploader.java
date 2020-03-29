package com.quick.dfs.backupnode.server;

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
 * @描述:  上传fsiamge 到namenode的线程
 * @作者: fansy
 * @日期: 2020/03/25 22:24
 **/
public class FSImageUploader extends Thread{

    private FSImage fsImage;

    public  FSImageUploader(FSImage fsImage){
        this.fsImage = fsImage;
    }

    @Override
    public void run() {
        SocketChannel channel = null;
        Selector selector = null;
        try{
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(ConfigConstant.NAME_NODE_HOST_NAME
                    ,ConfigConstant.FS_IMAGE_UPLOAD_PORT));

            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean uploading = true;
            while (uploading){
                selector.select();
                Iterator<SelectionKey> keyIterator= selector.selectedKeys().iterator();
                while (keyIterator.hasNext()){
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if(key.isConnectable()){
                        channel = (SocketChannel) key.channel();
                        if(channel.isConnectionPending()){
                            channel.finishConnect();
                            ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsImageJosn().getBytes());
                            System.out.println("准备上传fsimage文件数据，大小为：" + buffer.capacity());
                            channel.write(buffer);
                        }
                        key.interestOps(key.interestOps() &~ SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
//                        channel.register(selector,SelectionKey.OP_READ);
                    }else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int length = channel.read(buffer);
                        if(length > 0){
                            String response = new String(buffer.array(),0,length);
                            System.out.println("fsiamge上报成功，响应信息："+response);
                            channel.close();
                            uploading = false;
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
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            if(selector != null){
                try {
                    selector.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
