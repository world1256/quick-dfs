package com.quick.dfs.namenode.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

/**
 * @项目名称: quick-dfs
 * @描述: 响应fsImage上传  服务端
 * @作者: fansy
 * @日期: 2020/3/25 17:20
 **/
public class FSImageUploadServer {

    private Selector selector;

    private void init(){
        ServerSocketChannel serverSocketChannel = null;
        try{
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(9000),100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

}
