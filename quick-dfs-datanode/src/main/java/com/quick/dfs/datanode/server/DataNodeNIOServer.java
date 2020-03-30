package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ConfigConstant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 11:20
 **/
public class DataNodeNIOServer extends Thread{

    private Selector selector;

    public DataNodeNIOServer(){
        ServerSocketChannel channel = null;
        try {
            selector = Selector.open();
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(ConfigConstant.DATA_NODE_UPLOAD_PORT),100);
            channel.register(selector, SelectionKey.OP_ACCEPT);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {

    }
}
