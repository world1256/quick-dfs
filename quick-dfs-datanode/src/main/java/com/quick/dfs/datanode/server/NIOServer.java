package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ConfigConstant;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/30 11:20
 **/
public class NIOServer extends Thread{

    private Selector selector;

    /**
     * 处理连接请求的processor
     */
    private List<NIOProcessor> processors = new ArrayList<>();

    private NameNodeRpcClient nameNode;

    public NIOServer(NameNodeRpcClient nameNode){
        this.nameNode = nameNode;
        ServerSocketChannel channel = null;
        try {
            selector = Selector.open();
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(ConfigConstant.DATA_NODE_UPLOAD_PORT),100);
            channel.register(selector, SelectionKey.OP_ACCEPT);

            NetworkResponseQueue responseQueue = NetworkResponseQueue.getInstance();
            //启动指定数量的worker  来具体处理文件的上传
            for(int i = 0;i < ConfigConstant.DATA_NODE_NIO_PROCESSOR_COUNT;i++){
                NIOProcessor processor = new NIOProcessor(i);
                processors.add(processor);
                processor.start();

                responseQueue.initResponseQueue(i);
            }

            //启动指定数量的worker  来具体处理文件的上传
            for(int i = 0;i < ConfigConstant.DATA_NODE_IO_THREAD_COUNT;i++){
                new IOThread(nameNode).start();
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

                    if (key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                        SocketChannel channel = serverSocketChannel.accept();
                        if(channel != null){
                            channel.configureBlocking(false);

                            int processorIndex = new Random().nextInt(ConfigConstant.DATA_NODE_NIO_PROCESSOR_COUNT);
                            NIOProcessor processor = processors.get(processorIndex);
                            processor.addChannel(channel);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
