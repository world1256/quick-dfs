package com.quick.dfs.datanode.server;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/4/14 14:43
 **/
public class NIOProcessor  extends Thread{

    private Selector selector;

    /**
     * 等待注册的网络连接队列
     */
    private ConcurrentLinkedQueue<SocketChannel> channelQueue = new ConcurrentLinkedQueue<>();

    /**
     * 监听时的最大阻塞时间
     */
    private static final Long POLL_MAX_BLOCK_TIME = 1000L;

    public NIOProcessor(){
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true){
            registerQueuedChannel();

            poll();
        }
    }

    /**
     * @方法名: addChannel
     * @描述:  添加一个监听的网络连接
     * @param channel
     * @return void
     * @作者: fansy
     * @日期: 2020/4/14 14:45
     */
    public void addChannel(SocketChannel channel){
        this.channelQueue.offer(channel);
        this.selector.wakeup();
    }

    /**
     * @方法名: registerQueuedChannel
     * @描述:   将缓存队列中的channel 注册到selector上
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/4/14 15:00
    */
    private void registerQueuedChannel(){
        SocketChannel channel = null;
        while ((channel = channelQueue.poll())!=null){
            try {
                channel.register(selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @方法名: poll
     * @描述:   监听各个连接请求
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/4/14 15:13
    */
    private void poll(){
        try {
            int keys = this.selector.select(POLL_MAX_BLOCK_TIME);
            if(keys > 0){
                Iterator<SelectionKey> keyIterator = this.selector.selectedKeys().iterator();
                while (keyIterator.hasNext()){
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if(key.isReadable()){
                        SocketChannel channel = (SocketChannel) key.channel();

                        NetworkRequest networkRequest = new NetworkRequest(key,channel);
                        networkRequest.read();

                        if(networkRequest.hasCompletedRead()){

                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
