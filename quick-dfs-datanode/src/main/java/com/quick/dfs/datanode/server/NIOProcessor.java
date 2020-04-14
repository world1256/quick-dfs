package com.quick.dfs.datanode.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求处理线程
 * @作者: fansy
 * @日期: 2020/4/14 14:43
 **/
public class NIOProcessor  extends Thread{

    private Selector selector;

    /**
     * 标识
     */
    private Integer processorId;

    /**
     * 等待注册的网络连接队列
     */
    private ConcurrentLinkedQueue<SocketChannel> channelQueue = new ConcurrentLinkedQueue<>();

    /**
     * 监听时的最大阻塞时间
     */
    private static final Long POLL_MAX_BLOCK_TIME = 1000L;

    /**
     * 缓存 网络连接
     */
    private Map<String,SelectionKey> cachedKeys = new HashMap<>();

    /**
     * 缓存 没处理完毕的网络请求
     */
    private Map<String,NetworkRequest> cachedRequests = new HashMap<>();

    /**
     * 缓存 没处理完毕的网络请求响应
     */
    private Map<String,NetworkResponse> cachedResponses = new HashMap<>();

    public NIOProcessor(Integer processorId){
        try {
            this.processorId = processorId;
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (true){
            //注册网络连接
            registerQueuedChannel();

            //处理网络连接请求
            poll();

            //将响应队列中的响应请求缓存起来
            cacheQueuedResponse();
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
     * @描述:   处理连接请求
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

                    SocketChannel channel = (SocketChannel) key.channel();
                    String client = channel.getRemoteAddress().toString();
                    if(key.isReadable()){
                        NetworkRequest networkRequest;
                        if(cachedRequests.containsKey(client)){
                            networkRequest = cachedRequests.get(client);
                        }else{
                            networkRequest = new NetworkRequest(key,channel);
                            networkRequest.setProcessorId(processorId);
                        }
                        networkRequest.read();

                        if(networkRequest.hasCompletedRead()){
                            NetworkRequestQueue.getInstance().offer(networkRequest);

                            cachedKeys.put(client,key);
                            cachedRequests.remove(client);

                            key.interestOps(key.interestOps() &~ SelectionKey.OP_READ);
                        }else{
                            cachedRequests.put(client,networkRequest);
                        }
                    }else if(key.isWritable()){
                        NetworkResponse response = cachedResponses.get(client);
                        ByteBuffer buffer = response.getBuffer();
                        channel.write(buffer);

                        cachedResponses.remove(client);
                        cachedKeys.remove(client);

                        key.interestOps(key.interestOps() &~ SelectionKey.OP_WRITE);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**  
     * 方法名: cacheQueuedResponse
     * 描述:  将响应队列中的响应请求缓存起来
     * @param   
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/14 20:28 
     */  
    private void cacheQueuedResponse(){
        NetworkResponseQueue responseQueue = NetworkResponseQueue.getInstance();
        NetworkResponse response = null;
        while((response = responseQueue.poll(processorId)) != null){
            String client = response.getClient();
            cachedResponses.put(client,response);
            cachedKeys.get(client).interestOps(SelectionKey.OP_WRITE);
        }
    }
}
