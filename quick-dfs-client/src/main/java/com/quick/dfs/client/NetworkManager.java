package com.quick.dfs.client;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.ConnectionStatus;
import com.quick.dfs.constant.ResponseStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 网络连接管理组件
 * @作者: fansy
 * @日期: 2020/4/16 9:25
 **/
public class NetworkManager {

    /**
     * 检测网络连接请求超时间隔
     */
    private static final long REQUEST_TIMEOUT_CHECK_INTERVAL = 1000L;

    /**
     * 网络连接请求超时时间
     */
    private static final long REQUEST_TIMEOUT = 30 * 1000L;

    private Selector selector;

    /**
     * 所有连接
     */
    private Map<String, SelectionKey> connections;

    /**
     * 每个连接的状态
     */
    private Map<String,Integer> connectStatus;

    /**
     * 等待建立连接的主机
     */
    private ConcurrentLinkedQueue<String> waitingConnectHosts;

    /**
     * 等待发送的网络请求
     */
    private Map<String,ConcurrentLinkedQueue<NetWorkRequest>> waitingRequests;

    /**
     * 准备发送的网络请求
     */
    private Map<String,NetWorkRequest> toSendRequests;

    /**
     * 处理完成的网络请求响应
     */
    private Map<String,NetworkResponse> completedResponses;

    /**
     * 未处理完成的网络请求响应
     */
    private Map<String,NetworkResponse> unCompletedResponses;

    public NetworkManager(){
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.connections = new ConcurrentHashMap<>();
        this.connectStatus = new ConcurrentHashMap<>();
        this.waitingConnectHosts = new ConcurrentLinkedQueue<>();
        this.waitingRequests = new ConcurrentHashMap<>();
        this.toSendRequests = new ConcurrentHashMap<>();
        this.completedResponses = new ConcurrentHashMap<>();
        this.unCompletedResponses = new ConcurrentHashMap<>();

        new NetWorkThread().start();
        new RequestTimeOutCheckThread().start();
    }

    /**
     * @方法名: maybeConnect
     * @描述:   尝试和指定主机建立连接
     * @param host
     * @return void
     * @作者: fansy
     * @日期: 2020/4/16 14:16
    */
    public Boolean maybeConnect(String host){
        synchronized (this){
            if(!connectStatus.containsKey(host) ||
                connectStatus.get(host).equals(ConnectionStatus.DIS_CONNECTED)){
                connectStatus.put(host, ConnectionStatus.CONNECTING);
                waitingConnectHosts.offer(host);
            }
            while (connectStatus.get(host).equals(ConnectionStatus.CONNECTING)){
                try {
                    wait(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if(connectStatus.get(host).equals(ConnectionStatus.CONNECTING)){
                return false;
            }

            return true;
        }
    }

    /**
     * @方法名: sendRequest
     * @描述:   发送网络请求
     * @param request
     * @return void
     * @作者: fansy
     * @日期: 2020/4/16 14:17
    */
    public void sendRequest(NetWorkRequest request){
        waitingRequests.get(request.getHostname()).offer(request);
    }

    /**
     * @方法名: waitResponse
     * @描述:   等待请求响应
     * @param requestId  
     * @return com.quick.dfs.client.NetworkResponse  
     * @作者: fansy
     * @日期: 2020/4/16 16:48 
    */  
    public NetworkResponse waitResponse(String requestId) throws Exception {
        NetworkResponse response = null;
        while((response = completedResponses.get(requestId)) == null){
            Thread.sleep(100);
        }

        toSendRequests.remove(response.getHostname());
        completedResponses.remove(requestId);
        
        return response;
    }

    /**
     * 网络请求处理线程    这里参考了kafka的实现方式
     */
    class NetWorkThread extends Thread{
        @Override
        public void run() {
            while (true){

                //尝试建立连接
                tryConnect();

                //准备需要发送的请求
                prepareRequests();

                //处理网络请求
                poll();
            }
        }

        /**
         * @方法名: tryConnect
         * @描述:  将等待连接队列中的网络连接进行连接注册
         * @param
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 14:18
         */
        private void tryConnect(){
            String host = null;
            SocketChannel channel = null;
            try{
                while((host = waitingConnectHosts.poll()) != null){
                    channel = SocketChannel.open();
                    channel.configureBlocking(false);
                    channel.connect(new InetSocketAddress(host, ConfigConstant.DATA_NODE_UPLOAD_PORT));
                    channel.register(selector, SelectionKey.OP_CONNECT);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * @方法名: prepareRequests
         * @描述:   准备一批马上发送出去的请求
         *      每个host取一个请求出来  使请求尽量均匀分布在各个dataNode上
         * @param
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 15:22
         */
        private void prepareRequests(){
            for(String host : waitingRequests.keySet()){
                ConcurrentLinkedQueue<NetWorkRequest> requestQueue = waitingRequests.get(host);
                if(!requestQueue.isEmpty() && !toSendRequests.containsKey(host)){
                    toSendRequests.put(host,requestQueue.poll());
                    connections.get(host).interestOps(SelectionKey.OP_WRITE);
                }
            }
        }

        /**
         * @方法名: poll
         * @描述:   处理网络连接请求
         * @param
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 14:18
         */
        private void poll(){
            try{
                int count = selector.select(500);
                if(count <= 0){
                    return;
                }

                Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                while (iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    SocketChannel channel = (SocketChannel) key.channel();

                    if(key.isConnectable()){
                        finishConnect(key,channel);
                    }else if(key.isWritable()){
                        sendRequest(key,channel);
                    }else if(key.isReadable()){
                        readResponse(key,channel);
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * @方法名: finishConnect
         * @描述:   完成连接建立
         * @param key
         * @param channel
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 15:39
        */
        private void finishConnect(SelectionKey key,SocketChannel channel){
            String host = null;
            try{
                if(channel.isConnectionPending()){
                    while(!channel.finishConnect()){
                        Thread.sleep(100);
                    }
                }
                host = ((InetSocketAddress)channel.getRemoteAddress()).getHostName();
                System.out.println("连接建立完成，host:"+host);

                waitingRequests.put(host,new ConcurrentLinkedQueue<>());
                connectStatus.put(host,ConnectionStatus.CONNECTED);
                connections.put(host,key);
            }catch (Exception e){
                e.printStackTrace();
                if(host != null){
                    connectStatus.put(host,ConnectionStatus.DIS_CONNECTED);
                }
            }

        }

        /**
         * @方法名: sendRequest
         * @描述:   发送请求
         * @param key
         * @param channel
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 15:39
        */
        private void sendRequest(SelectionKey key,SocketChannel channel){
            String host = null;
            try{
                host = ((InetSocketAddress)channel.getRemoteAddress()).getHostName();

                NetWorkRequest request = toSendRequests.get(host);


                ByteBuffer buffer = request.getBuffer();

                while (buffer.hasRemaining()){
                    channel.write(buffer);
                }

                System.out.println("请求发送完毕，hos:"+host);

                request.setSendTime(System.currentTimeMillis());

                key.interestOps(SelectionKey.OP_READ);

            }catch (Exception e){
                e.printStackTrace();

                key.interestOps(key.interestOps() &~ SelectionKey.OP_WRITE);

                if(host != null){
                    NetWorkRequest request = toSendRequests.get(host);

                    NetworkResponse response = new NetworkResponse();
                    response.setHostname(host);
                    response.setRequestId(request.getId());
                    response.setStatus(ResponseStatus.STATUS_FAILURE);

                    if(request.isNeedResponse()){
                        completedResponses.put(request.getId(),response);
                    }else{
                        if(request.getCallback() != null){
                            request.getCallback().process(response);
                        }

                        toSendRequests.remove(host);
                    }
                }
            }

        }

        /**
         * @方法名: readResponse
         * @描述:   读取网络请求响应
         * @param key
         * @param channel
         * @return void
         * @作者: fansy
         * @日期: 2020/4/16 16:49
        */
        private void readResponse(SelectionKey key,SocketChannel channel) throws Exception{
            String host = ((InetSocketAddress)channel.getRemoteAddress()).getHostName();
            NetWorkRequest request = toSendRequests.get(host);

            NetworkResponse response =null;
            if(request.getRequestType() == ClientRequestType.SEND_FILE){
                response = getSendFileResponse(key,channel,request.getId());
            }else if(request.getRequestType() == ClientRequestType.READ_FILE){
                response = getReadFileResponse(key,channel,request.getId());
            }

            if(!response.isCompleted()){
                return;
            }

            key.interestOps(key.interestOps() &~ SelectionKey.OP_READ);
            if(request.isNeedResponse()){
                completedResponses.put(request.getId(),response);
            }else{
                if(request.getCallback() != null){
                    request.getCallback().process(response);
                }
                toSendRequests.remove(host);
            }

        }

        /**  
         * @方法名: getReadFileResponse
         * @描述:   读取下载文件请求响应
         * @param key
         * @param channel
         * @param requestId  
         * @return com.quick.dfs.client.NetworkResponse  
         * @作者: fansy
         * @日期: 2020/4/20 15:13 
        */  
        private NetworkResponse getReadFileResponse(SelectionKey key,SocketChannel channel,String requestId) throws Exception {
            String host = ((InetSocketAddress)channel.getRemoteAddress()).getHostName();

            NetworkResponse response = null;
            if(unCompletedResponses.containsKey(requestId)){
                response = unCompletedResponses.get(requestId);
            }else{
                response = new NetworkResponse();
                response.setHostname(host);
                response.setRequestId(requestId);
                response.setCompleted(false);
            }

            Long fileLength = null;
            //请求中的文件内容没有读取过  才需要先读取文件长度
            if(response.getBuffer() == null){
                ByteBuffer fileLengthBuffer = null;
                if(response.getFileLengthBuffer() != null){
                    fileLengthBuffer = response.getFileLengthBuffer();
                }else{
                    fileLengthBuffer = ByteBuffer.allocate(8);
                    response.setFileLengthBuffer(fileLengthBuffer);
                }
                channel.read(fileLengthBuffer);

                if(!fileLengthBuffer.hasRemaining()){
                    fileLengthBuffer.rewind();
                    fileLength = fileLengthBuffer.getLong();
                }else{
                    unCompletedResponses.put(requestId,response);
                }
            }

            //读取文件内容
            if(fileLength != null || response.getBuffer() != null){
                ByteBuffer fileBuffer = null;
                if(response.getBuffer() != null){
                    fileBuffer = response.getBuffer();
                }else{
                    fileBuffer = ByteBuffer.allocate(Integer.parseInt(fileLength.toString()));
                    response.setBuffer(fileBuffer);
                }
                channel.read(fileBuffer);
                //文件读取完成
                if(!fileBuffer.hasRemaining()){
                    fileBuffer.rewind();
                    response.setCompleted(true);
                    unCompletedResponses.remove(requestId);
                }else{
                    //文件读取未完成，缓存起来
                    unCompletedResponses.put(requestId,response);
                }
            }
            return response;
        }


        /**
         * @方法名: getSendFileResponse
         * @描述: 读取发送文件请求响应
         * @param key
         * @param channel
         * @param requestId
         * @return com.quick.dfs.client.NetworkResponse
         * @作者: fansy
         * @日期: 2020/4/16 16:49
        */
        private NetworkResponse getSendFileResponse(SelectionKey key,SocketChannel channel,String requestId) throws Exception {
            String host = ((InetSocketAddress)channel.getRemoteAddress()).getHostName();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            channel.read(buffer);
            buffer.flip();

            NetworkResponse response = new NetworkResponse();
            response.setHostname(host);
            response.setRequestId(requestId);
            response.setBuffer(buffer);
            response.setStatus(ResponseStatus.STATUS_SUCCESS);
            response.setCompleted(true);
            return response;
        }
    }

    /**
     * 网络请求超时检测线程
     */
    class RequestTimeOutCheckThread extends Thread{
        @Override
        public void run() {
            try{
                while (true){
                    long now = System.currentTimeMillis();
                    for(NetWorkRequest request : toSendRequests.values()){
                        if(now - request.getSendTime() > REQUEST_TIMEOUT){
                            String host = request.getHostname();

                            NetworkResponse response = new NetworkResponse();
                            response.setStatus(ResponseStatus.STATUS_FAILURE);
                            response.setRequestId(request.getId());
                            response.setHostname(host);

                            if(request.isNeedResponse()){
                                completedResponses.put(request.getId(),response);
                            }else{
                                if(request.getCallback() != null){
                                    request.getCallback().process(response);
                                }
                                toSendRequests.remove(host);
                            }

                        }
                    }

                    Thread.sleep(REQUEST_TIMEOUT_CHECK_INTERVAL);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
