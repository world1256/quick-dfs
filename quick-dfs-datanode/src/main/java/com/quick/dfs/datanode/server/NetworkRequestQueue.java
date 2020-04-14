package com.quick.dfs.datanode.server;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求队列
 * @作者: fansy
 * @日期: 2020/04/14 19:07
 **/
public class NetworkRequestQueue {

    public static volatile  NetworkRequestQueue instance = null;

    public static NetworkRequestQueue getInstance() {
        if(instance == null){
            synchronized (NetworkRequestQueue.class){
                if(instance == null){
                    instance = new NetworkRequestQueue();
                }
            }
        }
        return instance;
    }


    private ConcurrentLinkedQueue<NetworkRequest> requestQueue = new ConcurrentLinkedQueue<>();

    public void offer(NetworkRequest request){
        this.requestQueue.offer(request);
    }

    public NetworkRequest poll(){
        return this.requestQueue.poll();
    }

}


