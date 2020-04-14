package com.quick.dfs.datanode.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求响应队列
 * @作者: fansy
 * @日期: 2020/04/14 19:07
 **/
public class NetworkResponseQueue {

    public static volatile NetworkResponseQueue instance = null;

    public static NetworkResponseQueue getInstance() {
        if(instance == null){
            synchronized (NetworkResponseQueue.class){
                if(instance == null){
                    instance = new NetworkResponseQueue();
                }
            }
        }
        return instance;
    }


    private Map<Integer,ConcurrentLinkedQueue<NetworkResponse>> responseQueues = new HashMap<>();

    public void offer(Integer processorId,NetworkResponse response){
        this.responseQueues.get(processorId).offer(response);
    }

    public NetworkResponse poll(Integer processorId){
        return this.responseQueues.get(processorId).poll();
    }

    public void initResponseQueue(Integer processorId) {
        ConcurrentLinkedQueue<NetworkResponse> responseQueue =
                new ConcurrentLinkedQueue<NetworkResponse>();
        responseQueues.put(processorId, responseQueue);
    }
}


