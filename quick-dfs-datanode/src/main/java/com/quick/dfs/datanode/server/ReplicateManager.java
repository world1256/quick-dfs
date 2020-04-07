package com.quick.dfs.datanode.server;

import com.alibaba.fastjson.JSONObject;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 文件复制任务管理组件
 * @作者: fansy
 * @日期: 2020/4/7 16:45
 **/
public class ReplicateManager {

    private ConcurrentLinkedQueue<JSONObject> replicateTaskQueue = new ConcurrentLinkedQueue<>();

    /**
     * @方法名: addReplicateTask
     * @描述:   添加复制任务
     * @param relicateTask
     * @return void
     * @作者: fansy
     * @日期: 2020/4/7 16:48
    */
    public void addReplicateTask(JSONObject relicateTask){
        this.replicateTaskQueue.offer(relicateTask);
    }

}
