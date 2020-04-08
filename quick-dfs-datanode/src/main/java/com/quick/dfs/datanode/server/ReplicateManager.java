package com.quick.dfs.datanode.server;

import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.util.FileUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @项目名称: quick-dfs
 * @描述: 文件复制任务管理组件
 * @作者: fansy
 * @日期: 2020/4/7 16:45
 **/
public class ReplicateManager {

    private ConcurrentLinkedQueue<JSONObject> replicateTaskQueue = new ConcurrentLinkedQueue<>();

    private NIOClient nioClient = new NIOClient();

    public ReplicateManager(){

        for(int i = 0;i < ConfigConstant.DATA_NODE_UPLOAD_THREAD_COUNT;i++){
            new ReplicateWorker().start();
        }

    }

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

    /**
     * 复制任务执行线程
     */
    class ReplicateWorker extends Thread{
        @Override
        public void run() {
            while(true){
                FileOutputStream out = null;
                FileChannel fileChannel = null;

                try {
                    JSONObject relicateTask = replicateTaskQueue.poll();
                    if(relicateTask == null){
                        Thread.sleep(1000);
                        continue;
                    }

                    String fileName = relicateTask.getString("fileName");
                    JSONObject sourceDataNode = relicateTask.getJSONObject("sourceDataNode");

                    String hostName = sourceDataNode.getString("hostName");

                    //从源数据节点获取文件数据
                    byte[] fileData = nioClient.readFile(hostName,fileName);

                    String absoluteFileName = FileUtil.getAbsoluteFileName(fileName);

                    ByteBuffer buffer = ByteBuffer.wrap(fileData);

                    //文件写入磁盘
                    out = new FileOutputStream(absoluteFileName);
                    fileChannel = out.getChannel();
                    fileChannel.write(buffer);
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    try {
                        FileUtil.closeOutputFile(null,out,fileChannel);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


}
