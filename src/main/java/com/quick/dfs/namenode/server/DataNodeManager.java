package com.quick.dfs.namenode.server;

import com.quick.dfs.thread.Demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @项目名称: quick-dfs
 * @描述: DataNode管理组件
 * @作者: fansy
 * @日期: 2020/3/18 15:14
 **/
public class DataNodeManager {


    private static long DATA_NODE_ALIVE_MONITOR_INTERVAL = 30*1000L;

    private static long DATA_NODE_DEAD_THRESHOLD = 90 * 1000L;

    /**
     * 集群中的所有DataNode
     */
    private Map<String,DataNodeInfo> dataNodes = new ConcurrentHashMap<String, DataNodeInfo>();

    /**
     * @方法名: DataNodeManager
     * @描述:   初始化的时候启动心跳检测线程
     * @param
     * @return
     * @作者: fansy
     * @日期: 2020/3/20 17:10
    */
    public DataNodeManager(){
        new DataNodeAliveMonitor().start();
    }

    /**
     * @方法名: register
     * @描述:  DataNode注册到NameNode中
     * @param ip
     * @param hostName  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/20 17:08 
    */  
    public boolean register(String ip,String hostName){
        DataNodeInfo  dataNode = new DataNodeInfo(ip,hostName);
        dataNodes.put(ip+"-"+hostName,dataNode);
        return true;
    }

    /**  
     * @方法名: heartBeat   
     * @描述:  接收dataNode心跳信息  更新最后心跳时间
     * @param ip
     * @param hostName  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/20 17:08 
    */  
    public boolean heartBeat(String ip,String hostName){
        String key = ip + "-" + hostName;
        DataNodeInfo dataNode = dataNodes.get(key);
        if(dataNode != null){
            dataNode.setLastHeartBeatTime(System.currentTimeMillis());
        }
        return true;
    }

    /**
     * 定时检查DataNode活性的后台线程
     */
    class DataNodeAliveMonitor extends Demo{
        @Override
        public void run() {
            while(true){
                try{
                    List<String> deadDataNodes = new ArrayList<>();
                    dataNodes.forEach((key,dataNode)->{
                        if(System.currentTimeMillis() - dataNode.getLastHeartBeatTime() > DATA_NODE_DEAD_THRESHOLD){
                            deadDataNodes.add(key);
                        }
                    });

                    for(String key : deadDataNodes){
                        System.out.println("DataNode "+key+" do not send hearbeat in "+DATA_NODE_DEAD_THRESHOLD+"s," +
                                "mark it dead,remove from the dataNodes.");
                        dataNodes.remove(key);
                    }

                    Thread.sleep(DATA_NODE_ALIVE_MONITOR_INTERVAL);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

}
