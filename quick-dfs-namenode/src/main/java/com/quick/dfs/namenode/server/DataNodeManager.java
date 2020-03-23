package com.quick.dfs.namenode.server;

import com.quick.dfs.thread.Daemon;

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

    /**
     * dataNode 活性 检测间隔
     */
    private static long DATA_NODE_ALIVE_MONITOR_INTERVAL = 30 * 1000L;

    /**
     * 判断dataNode 宕机 hearbeat间隔时间阈值
     */
    private static long DATA_NODE_DEAD_THRESHOLD = 90 * 1000L;

    private Map<String,DataNodeInfo> dataNodes = new ConcurrentHashMap<String, DataNodeInfo>();

    /**
     * 方法名: DataNodeManager
     * 描述:  初始化   启动DataNode活性检测线程
     * @param
     * @return
     * 作者: fansy
     * 日期: 2020/3/20 19:58
     */
    public DataNodeManager(){
        new DataNodeAliveMonitor().start();
    }

    /**
     * DataNode注册到NameNode中
     * @param ip
     * @param hostName
     * @return
     */
    public boolean register(String ip,String hostName){
        DataNodeInfo  dataNode = new DataNodeInfo(ip,hostName);
        dataNodes.put(ip + "-" +hostName,dataNode);
        return true;
    }

    /**  
     * 方法名: heatbeat
     * 描述: 接收dataNode心跳  更新对应的最后上报心跳时间
     * @param ip
     * @param hostName  
     * @return boolean  
     * 作者: fansy 
     * 日期: 2020/3/20 19:49 
     */  
    public boolean heatbeat(String ip,String hostName){
        String key = ip + "-" +hostName;
        DataNodeInfo dataNode = dataNodes.get(key);
        if(dataNode != null){
            dataNode.setLastHeartbeatTime(System.currentTimeMillis());
        }
        return true;
    }

    /**
     * 定时检测 DataNode活性的后台线程
     */
    class DataNodeAliveMonitor extends Daemon {

        @Override
        public void run() {
            while (true){
                try{
                    List<String> deadDataNodes = new ArrayList<String>();
                    dataNodes.forEach((key,dataNode)->{
                        if(System.currentTimeMillis() - dataNode.getLastHeartbeatTime() > DATA_NODE_DEAD_THRESHOLD){
                            deadDataNodes.add(key);
                        }
                    });

                    for(String key : deadDataNodes){
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
