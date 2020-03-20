package com.quick.dfs.namenode.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @项目名称: quick-dfs
 * @描述: DataNode管理组件
 * @作者: fansy
 * @日期: 2020/3/18 15:14
 **/
public class DataNodeManager {

    private Map<String,DataNodeInfo> dataNodes = new ConcurrentHashMap<String, DataNodeInfo>();

    /**
     * DataNode注册到NameNode中
     * @param ip
     * @param hostName
     * @return
     */
    public boolean register(String ip,String hostName){
        DataNodeInfo  dataNode = new DataNodeInfo(ip,hostName);
        dataNodes.put(ip,dataNode);
        return true;
    }

}
