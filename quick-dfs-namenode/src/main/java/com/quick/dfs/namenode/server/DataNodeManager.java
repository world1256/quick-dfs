package com.quick.dfs.namenode.server;

import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.SPLITOR;
import com.quick.dfs.namenode.task.RemoveTask;
import com.quick.dfs.namenode.task.ReplicateTask;
import com.quick.dfs.thread.Daemon;
import sun.awt.windows.ThemeReader;

import java.util.*;
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
     * 元数据管理组件
     */
    private FSNameSystem nameSystem;

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

    public void setNameSystem(FSNameSystem nameSystem) {
        this.nameSystem = nameSystem;
    }

    /**
     * DataNode注册到NameNode中
     * @param ip
     * @param hostName
     * @return
     */
    public boolean register(String ip,String hostName){
        String key = ip + SPLITOR.DATA_NODE_IP_HOST +hostName;
        if(dataNodes.containsKey(key)){
            System.out.println("已经存在该dataNode,不需要再次注册...");
            return false;
        }
        DataNodeInfo  dataNode = new DataNodeInfo(ip,hostName);
        dataNodes.put(key,dataNode);
        System.out.println("dataNode 进行注册，ip: " + ip + " ,hostname: "+hostName);
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
        String key = ip + SPLITOR.DATA_NODE_IP_HOST + hostName;
        DataNodeInfo dataNode = dataNodes.get(key);

        //nameNode中没有该dataNode信息  需要下发命令进行重新注册以及上报存储文件信息
        if(dataNode == null){
            return false;
        }

        dataNode.setLastHeartbeatTime(System.currentTimeMillis());
        return true;
    }

    /**
     * @方法名: allocateDataNodes
     * @描述:   获取文件应该上报到的dataNode
     * @param fileSize
     * @return java.util.List<com.quick.dfs.namenode.server.DataNodeInfo>
     * @作者: fansy
     * @日期: 2020/3/30 10:16
    */
    public List<DataNodeInfo> allocateDataNodes(long fileSize){
        List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();
        for(DataNodeInfo dataNodeInfo : dataNodes.values()){
            dataNodeInfoList.add(dataNodeInfo);
        }

        //根据dataNode存储数据量大小进行排序
        dataNodeInfoList.sort(Comparator.comparing(DataNodeInfo::getStoredDataSize));

        //这里取存储数据量最少的那些dataNode
        List<DataNodeInfo> allocateDataNodes = new ArrayList<>();
        if(dataNodeInfoList.size() >= ConfigConstant.DATA_STORE_REPLICA){
            for(int i = 0; i<ConfigConstant.DATA_STORE_REPLICA; i++){
                allocateDataNodes.add(dataNodeInfoList.get(i));
            }
        }
        return allocateDataNodes;
    }

    /**  
     * @方法名: relocateDataNode
     * @描述:   重新分配一个文件需要上传到的dataNode
     * @param fileSize
     * @param excludeDataNodes  
     * @return com.quick.dfs.namenode.server.DataNodeInfo  
     * @作者: fansy
     * @日期: 2020/4/9 15:15 
    */  
    public DataNodeInfo relocateDataNode(long fileSize,List<DataNodeInfo> excludeDataNodes){
        List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();
        for(DataNodeInfo dataNodeInfo : dataNodes.values()){
            dataNodeInfoList.add(dataNodeInfo);
        }

        //根据dataNode存储数据量大小进行排序
        dataNodeInfoList.sort(Comparator.comparing(DataNodeInfo::getStoredDataSize));

        Set<DataNodeInfo> excludeDataNodeSet = new HashSet<>();
        for(DataNodeInfo excludeDataNode : excludeDataNodes){
            excludeDataNodeSet.add(excludeDataNode);
        }

        //这里取存储数据量最少的那些dataNode
        if(dataNodeInfoList.size() >= ConfigConstant.DATA_STORE_REPLICA){
            for(DataNodeInfo dataNodeInfo : dataNodeInfoList){
                if(!excludeDataNodeSet.contains(dataNodeInfo)){
                    return dataNodeInfo;
                }
            }
        }
        return null;
    }

    /**  
     * 方法名: setStoredDataSize
     * 描述:   更新dataNode存储数据总量
     * @param ip
     * @param hostname
     * @param storedDataSize  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/4 13:21 
     */  
    public void setStoredDataSize(String ip,String hostname,long storedDataSize){
        String key = ip + SPLITOR.DATA_NODE_IP_HOST +hostname;
        DataNodeInfo dataNode = this.dataNodes.get(key);
        dataNode.setStoredDataSize(storedDataSize);
    }


    /**
     * 方法名: getDataNode
     * 描述:   获取dataNode信息
     * @param ip
     * @param hostname
     * @return com.quick.dfs.namenode.server.DataNodeInfo
     * 作者: fansy
     * 日期: 2020/4/4 12:53
     */
    public DataNodeInfo getDataNode(String ip,String hostname){
        String key = ip + SPLITOR.DATA_NODE_IP_HOST +hostname;
        return dataNodes.get(key);
    }

    /**  
     * @方法名: createLostReplicaTask
     * @描述: 创建文件复制任务
     * @param deadDataNode
     * @return void
     * @作者: fansy
     * @日期: 2020/4/7 14:15 
    */  
    public void createLostReplicaTask(DataNodeInfo deadDataNode){
        List<String> files = this.nameSystem.getFilesByDataNode(deadDataNode.getIp(),deadDataNode.getHostName());

        for(String file : files){
            String filename = file.split(SPLITOR.FILE_NAME_LENGTH)[0];
            Long fileLength = Long.valueOf(file.split(SPLITOR.FILE_NAME_LENGTH)[1]);

            //文件所在源数据节点
            DataNodeInfo sourceDataNode = this.nameSystem.getReplicaSource(filename,deadDataNode);
            //文件需要复制到的数据节点
            DataNodeInfo destDataNode = allocateReplicaDataNode(fileLength);

            if(sourceDataNode != null && destDataNode != null){
                ReplicateTask replicateTask = new ReplicateTask(filename,fileLength,sourceDataNode,destDataNode);
                destDataNode.addReplicateTask(replicateTask);
            }
        }

    }

    /**  
     * @方法名: allocateReplicaDataNode
     * @描述:   找到一个文件复制任务的目标数据节点
     * @param fileSize  
     * @return com.quick.dfs.namenode.server.DataNodeInfo  
     * @作者: fansy
     * @日期: 2020/4/7 15:03
    */  
    public DataNodeInfo allocateReplicaDataNode(long fileSize){
        List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();
        for(DataNodeInfo dataNodeInfo : dataNodes.values()){
            dataNodeInfoList.add(dataNodeInfo);
        }

        //根据dataNode存储数据量大小进行排序
        dataNodeInfoList.sort(Comparator.comparing(DataNodeInfo::getStoredDataSize));
        if(dataNodeInfoList.size()>0){
            return  dataNodeInfoList.get(0);
        }
        return null;
    }

    /**  
     * 方法名: rebalance
     * 描述:   数据节点 数据容量重平衡  使各个节点数据尽量均衡
     * @param   
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/12 13:15 
     */  
    public void rebalance(){
        long totalDataSize = 0;
        long avgDataSize = 0;

        //数据节点上的文件容量   这里是分配任务  不对dataNode上保存的实际容量直接修改
        //使用这个map来进行计算
        Map<String,Long> dataNodeSize = new HashMap<>();
        for(DataNodeInfo dataNodeInfo : dataNodes.values()){
            totalDataSize += dataNodeInfo.getStoredDataSize();
            avgDataSize = totalDataSize/dataNodes.size();
            dataNodeSize.put(dataNodeInfo.getIp()+SPLITOR.DATA_NODE_IP_HOST+dataNodeInfo.getHostName(),dataNodeInfo.getStoredDataSize());
        }

        //数据总量大于平均值的节点   需要将数据迁移到其他节点
        List<DataNodeInfo> sourceDataNodes = new ArrayList<>();
        //数据总量小于平均值的节点   需要从其他节点接收数据
        List<DataNodeInfo> destDataNodes = new ArrayList<>();

        for(DataNodeInfo dataNodeInfo : dataNodes.values()){
            if(dataNodeInfo.getStoredDataSize() > avgDataSize){
                sourceDataNodes.add(dataNodeInfo);
            }
            if(dataNodeInfo.getStoredDataSize() < avgDataSize){
                destDataNodes.add(dataNodeInfo);
            }
        }

        List<RemoveTask> removeTasks = new ArrayList<>();
        for(DataNodeInfo sourceDataNode : sourceDataNodes){
            long maxReplicateSize = avgDataSize - sourceDataNode.getStoredDataSize();
            for(DataNodeInfo destDataNode : destDataNodes){
                String key = destDataNode.getIp()+SPLITOR.DATA_NODE_IP_HOST+destDataNode.getHostName();
                long storedDataSize = dataNodeSize.get(key);
                //如果目标节点能接收全部需要复制的文件  则一次处理
                if(avgDataSize - storedDataSize >= maxReplicateSize){
                    createRebalanceTask(sourceDataNode,destDataNode,maxReplicateSize,removeTasks);
                    dataNodeSize.put(key,storedDataSize-maxReplicateSize);
                    break;
                }
                //不能完全接收  那么接收一部分文件
                else if(storedDataSize < avgDataSize){
                    long maxRemoveSize = avgDataSize - destDataNode.getStoredDataSize();
                    long removedSize = createRebalanceTask(sourceDataNode,destDataNode,maxRemoveSize,removeTasks);
                    maxReplicateSize -= removedSize;
                    dataNodeSize.put(key,storedDataSize-removedSize);
                }
            }
        }
        new DelayRemoveReplicateThread(removeTasks).start();
    }

    /**  
     * 方法名: createRebalanceTask
     * 描述:   创建重平衡任务
     * @param sourceDataNode
     * @param destDataNode
     * @param maxReplicateSize
     * @param removeTasks  
     * @return long  
     * 作者: fansy 
     * 日期: 2020/4/12 14:04 
     */  
    public long createRebalanceTask(DataNodeInfo sourceDataNode,DataNodeInfo destDataNode
            ,long maxReplicateSize,List<RemoveTask> removeTasks){
        long removedSize = 0;
        List<String> files = this.nameSystem.getFilesByDataNode(sourceDataNode.getIp(),sourceDataNode.getHostName());
        for(String file : files){
            String fileName = file.split(SPLITOR.FILE_NAME_LENGTH)[0];
            long fileLength = Long.parseLong(file.split(SPLITOR.FILE_NAME_LENGTH)[1]);
            if(removedSize + fileLength > maxReplicateSize){
                break;
            }

            //文件删除任务   dataNode的数据大小   不在这里做处理   等后面分发removeTask时再减去容量大小
            RemoveTask removeTask = new RemoveTask(fileName,sourceDataNode);
            removeTasks.add(removeTask);

            //文件复制任务  dataNode的数据大小  不在这里做处理   等上报接收文件成功的消息过来时再增加大小
            ReplicateTask replicateTask  = new ReplicateTask(fileName,fileLength,sourceDataNode,destDataNode);
            destDataNode.addReplicateTask(replicateTask);

            removedSize += fileLength;
        }
        return removedSize;
    }

    /**
     * 延时删除副本线程
     */
    class DelayRemoveReplicateThread extends Thread{

        private List<RemoveTask> removeTasks;

        public DelayRemoveReplicateThread(List<RemoveTask> removeTasks){
            this.removeTasks = removeTasks;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();

            while (true){
                try{
                    long now = System.currentTimeMillis();
                    //这里需要延迟一定时间再删除   确保重平衡产生的复制任务完全跑完
                    if(now - start >= ConfigConstant.REBALANCE_REMOVE_REPLICATE_DELAY){
                        for(RemoveTask removeTask : removeTasks){
                            removeTask.getDataNodeInfo().addRemoveTask(removeTask);
                        }
                        break;
                    }
                    Thread.sleep(60 * 1000);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
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
                        //从dataNode信息中移除死掉的dataNode
                        DataNodeInfo deadDataNode = dataNodes.remove(key);

                        //为死掉的dataNode创建文件复制任务
                        createLostReplicaTask(deadDataNode);

                        //移除掉死掉的dataNode的相关信息
                        nameSystem.removeDeadDataNode(deadDataNode);
                    }

                    Thread.sleep(DATA_NODE_ALIVE_MONITOR_INTERVAL);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }
    }

}
