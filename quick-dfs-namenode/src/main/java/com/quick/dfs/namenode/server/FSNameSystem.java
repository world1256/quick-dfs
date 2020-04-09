package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.constant.EditLogOp;
import com.quick.dfs.constant.SPLITOR;
import com.quick.dfs.namenode.task.RemoveTask;
import com.quick.dfs.util.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @项目名称: quick-dfs
 * @描述: 元数据管理组件
 * @作者: fansy
 * @日期: 2020/3/18 15:12
 **/
public class FSNameSystem {

    /**
     * 内存文件目录管理组件
     */
    private FSDirectory directory;

    /**
     * 负责edit log 写入磁盘的组件
     */
    private FSEditLog editLog;

    /**
     * 最近一次checkpoint更新到的txid
     */
    private long checkpointTxid;

    /**
     * datanode 信息管理组件
     */
    private DataNodeManager dataNodeManager;

    /**
     * 文件副本信息   文件保存在哪些dataNode上
     */
    private Map<String,List<DataNodeInfo>> replicasByFileName = new ConcurrentHashMap<>();

    /**
     * 每个dataNode上有哪些文件
     */
    private Map<String,List<String>> filesByDataNode = new ConcurrentHashMap<>();

    public FSNameSystem(DataNodeManager dataNodeManager){
        this.dataNodeManager = dataNodeManager;
        this.directory = new FSDirectory();
        this.editLog = new FSEditLog(this);
        recoveryNamespace();
    }

    /**
     * @方法名: mkDir
     * @描述: 创建目录
     * @param path  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public boolean mkDir(String path){
        this.directory.mkDir(path);
        this.editLog.logEdit(EditLogFactory.mkdir(path));
        return true;
    }

    /**
     * 方法名: createFile
     * 描述:   创建文件
     * @param filePath
     * @return boolean
     * 作者: fansy
     * 日期: 2020/3/29 19:57
     */
    public boolean createFile(String filePath){
        if(!this.directory.createFile(filePath)){
            return false;
        }
        this.editLog.logEdit(EditLogFactory.create(filePath));
        return true;
    }

    /**  
     * @方法名: flush   
     * @描述:  将内存中缓存的edit log强制刷入磁盘
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public void flush(){
        this.editLog.flush();
    }

    public FSEditLog getEditLog() {
        return editLog;
    }

    public long getCheckpointTxid() {
        return checkpointTxid;
    }

    public void setCheckpointTxid(long checkpointTxid) {
        System.out.println("接收到的checkpoint txid:" + checkpointTxid);
        this.checkpointTxid = checkpointTxid;
    }

    /**  
     * 方法名: saveCheckpointTxid
     * 描述:   将checkpoint txid 保存到磁盘中
     * @param   
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 18:11 
     */  
    public void saveCheckpointTxid(){
        String path = ConfigConstant.NAME_NODE_EDIT_LOG_PATH+ConfigConstant.CHECKPOINT_META;

        RandomAccessFile raf = null;
        FileOutputStream out = null;
        FileChannel channel = null;
        try{
            File file = new File(path);
            if(file.exists()) {
                file.delete();
            }

            raf = new RandomAccessFile(path,"rw");
            out = new FileOutputStream(raf.getFD());
            channel = out.getChannel();

            ByteBuffer buffer = ByteBuffer.wrap(String.valueOf(checkpointTxid).getBytes());
            channel.write(buffer);
            channel.force(false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                FileUtil.closeOutputFile(raf,out,channel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**  
     * 方法名: recoveryNamespace
     * 描述:  恢复元数据
     * @param   
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 19:14 
     */  
    private void recoveryNamespace(){
        try {
            loadFsImage();
            loadCheckpointTxid();
            loadEditLog();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 方法名: loadFsImage
     * 描述: 加载FsImage文件  恢复文件目录树
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/3/28 19:16
     */
    private void loadFsImage() throws Exception {
        FileInputStream in = null;
        FileChannel channel = null;
        try{
            String fsImagePath = ConfigConstant.NAME_NODE_FS_IMAGE_PATH + "fsimage" + ConfigConstant.FS_IMAGE_SUFFIX;

            File file = new File(fsImagePath);
            if(!file.exists()){
                System.out.println("找不到fsImage文件，不需要进行恢复...");
                return;
            }

            in = new FileInputStream(fsImagePath);
            channel = in.getChannel();

            //这里后续需要修改   将数值设为上报过来的fsImage大小
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            int length = channel.read(buffer);
            buffer.flip();
            String fsImageJson = new String(buffer.array(),0,length);
            System.out.println("恢复fsimage文件中的数据：" + file.getName());

            FSDirectory.INode root = JSONObject.parseObject(fsImageJson, new TypeReference<FSDirectory.INode>(){});
            this.directory.setRoot(root);
        }finally {
            FileUtil.closeInputFile(in,channel);
        }
    }

    /**  
     * 方法名: loadCheckpointTxid
     * 描述:  恢复checkpoint txid
     * @param   
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 19:43 
     */  
    private void loadCheckpointTxid() throws Exception{
        FileInputStream in = null;
        FileChannel channel = null;
        try{
            String checkPonitPath = ConfigConstant.NAME_NODE_EDIT_LOG_PATH+ConfigConstant.CHECKPOINT_META;
            File file = new File(checkPonitPath);
            if(!file.exists()){
                System.out.println("checkpoint meta文件不存在，不需要恢复.");
                return;
            }

            in = new FileInputStream(checkPonitPath);
            channel = in.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int length = channel.read(buffer);
            buffer.flip();
            long checkpointTxId = Long.valueOf(new String(buffer.array(),0,length));
            this.checkpointTxid = checkpointTxId;
        }finally {
            FileUtil.closeInputFile(in,channel);
        }
     }

     /**
     * 方法名: loadEditLog
     * 描述:   加载editlog文件到namespace中
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/3/28 20:28
     */
    private void loadEditLog() throws Exception {

        File dir = new File(ConfigConstant.NAME_NODE_EDIT_LOG_PATH);
        List<File> files = new ArrayList<>();
        if(!dir.exists()){
            System.out.println("当前没有任何editLog文件   不需要恢复...");
            return;
        }
        for(File file : dir.listFiles()){
            if(file.getName().endsWith(ConfigConstant.NAME_NODE_EDIT_LOG_SUFFIX)){
                files.add(file);
            }
        }

        //对日志文件进行排序  元数据恢复必须严格按照操作顺序来
        files.sort((x,y)->{
            long xStart = Long.valueOf(x.getName().split(SPLITOR.TX_ID_START_END)[0]);
            long yStart = Long.valueOf(y.getName().split(SPLITOR.TX_ID_START_END)[0]);
            return (int)(xStart - yStart);
        });

        List<String> flushedTxids = this.editLog.getFlushedTxid();
        for(File file : files){
            long endTxid = Long.valueOf(file.getName().split(SPLITOR.TX_ID_START_END)[1].split(SPLITOR.FILE_NAME)[0]);
            //如果当前日志不在checkpoint 快照中  需要读取恢复
            if(endTxid > checkpointTxid){
                System.out.println("读取editLog恢复元数据："+file.getName());
                List<String> editLogs = Files.readAllLines(file.toPath());
                for(String editLogJson : editLogs){
                    JSONObject editLog = JSONObject.parseObject(editLogJson);

                    long txid = editLog.getLong("txid");

                    if(txid > checkpointTxid){
                        editLog2Namespace(editLog);
                    }
                }
                flushedTxids.add(file.getName().split(SPLITOR.FILE_NAME)[0]);
            }
        }
    }

    /**
     * 方法名: editLog2Namespace
     * 描述:   将editlog 还原到namespace中
     * @param editLog
     * @return void
     * 作者: fansy
     * 日期: 2020/3/28 20:28
     */
    private void editLog2Namespace(JSONObject editLog){
        String op = editLog.getString("OP");
        switch (op){
            case EditLogOp.MK_DIR:
                this.directory.mkDir(editLog.getString("PATH"));
                break;
            case EditLogOp.CREATE:
                this.directory.createFile(editLog.getString("PATH"));
                break;
            default:
                break;
        }
    }

    /**
     * 方法名: addReceivedReplica
     * 描述:   给指定的文件增加一个成功接收的副本
     * @param ip
     * @param hostname
     * @param fileName
     * @param fileLength
     * @return void
     * 作者: fansy
     * 日期: 2020/4/4 13:02
     */
    public void addReceivedReplica(String ip,String hostname,String fileName,long fileLength){
        List<DataNodeInfo> dataNodes = this.replicasByFileName.get(fileName);
        if(dataNodes == null){
            dataNodes = new ArrayList<>();
            this.replicasByFileName.put(fileName,dataNodes);
        }
        DataNodeInfo dataNode = this.dataNodeManager.getDataNode(ip,hostname);

        //如果文件副本数量超标了   直接删除该DataNode上的这个文件
        if(dataNodes.size() == ConfigConstant.DATA_STORE_REPLICA){
            RemoveTask removeTask = new RemoveTask(fileName,dataNode);
            dataNode.addRemoveTask(removeTask);
            return;
        }

        //文件接收完毕再增加 dataNode已经存储的文件大小比较合理
        dataNode.addStoredDataSize(fileLength);
        dataNodes.add(dataNode);

        String key = ip + SPLITOR.DATA_NODE_IP_HOST +hostname;
        List<String> files = this.filesByDataNode.get(key);
        if(files == null){
            files = new ArrayList<>();
            this.filesByDataNode.put(key,files);
        }
        files.add(fileName + SPLITOR.FILE_NAME_LENGTH + fileLength);
    }

    /**
     * 方法名: getDataNodeForFile
     * 描述:    获取文件所在dataNode
     *        这里随机返回一个dataNode  尽量让请求均匀分布到各个dataNode上去
     * @param fileName
     * @return com.quick.dfs.namenode.server.DataNodeInfo
     * 作者: fansy
     * 日期: 2020/4/6 13:19
     */
    public DataNodeInfo getDataNodeForFile(String fileName){
        DataNodeInfo dataNodeInfo = null;
        List<DataNodeInfo> dataNodeInfos = replicasByFileName.get(fileName);
        if(dataNodeInfos != null){
            int index = new Random().nextInt(dataNodeInfos.size());
            dataNodeInfo = dataNodeInfos.get(index);
        }
        return dataNodeInfo;
    }

    /**
     * @方法名: getFilesByDataNode
     * @描述:   获取指定dataNode上存储的文件信息
     * @param ip
     * @param hostname
     * @return java.util.List<java.lang.String>
     * @作者: fansy
     * @日期: 2020/4/7 14:23
    */
    public List<String> getFilesByDataNode(String ip,String hostname){
        String key = ip + SPLITOR.DATA_NODE_IP_HOST +hostname;
        return filesByDataNode.get(key);
    }

    /**  
     * @方法名: getReplicaSource   
     * @描述:   获取需要复制的文件源数据节点
     * @param fileName
     * @param deadDataNode  
     * @return com.quick.dfs.namenode.server.DataNodeInfo  
     * @作者: fansy
     * @日期: 2020/4/7 15:41 
    */  
    public DataNodeInfo getReplicaSource(String fileName,DataNodeInfo deadDataNode){
        List<DataNodeInfo> dataNodeInfos = replicasByFileName.get(fileName);
        if(dataNodeInfos != null){
            for(DataNodeInfo dataNode : dataNodeInfos){
                if(!dataNode.equals(deadDataNode)){
                    return dataNode;
                }
            }
        }
        return null;
    }

    /**
     * @方法名: removeDeadDataNode
     * @描述:   移除死掉的DataNode 相关信息
     * @param deadDataNode
     * @return void
     * @作者: fansy
     * @日期: 2020/4/7 15:50
    */
    public void removeDeadDataNode(DataNodeInfo deadDataNode){
        String key = deadDataNode.getIp() + SPLITOR.DATA_NODE_IP_HOST + deadDataNode.getHostName();
        List<String> files = filesByDataNode.get(key);
        for(String file : files){
            List<DataNodeInfo> dataNodes = replicasByFileName.get(file);
            dataNodes.remove(deadDataNode);
        }
        filesByDataNode.remove(key);
    }
    
}
