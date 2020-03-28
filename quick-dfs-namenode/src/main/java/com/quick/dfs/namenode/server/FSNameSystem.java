package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.util.ConfigConstant;
import com.quick.dfs.util.EditLogOp;
import com.quick.dfs.util.FileUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

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

    public FSNameSystem(){
        this.directory = new FSDirectory();
        recoveryNamespace();
        this.editLog = new FSEditLog(this);
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
        this.editLog.logEdit("{'OP':'"+ EditLogOp.MK_DIR +"','PATH':'" + path + "'}");
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
        String path = ConfigConstant.NAME_NODE_EDIT_LOG_PATH+ConfigConstant.NAME_NODE_CHECKPOINT_META;

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
                FileUtil.closeFile(raf,out,channel);
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
            String fsImagePath = ConfigConstant.FS_IMAGE_PATH + "fsimage" + ConfigConstant.FS_IMAGE_SUFFIX;
            in = new FileInputStream(fsImagePath);
            channel = in.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            channel.read(buffer);
            buffer.flip();
            String fsImageJson = new String(buffer.array());

            FSDirectory.INode root = JSONObject.parseObject(fsImageJson, FSDirectory.INode.class);
            this.directory.setRoot(root);
        }finally {
            if(channel != null){
                channel.close();
            }
            if(in != null){
                in.close();
            }
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
            String checkPonitPath = ConfigConstant.NAME_NODE_EDIT_LOG_PATH+ConfigConstant.NAME_NODE_CHECKPOINT_META;
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
            if(channel != null){
                channel.close();
            }
            if(in != null){
                in.close();
            }
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
        for(File file : dir.listFiles()){
            if(file.getName().endsWith(ConfigConstant.NAME_NODE_EDIT_LOG_SUFFIX)){
                files.add(file);
            }
        }

        //对日志文件进行排序  元数据恢复必须严格按照操作顺序来
        files.sort((x,y)->{
            long xStart = Long.valueOf(x.getName().split("-")[0]);
            long yStart = Long.valueOf(y.getName().split("-")[0]);
            return (int)(xStart - yStart);
        });

        for(File file : files){
            long endTxid = Long.valueOf(file.getName().split("-")[1].split(".")[0]);
            //如果当前日志不在checkpoint 快照中  需要读取恢复
            if(endTxid > checkpointTxid){
                List<String> editLogs = Files.readAllLines(file.toPath());
                for(String editLogJson : editLogs){
                    JSONObject editLog = JSONObject.parseObject(editLogJson);

                    long txid = editLog.getLong("txid");

                    if(txid > checkpointTxid){
                        editLog2Namespace(editLog);
                    }
                }
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
            default:
                break;
        }
    }

}
