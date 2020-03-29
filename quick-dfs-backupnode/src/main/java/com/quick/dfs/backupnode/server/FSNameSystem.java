package com.quick.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.quick.dfs.util.ConfigConstant;
import com.quick.dfs.util.FileUtil;
import com.quick.dfs.util.StringUtil;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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

    private long checkpointTime;

    private long syncedTxid;

    private String checkpointFile;

    private volatile boolean finishedRecovery = false;

    public FSNameSystem(){
        this.directory = new FSDirectory();
        recoveryNameSpace();
        this.finishedRecovery = true;
    }


    /**
     * @方法名: mkDir
     * @描述: 创建目录
     * @param txid
     * @param path  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public boolean mkDir(long txid,String path){
        this.directory.mkDir(txid,path);
        return true;
    }

    /**  
     * @方法名: getFsImage
     * @描述:   获取当前最新的内存目录树
     * @param   
     * @return com.quick.dfs.backupnode.server.FSImage  
     * @作者: fansy
     * @日期: 2020/3/25 16:33 
    */  
    public FSImage getFsImage(){
        return this.directory.getFsImage();
    }

    /**
     * 方法名: getSyncedTxid
     * 描述:   获取当前同步到得最大的txid
     * @param
     * @return long
     * 作者: fansy
     * 日期: 2020/3/28 20:37
     */
    public long getSyncedTxid(){
        return this.directory.getFsImage().getTxid();
    }

    /**
     * 方法名: recoveryNameSpace
     * 描述:   恢复元数据信息
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 14:10
     */
    private void recoveryNameSpace(){
        try{
            loadCheckpointInfo();
            loadFsImage();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 方法名: loadCheckpointInfo
     * 描述:   加载checkpoint 信息
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 14:10
     */
    private void loadCheckpointInfo() throws Exception {
        String path = ConfigConstant.FS_IMAGE_PATH+ConfigConstant.CHECKPOINT_META;

        FileInputStream in = null;
        FileChannel channel = null;

        File file = new File(path);
        if(!file.exists()){
            System.out.println("checkpoint 文件不存在，不需要进行恢复...");
            return;
        }
        try{
            in = new FileInputStream(path);
            channel = in.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024);

            int length = channel.read(buffer);
            buffer.flip();

            String checkpointInfo = new String(buffer.array(),0,length);
            String[] arr = checkpointInfo.split("_");
            this.checkpointTime = Long.valueOf(arr[0]);
            this.syncedTxid = Long.valueOf(arr[1]);
            this.checkpointFile = arr[2];
            this.directory.setMaxTxid(syncedTxid);
        }finally {
            FileUtil.closeInputFile(in,channel);
        }
    }

    /**
     * 方法名: loadFsImage
     * 描述:  加载fsImage
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 14:11
     */
    private void loadFsImage()throws Exception{
        if(StringUtil.isEmpty(this.checkpointFile)){
            System.out.println("fsImage文件不存在，不需要进行恢复...");
            return;
        }
        File file = new File(this.checkpointFile);
        if(!file.exists()){
            System.out.println("fsImage文件不存在，不需要进行恢复...");
            return;
        }

        FileInputStream in = null;
        FileChannel channel = null;
        try{
            in = new FileInputStream(this.checkpointFile);
            channel = in.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

            int length = channel.read(buffer);
            buffer.flip();

            String fsImageJson = new String(buffer.array(),0,length);
            FSDirectory.INode root = JSONObject.parseObject(fsImageJson, new TypeReference<FSDirectory.INode>(){});
            this.directory.setRoot(root);
        }finally {
            FileUtil.closeInputFile(in,channel);
        }
    }

    public long getCheckpointTime() {
        return checkpointTime;
    }

    public void setCheckpointTime(long checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    public void setSyncedTxid(long syncedTxid) {
        this.syncedTxid = syncedTxid;
    }

    public String getCheckpointFile() {
        return checkpointFile;
    }

    public void setCheckpointFile(String checkpointFile) {
        this.checkpointFile = checkpointFile;
    }

    public boolean isFinishedRecovery() {
        return finishedRecovery;
    }

    public void setFinishedRecovery(boolean finishedRecovery) {
        this.finishedRecovery = finishedRecovery;
    }
}
