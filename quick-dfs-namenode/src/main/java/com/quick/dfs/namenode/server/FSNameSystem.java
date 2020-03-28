package com.quick.dfs.namenode.server;

import com.quick.dfs.util.ConfigConstant;
import com.quick.dfs.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        this.editLog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");
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
}
