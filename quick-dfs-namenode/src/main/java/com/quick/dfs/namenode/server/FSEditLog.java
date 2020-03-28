package com.quick.dfs.namenode.server;

import com.quick.dfs.thread.Daemon;
import com.quick.dfs.util.ConfigConstant;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @项目名称: quick-dfs
 * @描述: edit log 写入磁盘组件
 * @作者: fansy
 * @日期: 2020/3/18 15:31
 **/
public class FSEditLog {

    /**
     * 元数据管理组件
     */
    private FSNameSystem nameSystem;

    /**
     *  当前递增到的txid序列号
     */
    private AtomicLong txIdSeq =new AtomicLong(0);

    /**
     * edit log 内存缓冲
     */
    private DoubleBuffer editLogBuffer = new DoubleBuffer();

    /**
     *  是否正在将数据写入磁盘
     */
    private volatile boolean isSyncRunning = false;

    /**
     * 是否有线程在等待刷新下一批数据到磁盘中
     */
    private volatile boolean isWaitSync = false;

    /**
     * 正在刷入磁盘的数据中的最大txId
     */
    private volatile long syncMaxTxId = 0l;

    /**
     * 每个线程本地保存的txid副本
     */
    private ThreadLocal<Long> localTxId = new ThreadLocal<Long>();

    public FSEditLog(FSNameSystem nameSystem){
        this.nameSystem = nameSystem;
        new EditLogCleaner().start();
    }

    /**
     * @方法名: logEdit
     * @描述:   记录edit  log日志
     * @param content
     * @return void
     * @作者: fansy
     * @日期: 2020/3/19 10:00
    */
    public void logEdit(String content){
        synchronized (this){
            //进来先判断是否有线程在等待刷数据到磁盘
            waitSync();

            long txId = txIdSeq.incrementAndGet();
            localTxId.set(txId);

            EditLog  editLog = new EditLog(txId,content);

            //将edit log 写入内存缓冲
            try{
                editLogBuffer.write(editLog);
            }catch (Exception e){
                e.printStackTrace();
            }

            //判断是否应该将内存缓冲中的editLog写入磁盘
            if(!editLogBuffer.shouldSync2Disk()){
                return;
            }

            //准备将数据刷入磁盘   暂停将editlog写入内存缓冲  等后面缓冲区交换完毕后再恢复
            isWaitSync = true;
        }
        syncLog();
    }


    /**  
     * @方法名: waitSync
     * @描述:   判断是否有线程在等待刷数据到磁盘
     *         如果有的话此时不应该继续将editlog写入缓存  因为2块buffer都已经写满了
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/23 15:21 
    */  
    private void waitSync(){
        try{
            while(isWaitSync){
                wait(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    /***  
     * @方法名: syncLog
     * @描述:   将缓冲中的数据写入磁盘
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/19 10:04 
    */  
    private void syncLog(){
        synchronized (this){
            //此时内存缓冲中的数据正在写入磁盘
            if(isSyncRunning){
                //如果当前线程中的txid小于正在输入磁盘缓冲中的最大txid
                //那么代表该txid的数据已经在刷入磁盘的缓冲当中  没有必要再次进行刷入操作
                long txId = localTxId.get();
                if(txId < syncMaxTxId){
                    return;
                }


                //如果此时还有数据在写入磁盘   那么该线程在这里等待
                try{
                    while(isSyncRunning){
                        wait(1000);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            //交换2块缓冲区域  准备写入数据
            editLogBuffer.setReady2Sync();
            syncMaxTxId = editLogBuffer.getSyncMaxTxId();

            //内存缓冲交换完毕，恢复editlog写入内存缓冲
            isWaitSync = false;
            notifyAll();

            isSyncRunning = true;
        }

        //内存缓冲中的数据刷入磁盘  这里耗时会稍长一点
        try{
            editLogBuffer.flush();
        }catch (IOException e){
            e.printStackTrace();
        }

        //数据写入完成后将写入标识置为false   唤醒可能在阻塞等待的其他线程来写入数据
        synchronized (this){
            isSyncRunning = false;
            notifyAll();
        }
    }

    /**  
     * @方法名: flush
     * @描述: 强制把内存中的数据刷入磁盘
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 9:02 
    */  
    public void flush(){
        try {
            editLogBuffer.setReady2Sync();
            editLogBuffer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**  
     * @方法名: getFlushedTxid
     * @描述:   获取已经刷入磁盘的editlog  txid 范围数据
     * @param   
     * @return java.util.List<java.lang.String>  
     * @作者: fansy
     * @日期: 2020/3/24 11:18 
    */  
    public List<String> getFlushedTxid(){
        synchronized (this){
            return editLogBuffer.getFlushedTxIds();
        }
    }

    /**
     * @方法名: getBufferedEditLog
     * @描述:   获取当前内存缓冲中的editLog 数据
     * @param
     * @return java.lang.String[]
     * @作者: fansy
     * @日期: 2020/3/24 11:19
    */
    public String[] getBufferedEditLog(){
        synchronized (this){
            return editLogBuffer.getBufferedEditLog();
        }
    }

    /**
     * editLog 定时清理线程
     */
    class EditLogCleaner extends Daemon {
        @Override
        public void run() {
            while(true){
                try{
                    Thread.sleep(ConfigConstant.NAME_NODE_EDIT_LOG_CLEAN_INTERVAL);
                    List<String> flushedTxids = getFlushedTxid();
                    if(flushedTxids != null && flushedTxids.size() > 0){
                        long checkpointTxid = nameSystem.getCheckpointTxid();
                        for(String flushedTxid : flushedTxids){
                            long endTxid = Long.valueOf(flushedTxid.split("-")[1]);
                            if(checkpointTxid >= endTxid){
                                String path = ConfigConstant.FS_IMAGE_PATH + flushedTxid + ConfigConstant.NAME_NODE_EDIT_LOG_SUFFIX;
                                File file = new File(path);
                                if(file.exists()){
                                    file.delete();
                                    System.out.println("删除已经checkpoint过的editlog文件：" + path);
                                }
                            }
                        }
                    }
                 }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


}
