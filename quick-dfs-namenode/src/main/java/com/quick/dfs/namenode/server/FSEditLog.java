package com.quick.dfs.namenode.server;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @项目名称: quick-dfs
 * @描述: edit log 写入磁盘组件
 * @作者: fansy
 * @日期: 2020/3/18 15:31
 **/
public class FSEditLog {

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
          long txId = txIdSeq.incrementAndGet();
          localTxId.set(txId);

          EditLog  editLog = new EditLog(txId,content);

          //将edit log 写入内存缓冲
          editLogBuffer.write(editLog);
        }
        syncLog();
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

                //已经有线程在等待下一批数据刷入磁盘
                if(isWaitSync){
                    return;
                }

                //有线程进入到这里进行等待
                isWaitSync = true;
                //如果此时还有数据在写入磁盘   那么该线程在这里等待
                if(isSyncRunning){
                    try{
                        wait(1000);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                isWaitSync = false;
            }

            //交换2块缓冲区域  准备写入数据
            editLogBuffer.setReady2Sync();
            syncMaxTxId = editLogBuffer.getSyncMaxTxId();
            isSyncRunning = true;
        }

        editLogBuffer.flush();

        //数据写入完成后将写入标识置为false   唤醒可能在阻塞等待的其他线程来写入数据
        synchronized (this){
            isSyncRunning = false;
            notifyAll();
        }
    }



    /**
     * 操作记录  一条edit log
     */
    private class EditLog{
        private long txId;
        private String content;
        public EditLog(long txId,String content){
            this.txId = txId;
            this.content = content;
        }
    }

    private class DoubleBuffer{

        /**
         * 当前正在执行添加动作的buffer
         */
        private LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();

        /**
         * 准备写入磁盘的buffer
         */
        private LinkedList<EditLog> readyBuffer = new LinkedList<EditLog>();

        /**  
         * @方法名: write
         * @描述:   将edit log写入缓存中
         * @param editLog  
         * @return void  
         * @作者: fansy
         * @日期: 2020/3/18 17:19 
        */  
        private void write(EditLog editLog){
            currentBuffer.add(editLog);
        }

        /***
         * @方法名: setReady2Sync
         * @描述:   交换当前操作的buffer和准备写入的buffer
         *          为数据写入磁盘做准备
         * @param
         * @return void
         * @作者: fansy
         * @日期: 2020/3/18 17:19
        */
        private void setReady2Sync(){
            LinkedList<EditLog> temp = currentBuffer;
            currentBuffer = readyBuffer;
            readyBuffer = temp;
        }
        
        /**  
         * @方法名: getSyncMaxTxId
         * @描述:   获取正在写入磁盘的最大txid
         * @param   
         * @return java.lang.Long  
         * @作者: fansy
         * @日期: 2020/3/18 17:22 
        */  
        public Long getSyncMaxTxId(){
            return readyBuffer.getLast().txId;
        }

        /**  
         * @方法名: flush
         * @描述:   将readyBuffer中的数据写入磁盘
         * @param   
         * @return void  
         * @作者: fansy
         * @日期: 2020/3/18 17:23 
        */  
        public void flush(){
            //TODO 数据写入磁盘

            readyBuffer.clear();
        }
    }

}
