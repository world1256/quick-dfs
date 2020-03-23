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
            //进来先判断是否有线程在等待刷数据到磁盘
            waitSync();

            long txId = txIdSeq.incrementAndGet();
            localTxId.set(txId);

            EditLog  editLog = new EditLog(txId,content);

            //将edit log 写入内存缓冲
            editLogBuffer.write(editLog);

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

        editLogBuffer.flush();

        //数据写入完成后将写入标识置为false   唤醒可能在阻塞等待的其他线程来写入数据
        synchronized (this){
            isSyncRunning = false;
            notifyAll();
        }
    }

}
