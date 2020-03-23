package com.quick.dfs.namenode.server;

import java.util.LinkedList;

/**
 * @项目名称: quick-dfs
 * @描述: editlog  内存缓冲
 *       采用双缓冲机制提升io吞吐量
 * @作者: fansy
 * @日期: 2020/3/23 15:29
 **/
public class DoubleBuffer {

    /**
     * 单块editlog 内存缓冲的大小   默认512字节
     */
    private static final Long EDIT_LOG_BUFFER_CAPACITY = 512 * 1024L;

    /**
     * 当前正在执行添加动作的buffer
     */
    private EditLogBuffer currentBuffer = new EditLogBuffer();

    /**
     * 准备写入磁盘的buffer
     */
    private EditLogBuffer readyBuffer = new EditLogBuffer();

    /**
     * @方法名: write
     * @描述:   将edit log写入缓存中
     * @param editLog
     * @return void
     * @作者: fansy
     * @日期: 2020/3/18 17:19
     */
    public void write(EditLog editLog){
        currentBuffer.write(editLog);
    }

    /**  
     * @方法名: shouldSync2Disk
     * @描述:   是否应该将内存中的editlog刷入磁盘
     * @param   
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/23 15:34 
    */  
    public boolean shouldSync2Disk(){
        if(currentBuffer.size()>EDIT_LOG_BUFFER_CAPACITY){
            return true;
        }
        return false;
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
    public void setReady2Sync(){
        EditLogBuffer temp = currentBuffer;
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
//        return readyBuffer.getLast().getTxId();
        return 0l;
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

    class EditLogBuffer{

        public void flush(){

        }

        public void write(EditLog editLog){

        }

        public void clear(){

        }

        public long size(){
            return 0l;
        }
    }

}
