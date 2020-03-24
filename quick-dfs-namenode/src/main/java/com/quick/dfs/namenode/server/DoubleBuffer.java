package com.quick.dfs.namenode.server;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * @项目名称: quick-dfs
 * @描述: editlog  内存缓冲
 *       采用双缓冲机制提升io吞吐量
 * @作者: fansy
 * @日期: 2020/3/23 15:29
 **/
public class DoubleBuffer {

    /**
     * 单块editlog 内存缓冲的大小   默认25kb
     */
    private static final Integer EDIT_LOG_BUFFER_CAPACITY = 25 * 1024;

    /**
     * editLog 文件存放路径
     */
    private static final String EDIT_LOG_PATH = "/home/quick-dfs/editlog/";

    /**
     * 当前正在执行添加动作的buffer
     */
    private EditLogBuffer currentBuffer = new EditLogBuffer();

    /**
     * 准备写入磁盘的buffer
     */
    private EditLogBuffer readyBuffer = new EditLogBuffer();

    /**
     * 上次写入磁盘的最大txId
     */
    private long lastSyncMaxTxId = 0L;

    /**
     * 已经落地到磁盘的txId 范围
     */
    private List<String> flushedTxIds = new ArrayList<>();

    /**
     * @方法名: write
     * @描述:   将edit log写入缓存中
     * @param editLog
     * @return void
     * @作者: fansy
     * @日期: 2020/3/18 17:19
     */
    public void write(EditLog editLog)throws IOException{
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
        return readyBuffer.maxTxId;
    }

    /**
     * @方法名: flush
     * @描述:   将readyBuffer中的数据写入磁盘
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/18 17:23
     */
    public void flush() throws IOException{
        //TODO 数据写入磁盘
        readyBuffer.flush();
        readyBuffer.clear();
    }

    /**  
     * @方法名: getFlushedTxIds
     * @描述:   获取已经刷入磁盘的edit log  txid范围数据
     * @param   
     * @return java.util.List<java.lang.String>  
     * @作者: fansy
     * @日期: 2020/3/24 11:02 
    */  
    public List<String> getFlushedTxIds() {
        return flushedTxIds;
    }

    /**  
     * @方法名: getBufferedEditLog
     * @描述:   获取当前正在写入的缓冲区数据
     * @param   
     * @return java.lang.String[]  
     * @作者: fansy
     * @日期: 2020/3/24 11:11 
    */  
    public String[] getBufferedEditLog(){
        String editLogRawData = new String(currentBuffer.getBufferData());
        return editLogRawData.split("\n");
    }


    class EditLogBuffer{

        /**
         * 针对内存缓冲区的字节数据输出流
         */
        ByteArrayOutputStream buffer;

        /**
         * 当前内存缓冲区中的最大txId
         */
        long maxTxId = 0L;

        public EditLogBuffer(){
            this.buffer = new ByteArrayOutputStream(EDIT_LOG_BUFFER_CAPACITY);
        }

        /**  
         * @方法名: write
         * @描述:   将editLog写入输出流中  这里使用\n来分割每条日志
         * @param editLog  
         * @return void  
         * @作者: fansy
         * @日期: 2020/3/23 16:32 
        */  
        public void write(EditLog editLog) throws IOException {
            this.maxTxId = editLog.getTxId();
            this.buffer.write(editLog.getContent().getBytes());
            this.buffer.write("\n".getBytes());
        }

        /**
         * @方法名: size
         * @描述:   返回当前缓冲区的大小
         * @param
         * @return long
         * @作者: fansy
         * @日期: 2020/3/23 16:35
        */
        public long size(){
            return this.buffer.size();
        }

        /**
         * @方法名: flush
         * @描述:   将缓冲区的数据刷入磁盘
         * @param
         * @return void
         * @作者: fansy
         * @日期: 2020/3/23 16:36
        */
        public void flush() throws IOException{
            byte[] data = this.buffer.toByteArray();
            ByteBuffer dataBuffer = ByteBuffer.wrap(data);

            //editlog输出文件   格式： 17789-24467.log
            String editLogPath = EDIT_LOG_PATH + lastSyncMaxTxId + "-" + maxTxId+".log";
            flushedTxIds.add(lastSyncMaxTxId + "-" + maxTxId);

            RandomAccessFile file = null;
            FileOutputStream out = null;
            FileChannel editLogFileChannel = null;
            try {
                file = new RandomAccessFile(editLogPath,"rw");
                out = new FileOutputStream(file.getFD());
                editLogFileChannel = out.getChannel();

                editLogFileChannel.write(dataBuffer);
                //强制刷新数据到磁盘   否则日志数据会缓存在os cache中
                //一旦宕机 这些os cache中的数据有可能丢失
                editLogFileChannel.force(false);
            }finally {
                if(editLogFileChannel != null){
                    editLogFileChannel.close();
                }
                if(out != null){
                    out.close();
                }
                if(file!=null){
                    file.close();
                }
            }
            lastSyncMaxTxId = maxTxId + 1;
        }

        /**  
         * @方法名: clear
         * @描述:   清理掉内存缓冲中的数据
         * @param   
         * @return void  
         * @作者: fansy
         * @日期: 2020/3/23 16:53 
        */  
        public void clear(){
            this.buffer.reset();
        }

        /**  
         * @方法名: getBufferData
         * @描述:   获取缓冲区的数据
         * @param   
         * @return byte[]  
         * @作者: fansy
         * @日期: 2020/3/24 11:10 
        */  
        public byte[] getBufferData(){
            return this.buffer.toByteArray();
        }
    }

}
