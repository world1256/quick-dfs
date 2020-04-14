package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.util.FileUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @项目名称: quick-dfs
 * @描述: 网络请求
 * @作者: fansy
 * @日期: 2020/4/14 15:21
 **/
public class NetworkRequest {

    /**
     * 本次网络请求对应的连接
     */
    private SelectionKey key;

    /**
     * 本次网络请求对应的连接
     */
    private SocketChannel channel;

    /**
     * processor标识
     */
    private Integer processorId;

    /**
     * 缓存没处理完的请求信息
     */
    private CachedRequest cachedRequest = new CachedRequest();

    /**
     * 缓存没读取完的请求类型
     */
    private ByteBuffer cachedRequestTypeBuffer;

    /**
     * 缓存没读取完的文件名长度
     */
    private ByteBuffer cachedFileNameLengthBuffer;

    /**
     * 缓存没读取完的文件名
     */
    private ByteBuffer cachedFileNameBuffer;

    /**
     * 缓存没读取完的文件长度
     */
    private ByteBuffer cachedFileLengthBuffer;

    /**
     * 缓存没读取完的文件内容
     */
    private ByteBuffer cachedFileBuffer;

    public NetworkRequest(SelectionKey key,SocketChannel channel){
        this.key = key;
        this.channel = channel;
    }


    public void read(){
        try {
            Integer requestType = null;
            if(cachedRequest.requestType != null){
                requestType = cachedRequest.requestType;
            }else{
                requestType = getRequestType();
            }

            if(requestType == null){
                return;
            }

            if(requestType == ClientRequestType.SEND_FILE){
                handleSendFileRequest(channel);
            }else if(requestType == ClientRequestType.READ_FILE){
                handleReadFileRequest(channel);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**  
     * @方法名: hasCompletedRead
     * @描述:   是否完成了一个请求的读取
     * @param   
     * @return java.lang.Boolean  
     * @作者: fansy
     * @日期: 2020/4/14 15:36 
    */  
    public Boolean hasCompletedRead() {
        return cachedRequest.readCompleted;
    }


    /**
     * 方法名: getRequestType
     * 描述:   获取请求类型
     * @return long
     * 作者: fansy
     * 日期: 2020/4/6 14:54
     */
    public Integer getRequestType() throws IOException {
        Integer requestType = null;

        if(cachedRequest.requestType != null){
            return cachedRequest.requestType;
        }

        ByteBuffer requestTypeBuffer;
        if(cachedRequestTypeBuffer != null){
            requestTypeBuffer = cachedRequestTypeBuffer;
        }else{
            requestTypeBuffer = ByteBuffer.allocate(4);
        }

        channel.read(requestTypeBuffer);

        //请求类型读取完毕  从未读取完毕缓存中删除  将该请求加入未处理完成请求缓存中
        if(!requestTypeBuffer.hasRemaining()){
            requestTypeBuffer.rewind();
            requestType = requestTypeBuffer.getInt();
            cachedRequest.requestType = requestType;
        }else{
            cachedRequestTypeBuffer = requestTypeBuffer;
        }
        return requestType;
    }

    /**
     * @方法名: getFileName
     * @描述:   获取文件名  顺便创建文件目录
     * @param channel
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/4/2 15:25
     */
    private FileName getFileName(SocketChannel channel) throws Exception {
        FileName fileName = new FileName();
        if(cachedRequest.fileName != null){
            return cachedRequest.fileName;
        }

        String relativeFileName = getRelativeFileName(channel);
        if(relativeFileName == null){
            return null;
        }

        String absoluteFileName = FileUtil.getAbsoluteFileName(relativeFileName);

        //将文件名保存到未完成请求缓存中
        fileName.relativeFileName = relativeFileName;
        fileName.absoluteFileName = absoluteFileName;
        cachedRequest.fileName = fileName;

        return fileName;
    }

    /**
     * @方法名: getRelativeFileName
     * @描述:   从channel中读取文件名
     * @param channel
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2020/4/2 15:13
     */
    private String getRelativeFileName(SocketChannel channel) throws Exception {
        Integer fileNameLength = null;
        String fileName = null;

        ByteBuffer fileNameLengthBuffer;
        //首先读取文件名长度  如果文件名在未读取完成缓存中  那么不需要读取文件名长度
        if(cachedRequest.fileNameLength == null){
            if(cachedFileNameLengthBuffer != null){
                fileNameLengthBuffer = cachedFileNameLengthBuffer;
            }else{
                fileNameLengthBuffer = ByteBuffer.allocate(4);
            }
            channel.read(fileNameLengthBuffer);
            if(!fileNameLengthBuffer.hasRemaining()){
                fileNameLengthBuffer.rewind();
                fileNameLength = fileNameLengthBuffer.getInt();
                cachedRequest.fileNameLength = fileNameLength;
            }else{
                cachedFileNameLengthBuffer = fileNameLengthBuffer;
                return null;
            }
        }

        ByteBuffer fileNameBuffer;
        if(cachedFileNameBuffer != null){
            fileNameBuffer = cachedFileNameBuffer;
        }else{
            fileNameBuffer = ByteBuffer.allocate(fileNameLength);
        }
        channel.read(fileNameBuffer);
        if(!fileNameBuffer.hasRemaining()){
            fileNameBuffer.rewind();
            fileName = new String(fileNameBuffer.array());
        }else{
            cachedFileNameBuffer = fileNameBuffer;
        }
        return fileName;
    }

    /**
     * @方法名: getFileLength
     * @描述:   获取文件的长度
     * @return long
     * @作者: fansy
     * @日期: 2020/4/2 15:38
     */
    public Long getFileLength() throws Exception {
        Long fileLength = null;
        if(cachedRequest.fileLength != null){
            return cachedRequest.fileLength;
        }

        ByteBuffer fileLengthBuffer;
        if(cachedFileLengthBuffer != null){
            fileLengthBuffer = cachedFileLengthBuffer;
        }else{
            fileLengthBuffer = ByteBuffer.allocate(8);
        }

        channel.read(fileLengthBuffer);

        if(!fileLengthBuffer.hasRemaining()){
            fileLengthBuffer.rewind();
            fileLength = fileLengthBuffer.getLong();
            cachedRequest.fileLength = fileLength;
        }else{
            cachedFileLengthBuffer = fileLengthBuffer;
        }
        return fileLength;
    }

    /**
     * 方法名: handleSendFileRequest
     * 描述:   处理客户端的上传文件请求
     * @param channel
     * @return void
     * 作者: fansy
     * 日期: 2020/4/6 14:47
     */
    private void handleSendFileRequest(SocketChannel channel) throws Exception {
        String client = channel.getRemoteAddress().toString();
        FileName fileName = getFileName(channel);
        if(fileName == null){
            return;
        }
        Long fileLength = getFileLength();
        if(fileLength == null){
            return;
        }

        ByteBuffer fileBuffer = null;
        if(cachedFileBuffer != null){
            fileBuffer = cachedFileBuffer;
        }else{
            fileBuffer = ByteBuffer.allocate(fileLength.intValue());
        }

        channel.read(fileBuffer);

        if(!fileBuffer.hasRemaining()){
            fileBuffer.rewind();
            cachedRequest.fileBuffer = fileBuffer;
            cachedRequest.readCompleted = true;
            System.out.println("本次文件上传读取完毕..." + client);
        }else{
            cachedFileBuffer = fileBuffer;
            System.out.println("本次文件上传出现拆包问题，缓存起来，下次继续读取......." + client);
        }

    }

    /**
     * 方法名: handleReadFileRequest
     * 描述:   处理客户端的下载文件请求
     * @param channel
     * @return void
     * 作者: fansy
     * 日期: 2020/4/6 15:26
     */
    private void handleReadFileRequest(SocketChannel channel) throws Exception {
        FileName fileName = getFileName(channel);
        if(fileName == null){
            return;
        }
        cachedRequest.readCompleted = true;
    }


    /**
     * 文件名
     */
    class FileName{
        /**
         * 相对路径文件名
         */
        private String relativeFileName;

        /**
         * 绝对路径文件名
         */
        private String absoluteFileName;

    }

    /**
     * 处理的请求缓存信息
     */
    class CachedRequest{

        private Integer requestType;

        private FileName fileName;

        private Long fileLength;

        private Integer fileNameLength;

        private ByteBuffer fileBuffer;

        /**
         * 是否处理完成
         */
        private boolean readCompleted = false;
    }

    public String getAbsoluteFileName(){
        return cachedRequest.fileName.absoluteFileName;
    }

    public ByteBuffer getFileBuffer(){
        return cachedRequest.fileBuffer;
    }

    public String getRelativeName(){
        return cachedRequest.fileName.relativeFileName;
    }

    public Integer getProcessorId() {
        return processorId;
    }

    public void setProcessorId(Integer processorId) {
        this.processorId = processorId;
    }

    public String getClient(){
        String client = null;
        try {
            client = channel.getRemoteAddress().toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return client;
    }
}
