package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ClientRequestType;
import com.quick.dfs.util.FileUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @项目名称: quick-dfs
 * @描述: 响应网络请求 中的文件读写事件
 * @作者: fansy
 * @日期: 2020/04/14 19:18
 **/
public class IOThread extends Thread{

    private NetworkRequestQueue requestQueue = NetworkRequestQueue.getInstance();

    private NameNodeRpcClient nameNode;

    public IOThread(NameNodeRpcClient nameNode){
        this.nameNode = nameNode;
    }

    @Override
    public void run() {
        while(true){
            try {
                NetworkRequest request = requestQueue.poll();
                if(request == null){
                    Thread.sleep(100);
                }

                Integer requestType = request.getRequestType();
                if(requestType == ClientRequestType.SEND_FILE){
                    writeFile2Disk(request);
                }else if(requestType == ClientRequestType.READ_FILE){
                    readFileFromDisk(request);
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    /**  
     * 方法名: readFileFromDisk
     * 描述:   从磁盘中读取文件
     * @param request  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/4/14 20:39 
     */  
    private void readFileFromDisk(NetworkRequest request) throws Exception{
        FileInputStream inputStream = null;
        FileChannel fileChannel = null;
        try{
            String fileName = request.getAbsoluteFileName();
            long fileLength = request.getFileLength();

            inputStream = new FileInputStream(fileName);
            fileChannel = inputStream.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(8+(int)fileLength);
            buffer.putLong(fileLength);

            fileChannel.read(buffer);
            buffer.rewind();

            NetworkResponse response = new NetworkResponse();
            response.setBuffer(buffer);
            response.setClient(request.getClient());
            NetworkResponseQueue.getInstance().offer(request.getProcessorId(),response);
        }finally {
            FileUtil.closeInputFile(inputStream,fileChannel);
        }
    }

    /**
     * 方法名: writeFile2Disk
     * 描述:   文件写入磁盘
     * @param request
     * @return void
     * 作者: fansy
     * 日期: 2020/4/14 20:40
     */
    private void writeFile2Disk(NetworkRequest request) throws Exception {
        FileOutputStream outputStream = null;
        FileChannel fileChannel = null;
        try{
             String fileName = request.getAbsoluteFileName();
             ByteBuffer buffer = request.getFileBuffer();

             outputStream = new FileOutputStream(fileName);
             fileChannel = outputStream.getChannel();

             fileChannel.write(buffer);
             System.out.println("文件写入磁盘成功，fileName:"+fileName);

             nameNode.informReplicaReceived(request.getRelativeName(),request.getFileLength());
             System.out.println("向nameNode上报接收到的文件信息...");
        }finally {
            FileUtil.closeOutputFile(null,outputStream,fileChannel);
        }

    }
}
