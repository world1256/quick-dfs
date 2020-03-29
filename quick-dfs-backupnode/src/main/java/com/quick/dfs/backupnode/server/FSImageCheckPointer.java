package com.quick.dfs.backupnode.server;

import com.quick.dfs.util.ConfigConstant;
import com.quick.dfs.util.FileUtil;
import com.quick.dfs.util.StringUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @项目名称: quick-dfs
 * @描述: fsimage文件的checkpoint组件
 * @作者: fansy
 * @日期: 2020/3/25 15:47
 **/
public class FSImageCheckPointer extends Thread{

    /**
     * 最后保存的fsImage
     */
    private String  lastFSImageFilePath;

    private BackupNode backupNode;

    private FSNameSystem nameSystem;

    private NameNodeRpcClient namenode;

    private long checkpointTime = System.currentTimeMillis();

    public FSImageCheckPointer(BackupNode backupNode,FSNameSystem nameSystem,NameNodeRpcClient namenode){
        this.backupNode = backupNode;
        this.nameSystem = nameSystem;
        this.namenode = namenode;
    }

    @Override
    public void run() {
        System.out.println("fsimage  定时checkpoint 组件启动...");
        while(this.backupNode.isRunning()){
            try{
                if(!this.nameSystem.isFinishedRecovery()){
                    System.out.println("元数据恢复还未完成，暂时不进行checkpoint");
                    Thread.sleep(1000);
                    continue;
                }

                if(StringUtil.isEmpty(lastFSImageFilePath)){
                    lastFSImageFilePath = this.nameSystem.getCheckpointFile();
                }

                if(System.currentTimeMillis() - checkpointTime > ConfigConstant.CHECKPOINT_INTERVAL){
                    if(!namenode.isNamenodeRunning()) {
                        System.out.println("namenode当前无法访问，不执行checkpoint......");
                        continue;
                    }

                    System.out.println("开始执行checkpoint操作...");
                    doCheckpoint();
                    System.out.println("checkpoint操作成功...");
                }
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**  
     * @方法名: doCheckpoint
     * @描述:   执行checkpoint操作
     * @param
     * @return void
     * @作者: fansy
     * @日期: 2020/3/25 16:56 
    */  
    private void doCheckpoint() throws Exception{
        FSImage fsImage = this.nameSystem.getFsImage();
        String filePath = fsImage2Disk(fsImage);
        //删除老的元数据快照
        removeLastFsImage();
        //将本次写入的元数据快照置为老的元数据快照   下次删除这个文件
        lastFSImageFilePath = filePath;
        //上报元数据到 namenode
        uploadFsImage(fsImage);
        //上报最新checkpoint txid
        updateCheckpointTxid(fsImage);

        checkpointInfo2Disk(fsImage);
    }

    /**  
     * 方法名: fsImage2Disk
     * 描述:   fsIamge写入磁盘文件 返回文件路径
     * @param fsImage  
     * @return java.lang.String  
     * 作者: fansy 
     * 日期: 2020/3/25 22:57 
     */  
    private String fsImage2Disk(FSImage fsImage) throws IOException{
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsImageJosn().getBytes());
        String fsImageFilePath = ConfigConstant.FS_IMAGE_PATH
                + ConfigConstant.FS_IMAGE_PREFIX
                + fsImage.getTxid()
                +ConfigConstant.FS_IMAGE_SUFFIX;
        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel channel = null;
        try{
            file = new RandomAccessFile(fsImageFilePath,"rw");
            out = new FileOutputStream(file.getFD());
            channel = out.getChannel();

            channel.write(buffer);
            //强制把数据输入磁盘
            channel.force(false);
        }finally {
            FileUtil.closeOutputFile(file,out,channel);
        }
        return fsImageFilePath;
    }

    /**  
     * @方法名: removeLastFsImage
     * @描述:   删除老的元数据快照
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/25 16:58 
    */  
    private void removeLastFsImage(){
        if(StringUtil.isNotEmpty(lastFSImageFilePath)){
            File file = new File(lastFSImageFilePath);
            if(file.exists()){
                file.delete();
            }
        }
    }

    /**
     * 方法名: uploadFsImage
     * 描述:  上报fsImage 到 namenode
     * @param fsImage
     * @return void
     * 作者: fansy
     * 日期: 2020/3/25 22:55
     */
    private void uploadFsImage(FSImage fsImage){
        new FSImageUploader(fsImage).start();
    }

    /**  
     * 方法名: updateCheckpointTxid
     * 描述:   上报 checkpont txid
     * @param fsImage  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/28 16:50 
     */  
    private void updateCheckpointTxid(FSImage fsImage){
        this.namenode.updateCheckpointTxid(fsImage.getTxid());
    }

    /**
     * 方法名: checkpointInfo2Disk
     * 描述:   checkpoint相关信息写入磁盘
     * @param fsImage
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 14:45
     */
    private void checkpointInfo2Disk(FSImage fsImage) throws Exception{
        String path = ConfigConstant.FS_IMAGE_PATH+ConfigConstant.CHECKPOINT_META;
        File file = new File(path);
        if(file.exists()){
            file.delete();
        }

        RandomAccessFile raf = null;
        FileOutputStream out = null;
        FileChannel channel = null;
        try{
            raf = new RandomAccessFile(path,"rw");
            out = new FileOutputStream(raf.getFD());
            channel = out.getChannel();

            this.checkpointTime = System.currentTimeMillis();
            String checkpointInfo = this.checkpointTime + "_"
                    + fsImage.getTxid()+ "_"
                    + lastFSImageFilePath;
            ByteBuffer buffer = ByteBuffer.wrap(checkpointInfo.getBytes());
            channel.write(buffer);
            channel.force(false);
        }finally {
            FileUtil.closeOutputFile(raf,out,channel);
        }
    }
}
