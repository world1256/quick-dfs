package com.quick.dfs.backupnode.server;

import com.quick.dfs.thread.Daemon;
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
public class FSImageCheckPointer extends Daemon {

    /**
     * checkpoint操作的时间间隔
     */
    private static final Long CHECKPOINT_INTERVAL = 60 * 60 * 1000L;

    /**
     * editLog 文件存放路径
     */
    private static final String FS_IMAGE_PATH = "/home/quick-dfs/fsimage/";

    /**
     * fsimage前缀
     */
    private static final String FS_IMAGE_PREFIX = "fsiamge-";

    /**
     * fsimage后缀
     */
    private static final String FS_IMAGE_SUFFIX = ".meta";

    /**
     * 最后保存的fsImage
     */
    private String  lastFSImageFilePath;

    private BackupNode backupNode;

    private FSNameSystem nameSystem;

    public FSImageCheckPointer(BackupNode backupNode,FSNameSystem nameSystem){
        this.backupNode = backupNode;
        this.nameSystem = nameSystem;
    }

    @Override
    public void run() {
        System.out.println("fsimage  定时checkpoint 组件启动...");
        while(this.backupNode.isRunning()){
            try{
                Thread.sleep(CHECKPOINT_INTERVAL);

                //将最新的元数据快照写入磁盘
                String filePath = doCheckpoint();
                //删除老的元数据快照
                removeLastFsImage();
                //将本次写入的元数据快照置为老的元数据快照   下次删除这个文件
                lastFSImageFilePath = filePath;

            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**  
     * @方法名: doCheckpoint
     * @描述:   保存元数据快照  返回文件路径
     * @param   
     * @return java.lang.String  
     * @作者: fansy
     * @日期: 2020/3/25 16:56 
    */  
    private String  doCheckpoint() throws IOException{
        FSImage fsImage = this.nameSystem.getFsImage();
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.getFsImageJosn().getBytes());

        String fsImageFilePath = FS_IMAGE_PATH + FS_IMAGE_PREFIX + fsImage.getTxid() +FS_IMAGE_SUFFIX;

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
            FileUtil.closeFile(file,out,channel);
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
}
