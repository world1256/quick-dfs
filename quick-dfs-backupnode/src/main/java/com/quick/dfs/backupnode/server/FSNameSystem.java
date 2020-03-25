package com.quick.dfs.backupnode.server;

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
//    private FSEditLog editLog;

    public FSNameSystem(){
        this.directory = new FSDirectory();
//        this.editLog = new FSEditLog();
    }



    /**
     * @方法名: mkDir
     * @描述: 创建目录
     * @param txid
     * @param path  
     * @return boolean  
     * @作者: fansy
     * @日期: 2020/3/24 9:03 
    */  
    public boolean mkDir(long txid,String path){
        this.directory.mkDir(txid,path);
        return true;
    }

    /**  
     * @方法名: getFsImage
     * @描述:   获取当前最新的内存目录树
     * @param   
     * @return com.quick.dfs.backupnode.server.FSImage  
     * @作者: fansy
     * @日期: 2020/3/25 16:33 
    */  
    public FSImage getFsImage(){
        return this.directory.getFsImage();
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
//        this.editLog.flush();
    }
}
