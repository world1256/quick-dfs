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


    public FSNameSystem(){
        this.directory = new FSDirectory();
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
     * 方法名: getSyncedTxid
     * 描述:   获取当前同步到得最大的txid
     * @param
     * @return long
     * 作者: fansy
     * 日期: 2020/3/28 20:37
     */
    public long getSyncedTxid(){
        return this.directory.getFsImage().getTxid();
    }
}
