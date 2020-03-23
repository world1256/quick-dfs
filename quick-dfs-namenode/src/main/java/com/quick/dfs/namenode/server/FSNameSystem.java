package com.quick.dfs.namenode.server;

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
    private FSEditLog editLog;

    public FSNameSystem(){
        this.directory = new FSDirectory();
        this.editLog = new FSEditLog();
    }

    /**
     * 创建目录
     * @param path
     * @return
     */
    public boolean mkDir(String path){
        this.directory.mkDir(path);
        this.editLog.logEdit("创建目录:"+path);
        return true;
    }

}
