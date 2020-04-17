package com.quick.dfs.client;

/**
 * @项目名称: quick-dfs
 * @描述: 文件系统操作接口
 * @作者: fansy
 * @日期: 2020/3/23 14:21
 **/
public interface FileSystem {
    
    /**  
     * @方法名: mkDir
     * @描述:   创建目录
     * @param path  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/23 14:23 
    */  
    void mkDir(String path)throws Exception;
    
    /**  
     * @方法名: shutdown
     * @描述:   关闭namenode  停止服务
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 8:49 
    */  
    void shutdown()throws Exception;

    /**
     * 方法名: upload
     * 描述:  上传文件
     * @param file
     * @param fileName
     * @param callback
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 19:30
     */
    boolean upload(byte[] file,String fileName,NetworkResponseCallback callback)throws Exception;

    /**  
     * 方法名: download
     * 描述:   下载文件
     * @param fileName  
     * @return byte[]  
     * 作者: fansy 
     * 日期: 2020/4/6 13:34 
     */  
    byte[] download(String fileName)throws Exception;

    /**
     * 方法名: rebalance
     * 描述:   数据节点重平衡
     * @param
     * @return void
     * 作者: fansy
     * 日期: 2020/4/12 14:22
     */
    void rebalance()throws Exception;
}
