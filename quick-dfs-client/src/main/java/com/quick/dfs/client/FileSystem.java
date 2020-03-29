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
     * @return void
     * 作者: fansy
     * 日期: 2020/3/29 19:30
     */
    boolean upload(byte[] file,String fileName)throws Exception;
}
