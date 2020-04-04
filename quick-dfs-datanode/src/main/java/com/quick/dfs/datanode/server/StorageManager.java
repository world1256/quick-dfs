package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.ConfigConstant;

import java.io.File;

/**
 * @项目名称: quick-dfs
 * @描述: 磁盘存储管理组件
 * @作者: fansy
 * @日期: 2020/04/04 14:07
 **/
public class StorageManager {
    /**
     * 方法名: getStorageInfo
     * 描述:   获取dataNode下存储的所有文件信息
     * @param
     * @return com.quick.dfs.datanode.server.StorageInfo
     * 作者: fansy
     * 日期: 2020/4/4 13:45
     */
    public StorageInfo getStorageInfo(){
        File dataDir = new File(ConfigConstant.DATA_NODE_DATA_PATH);
        File[] children = dataDir.listFiles();
        if(children == null || children.length == 0){
            return null;
        }
        StorageInfo storageInfo = new StorageInfo();
        for(File child :children){
            scanFiles(child,storageInfo);
        }
        return storageInfo;
    }

    /**
     * 方法名: scanFiles
     * 描述:   递归扫描所有文件信息
     * @param dir
     * @param storageInfo
     * @return void
     * 作者: fansy
     * 日期: 2020/4/4 13:44
     */
    private void scanFiles(File dir,StorageInfo storageInfo){
        if(dir.isFile()){
            String path = dir.getPath();
            String fileName = path.substring(ConfigConstant.DATA_NODE_DATA_PATH.length()-2);
            storageInfo.addFileName(fileName);
            storageInfo.addStoredDataSize(dir.length());
            return;
        }

        File[] children = dir.listFiles();
        if(children == null || children.length == 0){
            return;
        }
        for(File child :children){
            scanFiles(child,storageInfo);
        }
    }
}
