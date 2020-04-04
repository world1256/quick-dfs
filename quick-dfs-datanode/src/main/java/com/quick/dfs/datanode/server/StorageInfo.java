package com.quick.dfs.datanode.server;

import java.util.List;

/**
 * @项目名称: quick-dfs
 * @描述: dataNode 存储文件信息
 * @作者: fansy
 * @日期: 2020/04/04 13:33
 **/
public class StorageInfo {

    private List<String> fileNames;

    private long storedDataSize;

    public List<String> getFileNames() {
        return fileNames;
    }

    public void setFileNames(List<String> fileNames) {
        this.fileNames = fileNames;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addFileName(String fileName){
        this.fileNames.add(fileName);
    }

    public void addStoredDataSize(long fileSize){
        this.storedDataSize += fileSize;
    }
}
