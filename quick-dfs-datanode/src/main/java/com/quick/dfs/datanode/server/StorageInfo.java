package com.quick.dfs.datanode.server;

import com.quick.dfs.constant.SPLITOR;

import java.util.List;

/**
 * @项目名称: quick-dfs
 * @描述: dataNode 存储文件信息
 * @作者: fansy
 * @日期: 2020/04/04 13:33
 **/
public class StorageInfo {

    private List<String> files;

    private long storedDataSize;

    public List<String> getFiles() {
        return files;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }

    public long getStoredDataSize() {
        return storedDataSize;
    }

    public void setStoredDataSize(long storedDataSize) {
        this.storedDataSize = storedDataSize;
    }

    public void addFile(String fileName,long fileSize){
        this.files.add(fileName + SPLITOR.FILE_NAME_LENGTH + fileSize);
        this.storedDataSize += fileSize;
    }

}
