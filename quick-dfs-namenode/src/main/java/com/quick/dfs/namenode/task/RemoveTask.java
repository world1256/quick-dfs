package com.quick.dfs.namenode.task;

import com.quick.dfs.namenode.server.DataNodeInfo;

/**
 * @项目名称: quick-dfs
 * @描述: 文件副本过多  删除任务
 * @作者: fansy
 * @日期: 2020/4/8 16:22
 **/
public class RemoveTask {
    private String fileName;

    private DataNodeInfo dataNodeInfo;

    public RemoveTask(String fileName,DataNodeInfo dataNodeInfo){
        this.fileName = fileName;
        this.dataNodeInfo = dataNodeInfo;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public DataNodeInfo getDataNodeInfo() {
        return dataNodeInfo;
    }

    public void setDataNodeInfo(DataNodeInfo dataNodeInfo) {
        this.dataNodeInfo = dataNodeInfo;
    }
}
