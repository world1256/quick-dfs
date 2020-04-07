package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: 文件副本复制任务
 * @作者: fansy
 * @日期: 2020/4/7 14:10
 **/
public class ReplicateTask {

    private String fileName;

    private long fileLength;

    private DataNodeInfo sourceDataNode;

    private DataNodeInfo destDataNode;

    public ReplicateTask(String fileName,long fileLength,DataNodeInfo sourceDataNode,DataNodeInfo destDataNode){
        this.fileName = fileName;
        this.fileLength = fileLength;
        this.sourceDataNode = sourceDataNode;
        this.destDataNode = destDataNode;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public long getFileLength() {
        return fileLength;
    }

    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }

    public DataNodeInfo getSourceDataNode() {
        return sourceDataNode;
    }

    public void setSourceDataNode(DataNodeInfo sourceDataNode) {
        this.sourceDataNode = sourceDataNode;
    }

    public DataNodeInfo getDestDataNode() {
        return destDataNode;
    }

    public void setDestDataNode(DataNodeInfo destDataNode) {
        this.destDataNode = destDataNode;
    }
}
