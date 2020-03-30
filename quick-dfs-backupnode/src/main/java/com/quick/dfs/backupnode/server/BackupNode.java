package com.quick.dfs.backupnode.server;

/**
 * @项目名称: quick-dfs
 * @描述: edit log 备份节点
 * @作者: fansy
 * @日期: 2020/3/24 9:18
 **/
public class BackupNode {

    private volatile boolean isRunning;

    private FSNameSystem nameSystem;

    private NameNodeRpcClient namenode;

    public static void main(String[] args) {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
        backupNode.run();
    }

    private void init(){
        this.isRunning = true;
        this.nameSystem = new FSNameSystem();
        this.namenode = new NameNodeRpcClient();
    }

    public void start(){
        EditLogFetcher editLogFetcher = new EditLogFetcher(this,nameSystem,namenode);
        editLogFetcher.start();
        FSImageCheckPointer fsImageCheckPointer = new FSImageCheckPointer(this,nameSystem,namenode);
        fsImageCheckPointer.start();
    }

    public void run(){
        while(isRunning){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isRunning() {
        return isRunning;
    }
}
