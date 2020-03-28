package com.quick.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.thread.Daemon;
import com.quick.dfs.util.EditLogOp;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/24 9:53
 **/
public class EditLogFetcher extends Daemon {

    public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

    private BackupNode backupNode;

    private NameNodeRpcClient namenode;

    private FSNameSystem nameSystem;

    private long syncedTxid = 0l;

    public EditLogFetcher(BackupNode backupNode,FSNameSystem nameSystem,NameNodeRpcClient namenode){
        this.backupNode = backupNode;
        this.namenode = namenode;
        this.nameSystem = nameSystem;
    }

    @Override
    public void run() {
        System.out.println("editLog 拉取线程启动...");
        while (this.backupNode.isRunning()){
            try {
                JSONArray editLogs = namenode.fetchEditLog(syncedTxid);
                if(editLogs.size() == 0){
                    System.out.println("本次没有拉取到editLog,等待1秒后继续拉取...");
                    Thread.sleep(1000);
                    continue;
                }

                if(editLogs.size() < BACKUP_NODE_FETCH_SIZE){
                    System.out.println("拉取的editLog数量不足"+BACKUP_NODE_FETCH_SIZE+"条,等待1秒后继续拉取...");
                    Thread.sleep(1000);
                }

                for(int i = 0;i < editLogs.size();i++){
                    JSONObject editLog = editLogs.getJSONObject(i);
                    String op = editLog.getString("OP");
                    long txid = editLog.getLongValue("txid");

                    if(op.equals(EditLogOp.MK_DIR)) {
                        String path = editLog.getString("PATH");
                        try {
                            this.nameSystem.mkDir(txid,path);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    syncedTxid = txid;
                }
                namenode.setIsNamenodeRunning(true);
            }catch (Exception e) {
                namenode.setIsNamenodeRunning(false);
                e.printStackTrace();
            }
        }

    }
}
