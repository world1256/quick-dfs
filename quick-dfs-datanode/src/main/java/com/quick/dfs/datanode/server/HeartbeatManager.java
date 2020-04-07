package com.quick.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.quick.dfs.constant.CommandType;
import com.quick.dfs.constant.ConfigConstant;
import com.quick.dfs.namenode.rpc.model.HeartbeatResponse;
import com.quick.dfs.thread.Daemon;
import com.quick.dfs.util.StringUtil;

/**
 * @项目名称: quick-dfs
 * @描述: 心跳管理组件
 * @作者: fansy
 * @日期: 2020/04/04 14:13
 **/
public class HeartbeatManager {

    private NameNodeRpcClient nameNode;

    private StorageManager storageManager;

    public HeartbeatManager(NameNodeRpcClient nameNode,StorageManager storageManager){
        this.nameNode = nameNode;
        this.storageManager = storageManager;
    }

    /**
     * 开始发送心跳
     */
    public void start(){
        new HeartbeatThread().start();
    }

    /**
     * 心跳线程
     */
    class HeartbeatThread extends Daemon{
        @Override
        public void run() {
            while (true){
                //TODO  send heartbeat
                try {
                    HeartbeatResponse heartbeatResponse = nameNode.heartbeat();

                    String commands = heartbeatResponse.getCommands();
                    if(StringUtil.isNotEmpty(commands)){
                        executeCommands(commands);
                    }

                    //心跳间隔
                    Thread.sleep(ConfigConstant.DATA_NODE_HEARTBEAT_INTERVAL);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 方法名: executeCommands
     * 描述:   执行nameNode下发的命令
     * @param commands
     * @return void
     * 作者: fansy
     * 日期: 2020/4/4 14:27
     */
    private void executeCommands(String commands){
        JSONArray commandsJson = JSONArray.parseArray(commands);
        for(int i = 0;i < commandsJson.size();i++){
            JSONObject command = commandsJson.getJSONObject(i);
            String commandType = command.getString("type");
            switch (commandType){
                //重新注册
                case CommandType.REGISTER:
                    this.nameNode.register();
                    break;
                //全量上报存储文件信息
                case CommandType.REPORT_COMPLETE_STORAGE_INFO:
                    StorageInfo storageInfo = this.storageManager.getStorageInfo();
                    this.nameNode.reportCompleteStorageInfo(storageInfo);
                    break;
                //复制文件
                case CommandType.REPLICATE:
                    break;
                default:
                    break;
            }
        }
    }
}
