package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

/**
 * @项目名称: quick-dfs
 * @描述: 操作记录  一条edit log
 * @作者: fansy
 * @日期: 2020/3/23 15:26
 **/
public class EditLog {

    private long txId;
    private String content;
    public EditLog(long txId,String content){
        this.txId = txId;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid", txId);
        this.content = jsonObject.toJSONString();
    }

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}