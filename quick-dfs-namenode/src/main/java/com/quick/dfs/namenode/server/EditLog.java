package com.quick.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

/**
 * @项目名称: quick-dfs
 * @描述: 操作记录  一条edit log
 * @作者: fansy
 * @日期: 2020/3/23 15:26
 **/
public class EditLog {

    private long txid;
    private String content;
    public EditLog(long txid,String content){
        this.txid = txid;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid", txid);
        this.content = jsonObject.toJSONString();
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txId) {
        this.txid = txId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
