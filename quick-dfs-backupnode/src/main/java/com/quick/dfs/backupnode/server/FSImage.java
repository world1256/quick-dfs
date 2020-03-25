package com.quick.dfs.backupnode.server;

/**
 * @项目名称: quick-dfs
 * @描述: 元数据快照
 * @作者: fansy
 * @日期: 2020/3/25 16:21
 **/
public class FSImage {

    private long txid;

    private String fsImageJosn;

    public FSImage(long txid,String fsImageJosn){
        this.txid = txid;
        this.fsImageJosn = fsImageJosn;
    }

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public String getFsImageJosn() {
        return fsImageJosn;
    }

    public void setFsImageJosn(String fsImageJosn) {
        this.fsImageJosn = fsImageJosn;
    }
}
