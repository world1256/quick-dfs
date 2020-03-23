package com.quick.dfs.thread;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/20 16:14
 **/
public class Daemon extends Thread{
    public Daemon(){
        this.setDaemon(true);
    }
}
