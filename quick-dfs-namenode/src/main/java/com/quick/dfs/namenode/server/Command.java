package com.quick.dfs.namenode.server;

/**
 * @项目名称: quick-dfs
 * @描述: nameNode下发指令对象
 * @作者: fansy
 * @日期: 2020/4/7 16:17
 **/
public class Command {

    private String type;

    private String content;

    public Command(String type){
        this.type = type;
    }

    public Command(String type,String content){
        this.type = type;
        this.content = content;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
