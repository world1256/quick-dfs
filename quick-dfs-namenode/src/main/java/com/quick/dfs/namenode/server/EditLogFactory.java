package com.quick.dfs.namenode.server;

import com.quick.dfs.constant.EditLogOp;

/**
 * @项目名称: quick-dfs
 * @描述: eidtLog生成 工厂类
 * @作者: fansy
 * @日期: 2020/03/29 20:52
 **/
public class EditLogFactory {

    /**  
     * 方法名: mkdir
     * 描述:   创建文件夹
     * @param path  
     * @return java.lang.String  
     * 作者: fansy 
     * 日期: 2020/3/29 20:53 
     */  
    public static String mkdir(String path) {
        return "{'OP':'"+ EditLogOp.MK_DIR +"','PATH':'" + path + "'}";
    }

    /**
     * 方法名: create
     * 描述:   新建文件
     * @param path
     * @return java.lang.String
     * 作者: fansy
     * 日期: 2020/3/29 20:53
     */
    public static String create(String path) {
        return "{'OP':'"+ EditLogOp.CREATE +"','PATH':'" + path + "'}";
    }

}
