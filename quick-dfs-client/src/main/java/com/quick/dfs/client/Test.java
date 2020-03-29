package com.quick.dfs.client;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/03/29 15:41
 **/
public class Test {

    public static void main(String[] args) throws Exception{
        FileSystem fileSystem = new FileSystemImpl();
        for(int i=0;i<10;i++){
            for(int j=0;j<100;j++){
                String path = "/test/hahahahahahahahahaha/"+i+"/"+j;
                fileSystem.mkDir(path);
            }
        }
    }
}
