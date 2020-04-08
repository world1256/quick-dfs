package com.quick.dfs.util;

import com.quick.dfs.constant.ConfigConstant;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2020/3/25 16:43
 **/
public class FileUtil {

    /**  
     * @方法名: closeOutputFile
     * @描述:   关闭文件输出IO
     * @param file
     * @param out
     * @param channel  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/25 16:46 
    */  
    public static void closeOutputFile(RandomAccessFile file, FileOutputStream out, FileChannel channel) throws IOException {
        if(channel != null){
            channel.close();
        }
        if(out != null){
            out.close();
        }
        if(file!=null){
            file.close();
        }
    }

    /**
     * 方法名: closeInputFile
     * 描述:   关闭文件输入IO
     * @param in
     * @param channel  
     * @return void  
     * 作者: fansy 
     * 日期: 2020/3/29 14:09 
     */  
    public static void closeInputFile(FileInputStream in,FileChannel channel) throws IOException{
        if(channel != null){
            channel.close();
        }
        if(in != null){
            in.close();
        }
    }

    /**  
     * @方法名: getAbsoluteFileName
     * @描述:   获取文件存放的绝对路径名
     *  文件夹不存在  则先创建文件夹
     * @param relativeFileName  
     * @return String
     * @作者: fansy
     * @日期: 2020/4/8 15:05 
    */  
    public static String getAbsoluteFileName(String relativeFileName){
        //如果文件目录不存在  先创建文件目录
        String dirPath = ConfigConstant.DATA_NODE_DATA_PATH + relativeFileName.substring(0,relativeFileName.lastIndexOf("/")+1);
        File dir = new File(dirPath);
        if(!dir.exists()){
            dir.mkdirs();
        }
        return ConfigConstant.DATA_NODE_DATA_PATH + relativeFileName;
    }
}
