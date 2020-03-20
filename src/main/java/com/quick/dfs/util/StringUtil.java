package com.quick.dfs.util;

/**
 * @项目名称: quick-dfs
 * @描述:
 * @作者: fansy
 * @日期: 2019-07-11 16:54
 **/
public class StringUtil {

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    public static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
    }

    /**
     * @方法名: getOracleChar
     * @描述: 获取oracle中的字符串
     * @param obj
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2019/7/25 19:29
     */
    public static String toOracleChar(String obj){
        if(obj != null && !(obj.startsWith("'")&&obj.endsWith("'"))){
            return "'"+obj+"'";
        }
        return obj;
    }

    /**  
     * @方法名: removeEnter
     * @描述: 去除换行符
     * @param obj
     * @return java.lang.String
     * @作者: fansy
     * @日期: 2019/7/30 13:39 
     */
    public static String removeEnter(String obj){
        if(StringUtil.isNotEmpty(obj)){
            return obj.replaceAll("\r|\n", "");
        }
        return obj;
    }
}
