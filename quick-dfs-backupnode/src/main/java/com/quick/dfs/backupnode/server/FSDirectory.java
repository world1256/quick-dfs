package com.quick.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.quick.dfs.util.StringUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @项目名称: quick-dfs
 * @描述: 分布式文件目录管理组件
 * @作者: fansy
 * @日期: 2020/3/18 15:30
 **/
public class FSDirectory {

    /**
     * 文件目录树根目录
     */
    private static String FS_ROOT = "/";

    /**
     * 内存中的文件目录树
     */
    private INode root;

    /**
     * 当前文件目录树中最大的txid
     */
    private long maxTxid;

    /**
     * 文件目录树的读写锁
     */
    private ReentrantReadWriteLock lock  = new ReentrantReadWriteLock();

    private void writeLock(){
        lock.writeLock().lock();
    }

    private void writeUnLock(){
        lock.writeLock().unlock();
    }

    private void readLock(){
        lock.readLock().lock();
    }

    private void readUnLock(){
        lock.readLock().unlock();
    }

    public FSDirectory(){
        this.root = new INode(FS_ROOT);
    }

    /**
     * @方法名: getFsImage
     * @描述:  获取当前最新的内存目录树
     * @param   
     * @return com.quick.dfs.backupnode.server.FSImage  
     * @作者: fansy
     * @日期: 2020/3/25 16:32 
    */  
    public FSImage getFsImage(){
        FSImage fsImage = null;
        try{
            readLock();
            String fsImageJsonString = JSONObject.toJSONString(root);
            fsImage = new FSImage(maxTxid,fsImageJsonString);
        }finally {
            readUnLock();
        }
        return fsImage;
    }

    /**  
     * @方法名: mkDir
     * @描述:  创建目录
     * @param txid
     * @param path  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/25 16:12 
    */  
    public void mkDir(long txid,String path){

        try{
            writeLock();
            maxTxid = txid;
            String[] paths = path.split("/");
            INode parent = root;

            //对文件目录各层级做判断  如果父级目录不存在  先创建父级目录
            for(String splitPath : paths){
                //最目录最后的空名不做处理
                if(StringUtil.isEmpty(splitPath.trim())){
                    continue;
                }

                INode dir = findDiretory(splitPath,parent);
                //目录已经存在  不做处理
                if(dir != null){
                    parent = dir;
                    continue;
                }

                //创建不存在的目录  并将该目录置为parent  继续处理
                INode child = new INode(splitPath);
                parent.addChild(child);
                parent = child;
            }
        }finally {
            writeUnLock();
        }

//        showRoot(root,"");
    }

    /**  
     * @方法名: showRoot
     * @描述:   输出文件目录树   测试使用
     * @param root
     * @param blank  
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/23 15:09 
    */  
    private void showRoot(INode root,String blank){
        if(root.getChildren().size() == 0){
            return;
        }

        for(INode node : root.getChildren()){
            System.out.println(blank + node.getPath());
            showRoot(node,blank + " ");
        }
    }


    /**  
     * @方法名: findDiretory
     * @描述:   在指定目录中查找path
     * @param path
     * @param parent  
     * @return com.quick.dfs.namenode.server.FSDirectory.INode
     * @作者: fansy
     * @日期: 2020/3/18 16:55 
    */  
    public INode findDiretory(String path,INode parent){
        List<INode> children = parent.getChildren();
        for(INode child : children){
            if(child.getPath().equals(path)){
                return child;
            }
        }
        return null;
    }

    /**
     * 方法名: createFile
     * 描述:   新建文件
     * @param txid
     * @param filePath
     * @return boolean
     * 作者: fansy
     * 日期: 2020/3/29 20:34
     */
    public void createFile(long txid,String filePath){
        try{
            writeLock();
            this.maxTxid = txid;
            String[] paths = filePath.split("/");
            String fileName = paths[paths.length-1];
            INode parent = root;

            //对文件目录各层级做判断  如果父级目录不存在  先创建父级目录
            for(int i=0;i <paths.length-1;i++){
                String splitPath = paths[i];
                //最目录最后的空名不做处理
                if(StringUtil.isEmpty(splitPath.trim())){
                    continue;
                }

                INode dir = findDiretory(splitPath,parent);
                //目录已经存在  不做处理
                if(dir != null){
                    parent = dir;
                    continue;
                }

                //创建不存在的目录  并将该目录置为parent  继续处理
                INode child = new INode(splitPath);
                parent.addChild(child);
                parent = child;
            }

            //文件已存在
            if(existFile(fileName,parent)){
                return;
            }

            //文件不存在  创建
            INode child = new INode(fileName);
            parent.addChild(child);
        }finally {
            writeUnLock();
        }
    }

    /**
     * 方法名: existFile
     * 描述:   文件是否已存在
     * @param fileName
     * @param parent
     * @return boolean
     * 作者: fansy
     * 日期: 2020/3/29 20:34
     */
    private boolean existFile(String fileName,INode parent){
        for(INode child : parent.getChildren()){
            if(child.getPath().equals(fileName)){
                return true;
            }
        }
        return false;
    }

    public INode getRoot() {
        return root;
    }

    public void setRoot(INode root) {
        this.root = root;
    }

    public long getMaxTxid() {
        return maxTxid;
    }

    public void setMaxTxid(long maxTxid) {
        this.maxTxid = maxTxid;
    }

    /**
     * 代表文件目录树中的一个目录
     * @author zhonghuashishan
     *
     */
    public static class INode {

        private String path;
        private List<INode> children;

        public INode(String path) {
            this.path = path;
            this.children = new LinkedList<INode>();
        }

        public void addChild(INode inode) {
            this.children.add(inode);
        }

        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
        public List<INode> getChildren() {
            return children;
        }
        public void setChildren(List<INode> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "INode [path=" + path + ", children=" + children + "]";
        }

    }

}
