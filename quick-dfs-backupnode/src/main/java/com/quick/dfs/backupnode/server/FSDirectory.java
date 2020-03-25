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
    private INodeDirectory root;

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
        this.root = new INodeDirectory(FS_ROOT);
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
            INodeDirectory parent = root;

            //对文件目录各层级做判断  如果父级目录不存在  先创建父级目录
            for(String splitPath : paths){
                //最目录最后的空名不做处理
                if(StringUtil.isEmpty(splitPath.trim())){
                    continue;
                }

                INodeDirectory dir = findDiretory(splitPath,parent);
                //目录已经存在  不做处理
                if(dir != null){
                    parent = dir;
                    continue;
                }

                //创建不存在的目录  并将该目录置为parent  继续处理
                INodeDirectory child = new INodeDirectory(splitPath);
                parent.addChild(child);
                parent = child;
            }
        }finally {
            writeUnLock();
        }

        showRoot(root,"");
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
    private void showRoot(INodeDirectory root,String blank){
        if(root.getChildren().size() == 0){
            return;
        }

        for(INode node : root.getChildren()){
            if(node instanceof INodeDirectory){
                System.out.println(blank + ((INodeDirectory) node).getPath());
                showRoot((INodeDirectory)node,blank + " ");
            }
        }
    }


    /**  
     * @方法名: findDiretory
     * @描述:   在指定目录中查找path
     * @param path
     * @param parent  
     * @return com.quick.dfs.namenode.server.FSDirectory.INodeDirectory  
     * @作者: fansy
     * @日期: 2020/3/18 16:55 
    */  
    public INodeDirectory findDiretory(String path,INodeDirectory parent){
        List<INode> children = parent.getChildren();
        for(INode child : children){
            if(child instanceof INodeDirectory){
                INodeDirectory dir = (INodeDirectory) child;
                if(dir.getPath().equals(path)){
                    return dir;
                }
            }
        }
        return null;
    }


    /**
     * 文件目录数中的节点
     */
    private interface INode{

    }

    /**
     * 文件目录数中的目录
     */
    private static class INodeDirectory implements INode{

        private String path;
        private List<INode> children;

        public INodeDirectory(String path){
            this.path = path;
            this.children = new LinkedList<INode>();
        }

        public void addChild(INode child){
            children.add(child);
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
    }

    /**
     * 文件目录树中的文件
     */
    private static class INodeFile implements INode{
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

}
