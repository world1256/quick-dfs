package com.quick.dfs.namenode.server;

import com.quick.dfs.util.StringUtil;

import java.util.LinkedList;
import java.util.List;

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

    public FSDirectory(){
        this.root = new INode(FS_ROOT);
    }

    public void mkDir(String path){

        synchronized (root){
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

    public INode getRoot() {
        return root;
    }

    public void setRoot(INode root) {
        this.root = root;
    }

    /**
     * 代表文件目录树中的一个目录
     */
    public static class INode {

        private String path;
        private List<INode> children;

        public INode() {

        }

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
