package com.quick.dfs.client;

import com.quick.dfs.namenode.rpc.model.MkDirRequest;
import com.quick.dfs.namenode.rpc.model.MkDirResponse;
import com.quick.dfs.namenode.rpc.model.ShutdownRequest;
import com.quick.dfs.namenode.rpc.model.ShutdownResponse;
import com.quick.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @项目名称: quick-dfs
 * @描述: 客户端文件系统操作实现类
 * @作者: fansy
 * @日期: 2020/3/23 14:23
 **/
public class FileSystemImpl implements FileSystem{

    private static final String NAMENODE_HOSTNAME = "localhost";

    private static final int NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(this.NAMENODE_HOSTNAME,this.NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    /**
     * @方法名: mkDir
     * @描述:   创建目录
     * @param path
     * @return void
     * @作者: fansy
     * @日期: 2020/3/23 14:42
    */
    @Override
    public void mkDir(String path) throws Exception {
        MkDirRequest mkDirRequest = MkDirRequest.newBuilder()
                .setPath(path).build();

        MkDirResponse mkDirResponse = this.namenode.mkDir(mkDirRequest);

        System.out.println("创建目录的响应：" +mkDirResponse.getStatus());
    }

    /**  
     * @方法名: shutdown
     * @描述:   关闭namenode  停止服务
     * @param   
     * @return void  
     * @作者: fansy
     * @日期: 2020/3/24 8:51 
    */  
    @Override
    public void shutdown() throws Exception {
        ShutdownRequest shutdownRequest = ShutdownRequest.newBuilder()
                .setCode(1).build();

       this.namenode.shutdown(shutdownRequest);
    }
}
