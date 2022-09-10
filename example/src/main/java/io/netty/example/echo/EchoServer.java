/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.example.echo;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.util.concurrent.Callable;

/**
 * Echoes back any received data from a client.
 */
public final class EchoServer {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));

    public static final class Callabler implements Callable {

        @Override
        public Object call() throws Exception {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        // Configure SSL.
        // 配置 SSL
        final SslContext sslCtx;
        if (SSL) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        // Configure the server.
        // 创建两个 EventLoopGroup 对象
        EventLoopGroup bossGroup = new NioEventLoopGroup(1); // 创建 boss 线程组 用于服务端接受客户端的连接
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // 创建 worker 线程组 用于进行 SocketChannel 的数据读写

//        ((NioEventLoop) workerGroup.next()).threadProperties();
//        Collection<Callabler> callablers = new ArrayList<Callabler>();
//        for (int i = 0; i < 10; i++) {
//            callablers.add(new Callabler());
//        }

//        Set<Callable<Boolean>> set = Collections.<Callable<Boolean>>singleton(new Callable<Boolean>() {
//            @Override
//            public Boolean call() throws Exception {
//                return Boolean.TRUE;
//            }
//        });
//        ((NioEventLoop) workerGroup.next()).invokeAny(set);

//        Thread.sleep(Long.MAX_VALUE);

        // 创建 EchoServerHandler 对象
        final EchoServerHandler serverHandler = new EchoServerHandler();
        try {
            // 创建 ServerBootstrap 对象
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup) // 设置使用的 EventLoopGroup
             .channel(NioServerSocketChannel.class) // 设置要被实例化的为 NioServerSocketChannel 类
             .option(ChannelOption.SO_BACKLOG, 100) // 设置 NioServerSocketChannel 的可选项
             .handler(new LoggingHandler(LogLevel.INFO)) // 设置 NioServerSocketChannel 的处理器
//                .handler(new IdleStateHandler(0, 1, 0, TimeUnit.SECONDS))
//            .handler(new ChannelInitializer<Channel>() {
//
//                @Override
//                protected void initChannel(Channel ch) {
//                    final ChannelPipeline pipeline = ch.pipeline();
////                    ch.eventLoop().execute(new Runnable() {
////                        @Override
////                        public void run() {
////                            pipeline.addLast(new LoggingHandler(LogLevel.INFO));
////                        }
////                    });
//                    pipeline.addLast(new LoggingHandler(LogLevel.INFO));
////                    pipeline.addLast(new ChannelOutboundHandlerAdapter() {
////
////                        @Override
////                        public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
//////                            super.bind(ctx, localAddress, promise);
////                            if (true) {
////                                throw new RuntimeException("测试异常");
////                            }
////
////                        }
////
////                        @Override
////                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
////                            super.exceptionCaught(ctx, cause);
////                        }
////                    });
//
////                    pipeline.addLast(new ChannelInboundHandlerAdapter() {
////
//////                        @Override
//////                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//////                            super.exceptionCaught(ctx, cause);
//////                        }
////
////                        @Override
////                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
////                            throw new RuntimeException("测试异常");
////                        }
////                    });
//                }
//
//            })
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception { // 设置连入服务端的 Client 的 SocketChannel 的处理器
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc()));
                     }

//                     p.addLast(new IdleStateHandler(0, 1, 0, TimeUnit.SECONDS));

                     p.addLast(new LineBasedFrameDecoder(Integer.MAX_VALUE));

                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     p.addLast(serverHandler);
                 }
             })

            .childOption(ChannelOption.SO_SNDBUF, 5)
            .childOption(ChannelOption.SO_LINGER, 100)

            ;

            // Start the server.
            // 绑定端口，并同步等待成功，即启动服务端
            ChannelFuture f = b.bind(PORT).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    System.out.println("测试下被触发");
                }
            }).sync();

//            bossGroup.schedule(new Runnable() {
//                @Override
//                public void run() {
//                    System.out.println("执行定时任务");
//                }
//            }, 5, TimeUnit.SECONDS);

//            f.channel().writeAndFlush("123").addListener(new ChannelFutureListener() {
//                @Override
//                public void operationComplete(ChannelFuture future) throws Exception {
//                    System.out.println("干啥呢");
//                }
//            });

//            f.channel().close();

            // Wait until the server socket is closed.
            // 监听服务端关闭，并阻塞等待
            f.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            // 优雅关闭两个 EventLoopGroup 对象
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}