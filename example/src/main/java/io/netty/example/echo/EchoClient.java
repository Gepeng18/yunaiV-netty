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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Sends one message when a connection is open and echoes back any received
 * data to the server.  Simply put, the echo client initiates the ping-pong
 * traffic between the echo client and server by sending the first message to
 * the server.
 */
public final class EchoClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
//    static final String HOST = System.getProperty("host", "www.123132131321.com");
    static final int PORT = Integer.parseInt(System.getProperty("port", "8007"));
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));

    public static void main(String[] args) throws Exception {
        // Configure SSL.git
        // 配置 SSL
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        // Configure the client.
        // 创建一个 EventLoopGroup 对象
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            // 创建 Bootstrap 对象
            Bootstrap b = new Bootstrap();
            b.group(group) // 设置使用的 EventLoopGroup
             .channel(NioSocketChannel.class) // 设置要被实例化的为 NioSocketChannel 类
             .option(ChannelOption.TCP_NODELAY, true) // 设置 NioSocketChannel 的可选项
             .handler(new ChannelInitializer<SocketChannel>() { // 设置 NioSocketChannel 的处理器
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                     }
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     p.addLast(new EchoClientHandler());
                 }
             });

            // Start the client.
            // 连接服务器，并同步等待成功，即启动客户端
            final ChannelFuture f = b.connect(HOST, PORT);
//            final Channel channel = f.channel();
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    System.out.println("连接完成");

//                    f.channel().disconnect();

//                    channel.close();throw new RuntimeException("抛出异常");
                }
            });

//            f.cancel(false);



            // Wait until the connection is closed.
            // 监听客户端关闭，并阻塞等待
            f.channel().closeFuture().sync();
        } finally {
            // Shut down the event loop to terminate all threads.
            // 优雅关闭一个 EventLoopGroup 对象
            group.shutdownGracefully();
        }
    }
}