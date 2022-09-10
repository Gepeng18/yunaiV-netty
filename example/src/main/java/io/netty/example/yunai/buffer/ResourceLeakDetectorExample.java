package io.netty.example.yunai.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

public class ResourceLeakDetectorExample {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("io.netty.threadLocalDirectBufferSize", "20");
        ByteBuf byteBuf = ByteBufUtil.threadLocalDirectBuffer();
        System.out.println(byteBuf);
        byteBuf = ByteBufUtil.threadLocalDirectBuffer();
        System.out.println(byteBuf);
        byteBuf.release();

        byteBuf = ByteBufUtil.threadLocalDirectBuffer();
        System.out.println(byteBuf);


        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);


//        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;
//        ByteBuf buf = allocator.directBuffer(1, 32);
        test(allocator);

//        Thread.sleep(1);
        System.gc();

        test(allocator);
    }


    public static void test(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.directBuffer(511, 512);
        buf.touch();
        buf.touch(123);
        buf.touch("hhhh");

        buf.release();
    }

}
