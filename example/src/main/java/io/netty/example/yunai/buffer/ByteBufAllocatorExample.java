package io.netty.example.yunai.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;

import java.nio.ByteOrder;

public class ByteBufAllocatorExample {

    public static void main(String[] args) {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);


        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
//        ByteBuf buf = allocator.directBuffer(1, 32);
        ByteBuf buf = allocator.directBuffer(511, 512);
        buf = buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.retainedSlice();


        buf.capacity(257);

        buf.writeByte(1);

        buf = allocator.directBuffer(31, 100);
        buf.writeByte(1);
        System.out.println(buf.getInt(0));
    }

//    public static void main(String[] args) {
//        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
//        ByteBuf buf = allocator.heapBuffer(10, 100);
//        buf.writeByte(1);
//
//        buf = allocator.heapBuffer(31, 100);
//        buf.writeByte(1);
//        System.out.println(buf.getInt(0));
//    }

    public static void test() {

    }

}