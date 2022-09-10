package io.netty.example.yunai.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PoolChunkExample {

    public static void main(String[] args) {
//        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
//        ByteBuf buf = allocator.buffer( 16);
//        buf.release();
//
//        allocator.buffer( 8 * 1024 );
//        allocator.buffer( 8 * 1024 );

//        main1();
//        main2();
//        main3();
//        main4();
//        main5();
        main6();
    }

    // 情况1：
    // 先申请 16B
    // 再申请 32B
    // 猜测：是不是会有多个 Page ，拆分成 Subpage。
    // 结果：会
    public static void main1() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        allocator.buffer(16);
        allocator.buffer(32);
        allocator.buffer(64);
    }

    // 情况2：
    public static void main2() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        allocator.buffer(16);
        allocator.buffer(16);
        allocator.buffer(16);
    }

    // 情况3：
    // A 申请 4KB
    // B 申请 4KB 耗尽一个 Page
    // C 申请 4KB
    // B 释放 4KB
    // 猜测：看看链的情况
    public static void main3() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ByteBuf a = allocator.buffer(4096);
        ByteBuf b = allocator.buffer(4096);
        ByteBuf c = allocator.buffer(4096);

        a.release();
        b.release();
    }

    public static void main4() {
//        PooledByteBufAllocator.DEFAULT.buffer(1024);

        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        for (int i = 0; i < 64; i++) {
            allocator.buffer(16);
        }
        allocator.buffer(16);
    }

    public static void main5() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ByteBuf b = allocator.buffer(16);
        b.release();
    }

    public static void main6() {
        ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
        ByteBuf b = allocator.buffer(16 * 1024 * 1024 + 1);
        b.release();
    }

}