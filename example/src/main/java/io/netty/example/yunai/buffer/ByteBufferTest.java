package io.netty.example.yunai.buffer;

import java.nio.ByteBuffer;

public class ByteBufferTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put((byte) 1);
        buffer.put((byte) 1);
        System.out.println(buffer.position() + "\n" + buffer.limit());
        buffer.flip();
        System.out.println(buffer.position() + "\n" + buffer.limit());
        buffer.limit(buffer.capacity());
        System.out.println(buffer.position() + "\n" + buffer.limit());
    }

}
