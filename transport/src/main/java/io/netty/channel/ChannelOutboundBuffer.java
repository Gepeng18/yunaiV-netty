/*
 * Copyright 2013 The Netty Project
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
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.InternalThreadLocalMap;
import io.netty.util.internal.PromiseNotificationUtil;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.lang.Math.min;

/**
 * (Transport implementors only) an internal data structure used by {@link AbstractChannel} to store its pending
 * outbound write requests.
 * <p>
 * All methods must be called by a transport implementation from an I/O thread, except the following ones:
 * <ul>
 * <li>{@link #size()} and {@link #isEmpty()}</li>
 * <li>{@link #isWritable()}</li>
 * <li>{@link #getUserDefinedWritability(int)} and {@link #setUserDefinedWritability(int, boolean)}</li>
 * </ul>
 * </p>
 */
public final class ChannelOutboundBuffer {

    /**
     * Entry 对象自身占用内存的大小
     */
    // Assuming a 64-bit JVM:
    //  - 16 bytes object header
    //  - 8 reference fields
    //  - 2 long fields
    //  - 2 int fields
    //  - 1 boolean field
    //  - padding
    static final int CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD = SystemPropertyUtil.getInt("io.netty.transport.outboundBufferEntrySizeOverhead", 96);

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);

    /**
     * 线程对应的 ByteBuffer 数组缓存
     *
     * 每次调用 {@link #nioBuffers(int, long)} 会重新生成
     */
    private static final FastThreadLocal<ByteBuffer[]> NIO_BUFFERS = new FastThreadLocal<ByteBuffer[]>() {

        @Override
        protected ByteBuffer[] initialValue() throws Exception {
            return new ByteBuffer[1024];
        }

    };

    /**
     * Channel 对象
     */
    private final Channel channel;

    // Entry(flushedEntry) --> ... Entry(unflushedEntry) --> ... Entry(tailEntry)
    //
    /**
     * 第一个( 开始 ) flush Entry
     */
    // The Entry that is the first in the linked-list structure that was flushed
    private Entry flushedEntry;
    /**
     * 第一个未 flush Entry
     */
    // The Entry which is the first unflushed in the linked-list structure
    private Entry unflushedEntry;
    /**
     * 尾 Entry
     */
    // The Entry which represents the tail of the buffer
    private Entry tailEntry;
    /**
     * 已 flush 但未写入对端的 Entry 数量
     *
     * {@link #addFlush()}
     *
     * The number of flushed entries that are not written yet
     */
    private int flushed;

    /**
     * {@link #NIO_BUFFERS} 数组大小
     */
    private int nioBufferCount;
    /**
     * {@link #NIO_BUFFERS} 字节数
     */
    private long nioBufferSize;
    /**
     * 正在通知 flush 失败中
     */
    private boolean inFail;

    /**
     * {@link #totalPendingSize} 的原子更新器
     */
    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER = AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
    /**
     * 总共等待 flush 到对端的内存大小，通过 {@link Entry#pendingSize} 来合计。
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile long totalPendingSize;

    /**
     * {@link #unwritable} 的原子更新器
     */
    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> UNWRITABLE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "unwritable");
    /**
     * 是否不可写
     */
    @SuppressWarnings("UnusedDeclaration")
    private volatile int unwritable;

    /**
     * 触发 Channel 可写的改变的任务
     */
    private volatile Runnable fireChannelWritabilityChangedTask;

    ChannelOutboundBuffer(AbstractChannel channel) {
        this.channel = channel;
    }

    /**
     * Add given message to this {@link ChannelOutboundBuffer}. The given {@link ChannelPromise} will be notified once
     * the message was written.
     */
    public void addMessage(Object msg, int size, ChannelPromise promise) {
        // 创建新 Entry 对象
        Entry entry = Entry.newInstance(msg, size, total(msg), promise);
        // 若 tailEntry 为空，将 flushedEntry 也设置为空。防御型编程，实际不会出现
        if (tailEntry == null) {
            flushedEntry = null;
        // 若 tailEntry 非空，将原 tailEntry 指向新 Entry
        } else {
            Entry tail = tailEntry;
            tail.next = entry;
        }
        // 更新 tailEntry 为新 Entry
        tailEntry = entry;
        // 若 unflushedEntry 为空，更新为新 Entry
        if (unflushedEntry == null) {
            unflushedEntry = entry;
        }

        // 增加 totalPendingSize 计数
        // increment pending bytes after adding message to the unflushed arrays.
        // See https://github.com/netty/netty/issues/1619
        incrementPendingOutboundBytes(entry.pendingSize, false);
    }

    /**
     * Add a flush to this {@link ChannelOutboundBuffer}. This means all previous added messages are marked as flushed
     * and so you will be able to handle them.
     */
    public void addFlush() {
        // There is no need to process all entries if there was already a flush before and no new messages
        // where added in the meantime.
        //
        // See https://github.com/netty/netty/issues/2577
        Entry entry = unflushedEntry;
        if (entry != null) {
            // 若 flushedEntry 为空，赋值为 unflushedEntry ，用于记录第一个( 开始 ) flush 的 Entry 。
            if (flushedEntry == null) {
                // there is no flushedEntry yet, so start with the entry
                flushedEntry = entry;
            }
            // 计算 flush 的数量，并设置每个 Entry 对应的 Promise 不可取消
            do {
                // 增加 flushed
                flushed ++;
                // 设置 Promise 不可取消
                if (!entry.promise.setUncancellable()) { // 设置失败
                    // 减少 totalPending 计数
                    // Was cancelled so make sure we free up memory and notify about the freed bytes
                    int pending = entry.cancel();
                    decrementPendingOutboundBytes(pending, false, true);
                }
                // 获得下一个 Entry
                entry = entry.next;
            } while (entry != null);

            // 设置 unflushedEntry 为空，表示所有都 flush
            // All flushed so reset unflushedEntry
            unflushedEntry = null;
        }
    }

    /**
     * Increment the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void incrementPendingOutboundBytes(long size) {
        incrementPendingOutboundBytes(size, true);
    }

    private void incrementPendingOutboundBytes(long size, boolean invokeLater) {
        if (size == 0) {
            return;
        }

        // 增加 totalPendingSize 计数
        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, size);
        // totalPendingSize 大于高水位阀值时，设置为不可写
        if (newWriteBufferSize > channel.config().getWriteBufferHighWaterMark()) {
            setUnwritable(invokeLater);
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    void decrementPendingOutboundBytes(long size) {
        decrementPendingOutboundBytes(size, true, true);
    }

    private void decrementPendingOutboundBytes(long size, boolean invokeLater, boolean notifyWritability) {
        if (size == 0) {
            return;
        }

        // 减少 totalPendingSize 计数
        long newWriteBufferSize = TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);
        // totalPendingSize 小于低水位阀值时，设置为可写
        if (notifyWritability && newWriteBufferSize < channel.config().getWriteBufferLowWaterMark()) {
            setWritable(invokeLater);
        }
    }

    /**
     * 计算消息( 数据 )可读字节数
     *
     * @param msg ( 数据 )
     * @return 可读字节数
     */
    private static long total(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    /**
     * Return the current message to write or {@code null} if nothing was flushed before and so is ready to be written.
     */
    public Object current() {
        Entry entry = flushedEntry;
        if (entry == null) {
            return null;
        }

        return entry.msg;
    }

    /**
     * Notify the {@link ChannelPromise} of the current message about writing progress.
     */
    public void progress(long amount) {
        Entry e = flushedEntry;
        assert e != null;
        ChannelPromise p = e.promise;
        if (p instanceof ChannelProgressivePromise) {
            // 设置 Entry 对象的 progress 属性
            long progress = e.progress + amount;
            e.progress = progress;
            // 通知 ChannelProgressivePromise 进度
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
        }
    }

    /**
     * Will remove the current message, mark its {@link ChannelPromise} as success and return {@code true}. If no
     * flushed message exists at the time this method is called it will return {@code false} to signal that no more
     * messages are ready to be handled.
     */
    public boolean remove() {
        Entry e = flushedEntry;
        if (e == null) {
            // 清除 NIO ByteBuff 数组的缓存
            clearNioBuffers();
            return false;
        }
        Object msg = e.msg;

        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        // 移除指定 Entry 对象
        removeEntry(e);

        if (!e.cancelled) {
            // 释放消息( 数据 )相关的资源
            // only release message, notify and decrement if it was not canceled before.
            ReferenceCountUtil.safeRelease(msg);
            // 通知 Promise 执行成功
            safeSuccess(promise);
            // 减少 totalPending 计数
            decrementPendingOutboundBytes(size, false, true);
        }

        // 回收 Entry 对象
        // recycle the entry
        e.recycle();

        return true;
    }

    /**
     * Will remove the current message, mark its {@link ChannelPromise} as failure using the given {@link Throwable}
     * and return {@code true}. If no   flushed message exists at the time this method is called it will return
     * {@code false} to signal that no more messages are ready to be handled.
     */
    public boolean remove(Throwable cause) {
        return remove0(cause, true);
    }

    private boolean remove0(Throwable cause, boolean notifyWritability) {
        Entry e = flushedEntry;
        // 所有 flush 的 Entry 节点，都已经写到对端
        if (e == null) {
            // // 清除 NIO ByteBuff 数组的缓存
            clearNioBuffers();
            return false; // 没有后续的 flush 的 Entry 节点
        }
        Object msg = e.msg;

        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        removeEntry(e);

        if (!e.cancelled) {
            // 释放消息( 数据 )相关的资源
            // only release message, fail and decrement if it was not canceled before.
            ReferenceCountUtil.safeRelease(msg);
            // 通知 Promise 执行成功
            safeFail(promise, cause);
            // 减少 totalPendingSize 计数
            decrementPendingOutboundBytes(size, false, notifyWritability);
        }

        // 回收 Entry 对象
        // recycle the entry
        e.recycle();

        return true; // 还有后续的 flush 的 Entry 节点
    }

    private void removeEntry(Entry e) {
        // 已移除完已 flush 的 Entry 节点，置空 flushedEntry、tailEntry、unflushedEntry 。
        if (-- flushed == 0) {
            // processed everything
            flushedEntry = null;
            if (e == tailEntry) {
                tailEntry = null;
                unflushedEntry = null;
            }
        // 未移除完已 flush 的 Entry 节点，flushedEntry 指向下一个 Entry 对象
        } else {
            flushedEntry = e.next;
        }
    }

    /**
     * Removes the fully written entries and update the reader index of the partially written entry.
     * This operation assumes all messages in this buffer is {@link ByteBuf}.
     */
    public void removeBytes(long writtenBytes) {
        // 循环移除
        for (;;) {
            // 获得当前消息( 数据 )
            Object msg = current();
            if (!(msg instanceof ByteBuf)) {
                assert writtenBytes == 0;
                break;
            }

            final ByteBuf buf = (ByteBuf) msg;
            // 获得消息( 数据 )开始读取位置
            final int readerIndex = buf.readerIndex();
            // 获得消息( 数据 )可读取的字节数
            final int readableBytes = buf.writerIndex() - readerIndex;

            // 当前消息( 数据 )已被写完到对端
            if (readableBytes <= writtenBytes) {
                if (writtenBytes != 0) {
                    // 处理当前消息的 Entry 的写入进度
                    progress(readableBytes);
                    // 减小 writtenBytes
                    writtenBytes -= readableBytes;
                }
                // 移除当前消息对应的 Entry
                remove();
            // 当前消息( 数据 )未被写完到对端
            } else { // readableBytes > writtenBytes
                if (writtenBytes != 0) {
                    // 标记当前消息的 ByteBuf 的读取位置
                    buf.readerIndex(readerIndex + (int) writtenBytes);
                    // 处理当前消息的 Entry 的写入进度
                    progress(writtenBytes);
                }
                break;
            }
        }

        // 清除 NIO ByteBuff 数组的缓存
        clearNioBuffers();
    }

    // Clear all ByteBuffer from the array so these can be GC'ed.
    // See https://github.com/netty/netty/issues/3837
    private void clearNioBuffers() {
        int count = nioBufferCount;
        if (count > 0) {
            // 归零 nioBufferCount 。老艿艿觉得，应该把 nioBufferSize 也归零
            nioBufferCount = 0;
            // 置空 NIO ByteBuf 数组
            Arrays.fill(NIO_BUFFERS.get(), 0, count, null);
        }
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@link #nioBufferCount()} and {@link #nioBufferSize()} will return the number of NIO buffers in the returned
     * array and the total number of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     */
    public ByteBuffer[] nioBuffers() {
        return nioBuffers(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@link #nioBufferCount()} and {@link #nioBufferSize()} will return the number of NIO buffers in the returned
     * array and the total number of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     * @param maxCount The maximum amount of buffers that will be added to the return value.
     * @param maxBytes A hint toward the maximum number of bytes to include as part of the return value. Note that this
     *                 value maybe exceeded because we make a best effort to include at least 1 {@link ByteBuffer}
     *                 in the return value to ensure write progress is made.
     */
    public ByteBuffer[] nioBuffers(int maxCount, long maxBytes) {
        assert maxCount > 0;
        assert maxBytes > 0;
        long nioBufferSize = 0;
        int nioBufferCount = 0;
        // 获得当前线程的 NIO ByteBuffer 数组缓存。
        final InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        ByteBuffer[] nioBuffers = NIO_BUFFERS.get(threadLocalMap);
        // 从 flushedEntry 节点，开始向下遍历
        Entry entry = flushedEntry;
        while (isFlushedEntry(entry) && entry.msg instanceof ByteBuf) {
            // 若 Entry 节点已经取消，忽略。
            if (!entry.cancelled) {
                ByteBuf buf = (ByteBuf) entry.msg;
                // 获得消息( 数据 )开始读取位置
                final int readerIndex = buf.readerIndex();
                // 获得消息( 数据 )可读取的字节数
                final int readableBytes = buf.writerIndex() - readerIndex;

                // 若无可读取的数据，忽略。
                if (readableBytes > 0) {
                    // 前半段，可读取的字节数，不能超过 maxBytes
                    // 后半段，如果第一条数据，就已经超过 maxBytes ，那么只能“强行”读取，否则会出现一直无法读取的情况。
                    if (maxBytes - readableBytes < nioBufferSize && nioBufferCount != 0) {
                        // If the nioBufferSize + readableBytes will overflow maxBytes, and there is at least one entry
                        // we stop populate the ByteBuffer array. This is done for 2 reasons:
                        // 1. bsd/osx don't allow to write more bytes then Integer.MAX_VALUE with one writev(...) call
                        // and so will return 'EINVAL', which will raise an IOException. On Linux it may work depending
                        // on the architecture and kernel but to be safe we also enforce the limit here.
                        // 2. There is no sense in putting more data in the array than is likely to be accepted by the
                        // OS.
                        //
                        // See also:
                        // - https://www.freebsd.org/cgi/man.cgi?query=write&sektion=2
                        // - http://linux.die.net/man/2/writev
                        break;
                    }
                    // 增加 nioBufferSize
                    nioBufferSize += readableBytes;
                    // 初始 Entry 节点的 NIO ByteBuffer 数量
                    int count = entry.count;
                    if (count == -1) {
                        //noinspection ConstantValueVariableUse
                        entry.count = count = buf.nioBufferCount();
                    }
                    // 如果超过 NIO ByteBuffer 数组的大小，进行扩容。
                    int neededSpace = min(maxCount, nioBufferCount + count);
                    if (neededSpace > nioBuffers.length) {
                        nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                        NIO_BUFFERS.set(threadLocalMap, nioBuffers);
                    }
                    // 初始化 Entry 节点的 buf / bufs 属性
                    if (count == 1) {
                        ByteBuffer nioBuf = entry.buf;
                        if (nioBuf == null) {
                            // cache ByteBuffer as it may need to create a new ByteBuffer instance if its a
                            // derived buffer
                            entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                        }
                        nioBuffers[nioBufferCount++] = nioBuf;
                    } else {
                        ByteBuffer[] nioBufs = entry.bufs;
                        if (nioBufs == null) {
                            // cached ByteBuffers as they may be expensive to create in terms
                            // of Object allocation
                            entry.bufs = nioBufs = buf.nioBuffers();
                        }
                        for (int i = 0; i < nioBufs.length && nioBufferCount < maxCount; ++i) {
                            ByteBuffer nioBuf = nioBufs[i];
                            if (nioBuf == null) {
                                break;
                            } else if (!nioBuf.hasRemaining()) {
                                continue;
                            }
                            nioBuffers[nioBufferCount++] = nioBuf;
                        }
                    }

                    // 到达 maxCount 上限，结束循环。老艿艿的想法，这里最好改成 nioBufferCount >= maxCount ，是有可能会超过的
                    if (nioBufferCount == maxCount) {
                        break;
                    }
                }
            }

            // 下一个 Entry节点
            entry = entry.next;
        }

        // 设置 nioBufferCount 和 nioBufferSize 属性
        this.nioBufferCount = nioBufferCount;
        this.nioBufferSize = nioBufferSize;

        return nioBuffers;
    }

    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        // 计算扩容后的数组的大小，按照 2 倍计算
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        // 创建新的 ByteBuffer 数组
        ByteBuffer[] newArray = new ByteBuffer[newCapacity];

        // 复制老的 ByteBuffer 数组到新的 ByteBuffer 数组中
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    /**
     * Returns the number of {@link ByteBuffer} that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public int nioBufferCount() {
        return nioBufferCount;
    }

    /**
     * Returns the number of bytes that can be written out of the {@link ByteBuffer} array that was
     * obtained via {@link #nioBuffers()}. This method <strong>MUST</strong> be called after {@link #nioBuffers()}
     * was called.
     */
    public long nioBufferSize() {
        return nioBufferSize;
    }

    /**
     * Returns {@code true} if and only if {@linkplain #totalPendingWriteBytes() the total number of pending bytes} did
     * not exceed the write watermark of the {@link Channel} and
     * no {@linkplain #setUserDefinedWritability(int, boolean) user-defined writability flag} has been set to
     * {@code false}.
     */
    public boolean isWritable() {
        return unwritable == 0;
    }

    /**
     * Returns {@code true} if and only if the user-defined writability flag at the specified index is set to
     * {@code true}.
     */
    public boolean getUserDefinedWritability(int index) {
        return (unwritable & writabilityMask(index)) == 0;
    }

    /**
     * Sets a user-defined writability flag at the specified index.
     */
    public void setUserDefinedWritability(int index, boolean writable) {
        // 设置可写
        if (writable) {
            setUserDefinedWritability(index);
        // 设置不可写
        } else {
            clearUserDefinedWritability(index);
        }
    }

    private void setUserDefinedWritability(int index) {
        final int mask = ~writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue & mask;
            // CAS 设置 unwritable 为新值
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                // 若之前不可写，现在可写，触发 Channel WritabilityChanged 事件到 pipeline 中。
                if (oldValue != 0 && newValue == 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private void clearUserDefinedWritability(int index) {
        final int mask = writabilityMask(index);
        for (;;) {
            final int oldValue = unwritable;
            final int newValue = oldValue | mask;
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                // 若之前可写，现在不可写，触发 Channel WritabilityChanged 事件到 pipeline 中。
                if (oldValue == 0 && newValue != 0) {
                    fireChannelWritabilityChanged(true);
                }
                break;
            }
        }
    }

    private static int writabilityMask(int index) {
        // 不能 < 1 ，因为第 0 bits 为 ChannelOutboundBuffer 自己使用
        // 不能 > 31 ，因为超过 int 的 bits 范围
        if (index < 1 || index > 31) {
            throw new IllegalArgumentException("index: " + index + " (expected: 1~31)");
        }
        return 1 << index;
    }

    private void setWritable(boolean invokeLater) {
        for (;;) {
            final int oldValue = unwritable;
            // 并位操作，修改第 0 位 bits 为 0
            final int newValue = oldValue & ~1;
            // CAS 设置 unwritable 为新值
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                // 若之前不可写，现在可写，触发 Channel WritabilityChanged 事件到 pipeline 中。
                if (oldValue != 0 && newValue == 0) {
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    private void setUnwritable(boolean invokeLater) {
        for (;;) {
            final int oldValue = unwritable;
            // 或位操作，修改第 0 位 bits 为 1
            final int newValue = oldValue | 1;
            // CAS 设置 unwritable 为新值
            if (UNWRITABLE_UPDATER.compareAndSet(this, oldValue, newValue)) {
                // 若之前可写，现在不可写，触发 Channel WritabilityChanged 事件到 pipeline 中。
                if (oldValue == 0 && newValue != 0) {
                    fireChannelWritabilityChanged(invokeLater);
                }
                break;
            }
        }
    }

    private void fireChannelWritabilityChanged(boolean invokeLater) {
        final ChannelPipeline pipeline = channel.pipeline();
        // 延迟执行，即提交 EventLoop 中触发 Channel WritabilityChanged 事件到 pipeline 中
        if (invokeLater) {
            Runnable task = fireChannelWritabilityChangedTask;
            if (task == null) {
                fireChannelWritabilityChangedTask = task = new Runnable() {
                    @Override
                    public void run() {
                        pipeline.fireChannelWritabilityChanged();
                    }
                };
            }
            channel.eventLoop().execute(task);
        // 直接触发 Channel WritabilityChanged 事件到 pipeline 中
        } else {
            pipeline.fireChannelWritabilityChanged();
        }
    }

    /**
     * Returns the number of flushed messages in this {@link ChannelOutboundBuffer}.
     */
    public int size() {
        return flushed;
    }

    /**
     * Returns {@code true} if there are flushed messages in this {@link ChannelOutboundBuffer} or {@code false}
     * otherwise.
     */
    public boolean isEmpty() {
        return flushed == 0;
    }

    void failFlushed(Throwable cause, boolean notify) {
        // 正在通知 flush 失败中，直接返回
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {
            return;
        }

        try {
            // 标记正在通知 flush 失败中
            inFail = true;
            // 循环，移除所有已 flush 的 Entry 节点们
            for (;;) {
                if (!remove0(cause, notify)) {
                    break;
                }
            }
        } finally {
            // 标记不在通知 flush 失败中
            inFail = false;
        }
    }

    void close(final Throwable cause, final boolean allowChannelOpen) {
        // 正在通知 flush 失败中
        if (inFail) {
            // 提交 EventLoop 的线程中，执行关闭
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause, allowChannelOpen);
                }
            });
            // 返回
            return;
        }

        // 标记正在通知 flush 失败中
        inFail = true;

        if (!allowChannelOpen && channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        try {
            // 从 unflushedEntry 节点，开始向下遍历
            Entry e = unflushedEntry;
            while (e != null) {
                // 减少 totalPendingSize 计数
                // Just decrease; do not trigger any events via decrementPendingOutboundBytes()
                int size = e.pendingSize;
                TOTAL_PENDING_SIZE_UPDATER.addAndGet(this, -size);

                if (!e.cancelled) {
                    // 释放消息( 数据 )相关的资源
                    ReferenceCountUtil.safeRelease(e.msg);
                    // 通知 Promise 执行失败
                    safeFail(e.promise, cause);
                }
                // 回收当前节点，并获得下一个 Entry 节点
                e = e.recycleAndGetNext();
            }
        } finally {
            // 标记在在通知 flush 失败中
            inFail = false;
        }

        // 清除 NIO ByteBuff 数组的缓存。
        clearNioBuffers();
    }

    void close(ClosedChannelException cause) {
        close(cause, false);
    }

    private static void safeSuccess(ChannelPromise promise) {
        // Only log if the given promise is not of type VoidChannelPromise as trySuccess(...) is expected to return
        // false.
        PromiseNotificationUtil.trySuccess(promise, null, promise instanceof VoidChannelPromise ? null : logger);
    }

    private static void safeFail(ChannelPromise promise, Throwable cause) {
        // Only log if the given promise is not of type VoidChannelPromise as tryFailure(...) is expected to return
        // false.
        PromiseNotificationUtil.tryFailure(promise, cause, promise instanceof VoidChannelPromise ? null : logger);
    }

    @Deprecated
    public void recycle() {
        // NOOP
    }

    public long totalPendingWriteBytes() {
        return totalPendingSize;
    }

    /**
     * Get how many bytes can be written until {@link #isWritable()} returns {@code false}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code false} then 0.
     */
    public long bytesBeforeUnwritable() {
        long bytes = channel.config().getWriteBufferHighWaterMark() - totalPendingSize;
        // If bytes is negative we know we are not writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? bytes : 0; // 判断 #isWritable() 的原因是，可能已经被设置不可写
        }
        return 0;
    }

    /**
     * Get how many bytes must be drained from the underlying buffer until {@link #isWritable()} returns {@code true}.
     * This quantity will always be non-negative. If {@link #isWritable()} is {@code true} then 0.
     */
    public long bytesBeforeWritable() {
        long bytes = totalPendingSize - channel.config().getWriteBufferLowWaterMark();
        // If bytes is negative we know we are writable, but if bytes is non-negative we have to check writability.
        // Note that totalPendingSize and isWritable() use different volatile variables that are not synchronized
        // together. totalPendingSize will be updated before isWritable().
        if (bytes > 0) {
            return isWritable() ? 0 : bytes; // 判断 #isWritable() 的原因是，可能已经被设置不可写
        }
        return 0;
    }

    /**
     * Call {@link MessageProcessor#processMessage(Object)} for each flushed message
     * in this {@link ChannelOutboundBuffer} until {@link MessageProcessor#processMessage(Object)}
     * returns {@code false} or there are no more flushed messages to process.
     */
    // TODO 1015 forEachFlushedMessage 在 `netty-transport-native-poll` 和 `netty-transport-native-kqueue` 中使用，在后续的文章解析。
    public void forEachFlushedMessage(MessageProcessor processor) throws Exception {
        if (processor == null) {
            throw new NullPointerException("processor");
        }

        Entry entry = flushedEntry;
        if (entry == null) {
            return;
        }

        do {
            if (!entry.cancelled) {
                if (!processor.processMessage(entry.msg)) {
                    return;
                }
            }
            entry = entry.next;
        } while (isFlushedEntry(entry));
    }

    private boolean isFlushedEntry(Entry e) {
        return e != null && e != unflushedEntry;
    }

    // TODO 1015 forEachFlushedMessage 在 `netty-transport-native-poll` 和 `netty-transport-native-kqueue` 中使用，在后续的文章解析。
    public interface MessageProcessor {

        /**
         * Will be called for each flushed message until it either there are no more flushed messages or this
         * method returns {@code false}.
         */
        boolean processMessage(Object msg) throws Exception;

    }

    /**
     * 每条写入的数据 Entry
     */
    static final class Entry {

        /**
         * Recycler 对象，用于重用 Entry 对象
         */
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
            @Override
            protected Entry newObject(Handle<Entry> handle) {
                return new Entry(handle);
            }
        };

        /**
         * Recycler 处理器
         */
        private final Handle<Entry> handle;
        /**
         * 下一条 Entry
         */
        Entry next;
        /**
         * 消息（数据）
         */
        Object msg;
        /**
         * {@link #msg} 转化的 NIO ByteBuffer 数组
         */
        ByteBuffer[] bufs;
        /**
         * {@link #msg} 转化的 NIO ByteBuffer 对象
         */
        ByteBuffer buf;
        /**
         * Promise 对象
         */
        ChannelPromise promise;
        /**
         * 已写入的字节数
         */
        long progress;
        /**
         * 长度，可读字节数数。
         */
        long total;
        /**
         * 每个 Entry 预计占用的内存大小，计算方式为消息( {@link #msg} )的字节数 + Entry 对象自身占用内存的大小。
         */
        int pendingSize;
        /**
         * {@link #msg} 转化的 NIO ByteBuffer 的数量。
         *
         * 当 = 1 时，使用 {@link #buf}
         * 当 > 1 时，使用 {@link #bufs}
         */
        int count = -1;
        /**
         * 是否取消写入对端
         */
        boolean cancelled;

        private Entry(Handle<Entry> handle) {
            this.handle = handle;
        }

        static Entry newInstance(Object msg, int size, long total, ChannelPromise promise) {
            // 通过 Recycler 重用对象
            Entry entry = RECYCLER.get();
            // 初始化属性
            entry.msg = msg;
            entry.pendingSize = size + CHANNEL_OUTBOUND_BUFFER_ENTRY_OVERHEAD;
            entry.total = total;
            entry.promise = promise;
            return entry;
        }

        int cancel() {
            if (!cancelled) {
                // 标记取消
                cancelled = true;
                int pSize = pendingSize;

                // 释放消息( 数据 )相关的资源
                // release message and replace with an empty buffer
                ReferenceCountUtil.safeRelease(msg);
                // 设置为空 ByteBuf
                msg = Unpooled.EMPTY_BUFFER;

                // 置空属性
                pendingSize = 0;
                total = 0;
                progress = 0;
                bufs = null;
                buf = null;

                // 返回 pSize
                return pSize;
            }
            return 0;
        }

        void recycle() {
            // 重置属性
            next = null;
            bufs = null;
            buf = null;
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = -1;
            cancelled = false;
            // 回收 Entry 对象
            handle.recycle(this);
        }

        Entry recycleAndGetNext() {
            // 获得下一个 Entry 对象
            Entry next = this.next;
            // 回收当前 Entry 对象
            recycle();
            // 返回下一个 Entry 对象
            return next;
        }

    }
}
