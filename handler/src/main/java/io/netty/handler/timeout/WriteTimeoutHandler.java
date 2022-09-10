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
package io.netty.handler.timeout;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Raises a {@link WriteTimeoutException} when a write operation cannot finish in a certain period of time.
 *
 * <pre>
 * // The connection is closed when a write operation cannot finish in 30 seconds.
 *
 * public class MyChannelInitializer extends {@link ChannelInitializer}&lt;{@link Channel}&gt; {
 *     public void initChannel({@link Channel} channel) {
 *         channel.pipeline().addLast("writeTimeoutHandler", new {@link WriteTimeoutHandler}(30);
 *         channel.pipeline().addLast("myHandler", new MyHandler());
 *     }
 * }
 *
 * // Handler should handle the {@link WriteTimeoutException}.
 * public class MyHandler extends {@link ChannelDuplexHandler} {
 *     {@code @Override}
 *     public void exceptionCaught({@link ChannelHandlerContext} ctx, {@link Throwable} cause)
 *             throws {@link Exception} {
 *         if (cause instanceof {@link WriteTimeoutException}) {
 *             // do something
 *         } else {
 *             super.exceptionCaught(ctx, cause);
 *         }
 *     }
 * }
 *
 * {@link ServerBootstrap} bootstrap = ...;
 * ...
 * bootstrap.childHandler(new MyChannelInitializer());
 * ...
 * </pre>
 * @see ReadTimeoutHandler
 * @see IdleStateHandler
 */
public class WriteTimeoutHandler extends ChannelOutboundHandlerAdapter {

    /**
     * 最小的超时时间，单位：纳秒
     */
    private static final long MIN_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * 超时时间，单位：纳秒
     */
    private final long timeoutNanos;

    /**
     * WriteTimeoutTask 双向链表。
     *
     * lastTask 为链表的尾节点
     *
     * A doubly-linked list to track all WriteTimeoutTasks
     */
    private WriteTimeoutTask lastTask;

    /**
     * Channel 是否关闭
     */
    private boolean closed;

    /**
     * Creates a new instance.
     *
     * @param timeoutSeconds
     *        write timeout in seconds
     */
    public WriteTimeoutHandler(int timeoutSeconds) {
        this(timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Creates a new instance.
     *
     * @param timeout
     *        write timeout
     * @param unit
     *        the {@link TimeUnit} of {@code timeout}
     */
    public WriteTimeoutHandler(long timeout, TimeUnit unit) {
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        if (timeout <= 0) {
            timeoutNanos = 0;
        } else {
            timeoutNanos = Math.max(unit.toNanos(timeout), MIN_TIMEOUT_NANOS); // 保证大于等于 MIN_TIMEOUT_NANOS
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (timeoutNanos > 0) {
            // 如果 promise 是 VoidPromise ，则包装成非 VoidPromise ，为了后续的回调。
            promise = promise.unvoid();
            // 创建定时任务
            scheduleTimeout(ctx, promise);
        }
        // 写入
        ctx.write(msg, promise);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        WriteTimeoutTask task = lastTask;
        // 置空 lastTask
        lastTask = null;
        // 循环移除，知道为空
        while (task != null) {
            // 取消当前任务的定时任务
            task.scheduledFuture.cancel(false);
            // 记录前一个任务
            WriteTimeoutTask prev = task.prev;
            // 置空当前任务的前后节点
            task.prev = null;
            task.next = null;
            // 跳到前一个任务
            task = prev;
        }
    }

    private void scheduleTimeout(final ChannelHandlerContext ctx, final ChannelPromise promise) {
        // Schedule a timeout.
        // 创建 WriteTimeoutTask 任务
        final WriteTimeoutTask task = new WriteTimeoutTask(ctx, promise);
        // 定时任务
        task.scheduledFuture = ctx.executor().schedule(task, timeoutNanos, TimeUnit.NANOSECONDS);

        if (!task.scheduledFuture.isDone()) {
            // 添加到链表
            addWriteTimeoutTask(task);

            // Cancel the scheduled timeout if the flush promise is complete.
            // 将 task 作为监听器，添加到 promise 中。在写入完成后，可以移除该定时任务
            promise.addListener(task);
        }
    }

    private void addWriteTimeoutTask(WriteTimeoutTask task) {
        // 添加到链表的尾节点
        if (lastTask != null) {
            lastTask.next = task;
            task.prev = lastTask;
        }
        // 修改 lastTask 为当前任务
        lastTask = task;
    }

    private void removeWriteTimeoutTask(WriteTimeoutTask task) {
        // 从双向链表中，移除自己
        if (task == lastTask) { // 尾节点
            // task is the tail of list
            assert task.next == null;
            lastTask = lastTask.prev;
            if (lastTask != null) {
                lastTask.next = null;
            }
        } else if (task.prev == null && task.next == null) { // 已经被移除
            // Since task is not lastTask, then it has been removed or not been added.
            return;
        } else if (task.prev == null) { // 头节点
            // task is the head of list and the list has at least 2 nodes
            task.next.prev = null;
        } else { // 中间的节点
            task.prev.next = task.next;
            task.next.prev = task.prev;
        }
        // 重置 task 前后节点为空
        task.prev = null;
        task.next = null;
    }

    /**
     * Is called when a write timeout was detected
     */
    protected void writeTimedOut(ChannelHandlerContext ctx) throws Exception {
        if (!closed) {
            // 触发 Exception Caught 事件到 pipeline 中，异常为 WriteTimeoutException
            ctx.fireExceptionCaught(WriteTimeoutException.INSTANCE);
            // 关闭 Channel 通道
            ctx.close();
            // 标记 Channel 为已关闭
            closed = true;
        }
    }

    private final class WriteTimeoutTask implements Runnable, ChannelFutureListener {

        private final ChannelHandlerContext ctx;
        /**
         * 写入任务的 Promise 对象
         */
        private final ChannelPromise promise;

        // WriteTimeoutTask is also a node of a doubly-linked list
        /**
         * 前一个 task
         */
        WriteTimeoutTask prev;
        /**
         * 后一个 task
         */
        WriteTimeoutTask next;
        /**
         * 定时任务
         */
        ScheduledFuture<?> scheduledFuture;

        WriteTimeoutTask(ChannelHandlerContext ctx, ChannelPromise promise) {
            this.ctx = ctx;
            this.promise = promise;
        }

        @Override
        public void run() {
            // Was not written yet so issue a write timeout
            // The promise itself will be failed with a ClosedChannelException once the close() was issued
            // See https://github.com/netty/netty/issues/2159
            if (!promise.isDone()) { // 未完成，说明写入超时
                try {
                    // 写入超时，关闭 Channel 通道
                    writeTimedOut(ctx);
                } catch (Throwable t) {
                    // 触发 Exception Caught 事件到 pipeline 中
                    ctx.fireExceptionCaught(t);
                }
            }
            // 移除出链表
            removeWriteTimeoutTask(this);
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            // scheduledFuture has already be set when reaching here
            // 取消定时任务
            scheduledFuture.cancel(false);
            // 移除出链表
            removeWriteTimeoutTask(this);
        }
    }

}
