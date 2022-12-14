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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.util.internal.StringUtil;

import java.util.List;

/**
 * {@link ChannelInboundHandlerAdapter} which decodes bytes in a stream-like fashion from one {@link ByteBuf} to an
 * other Message type.
 *
 * For example here is an implementation which reads all readable bytes from
 * the input {@link ByteBuf} and create a new {@link ByteBuf}.
 *
 * <pre>
 *     public class SquareDecoder extends {@link ByteToMessageDecoder} {
 *         {@code @Override}
 *         public void decode({@link ChannelHandlerContext} ctx, {@link ByteBuf} in, List&lt;Object&gt; out)
 *                 throws {@link Exception} {
 *             out.add(in.readBytes(in.readableBytes()));
 *         }
 *     }
 * </pre>
 *
 * <h3>Frame detection</h3>
 * <p>
 * Generally frame detection should be handled earlier in the pipeline by adding a
 * {@link DelimiterBasedFrameDecoder}, {@link FixedLengthFrameDecoder}, {@link LengthFieldBasedFrameDecoder},
 * or {@link LineBasedFrameDecoder}.
 * <p>
 * If a custom frame decoder is required, then one needs to be careful when implementing
 * one with {@link ByteToMessageDecoder}. Ensure there are enough bytes in the buffer for a
 * complete frame by checking {@link ByteBuf#readableBytes()}. If there are not enough bytes
 * for a complete frame, return without modifying the reader index to allow more bytes to arrive.
 * <p>
 * To check for complete frames without modifying the reader index, use methods like {@link ByteBuf#getInt(int)}.
 * One <strong>MUST</strong> use the reader index when using methods like {@link ByteBuf#getInt(int)}.
 * For example calling <tt>in.getInt(0)</tt> is assuming the frame starts at the beginning of the buffer, which
 * is not always the case. Use <tt>in.getInt(in.readerIndex())</tt> instead.
 * <h3>Pitfalls</h3>
 * <p>
 * Be aware that sub-classes of {@link ByteToMessageDecoder} <strong>MUST NOT</strong>
 * annotated with {@link @Sharable}.
 * <p>
 * Some methods such as {@link ByteBuf#readBytes(int)} will cause a memory leak if the returned buffer
 * is not released or added to the <tt>out</tt> {@link List}. Use derived buffers like {@link ByteBuf#readSlice(int)}
 * to avoid leaking memory.
 */
public abstract class ByteToMessageDecoder extends ChannelInboundHandlerAdapter {

    /**
     * Cumulate {@link ByteBuf}s by merge them into one {@link ByteBuf}'s, using memory copies.
     */
    public static final Cumulator MERGE_CUMULATOR = new Cumulator() {

        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            final ByteBuf buffer;
            if (cumulation.writerIndex() > cumulation.maxCapacity() - in.readableBytes() // ?????????????????????????????????
                    || cumulation.refCnt() > 1 // ???????????? 1 ???????????????????????? slice().retain() ??? duplicate().retain() ???refCnt?????????????????? 1 ???
                                               // ???????????????????????????????????????ByteBuf?????????????????????????????????ByteBuf?????????????????????
                    || cumulation.isReadOnly()) { // ????????????????????????????????????????????????
                // Expand cumulation (by replace it) when either there is not more room in the buffer
                // or if the refCnt is greater then 1 which may happen when the user use slice().retain() or
                // duplicate().retain() or if its read-only.
                //
                // See:
                // - https://github.com/netty/netty/issues/2327
                // - https://github.com/netty/netty/issues/1764
                // ????????????????????? buffer
                buffer = expandCumulation(alloc, cumulation, in.readableBytes());
            } else {
                // ???????????? buffer
                buffer = cumulation;
            }
            // ?????? in ??? buffer ???
            buffer.writeBytes(in);
            // ???????????? in
            in.release();
            // ?????? buffer
            return buffer;
        }

    };

    /**
     * Cumulate {@link ByteBuf}s by add them to a {@link CompositeByteBuf} and so do no memory copy whenever possible.
     * Be aware that {@link CompositeByteBuf} use a more complex indexing implementation so depending on your use-case
     * and the decoder implementation this may be slower then just use the {@link #MERGE_CUMULATOR}.
     *
     * ?????? MERGE_CUMULATOR ?????????
     *
     * ???????????????????????????
     * ??????????????????????????????????????????????????????????????????????????? MERGE_CUMULATOR
     */
    public static final Cumulator COMPOSITE_CUMULATOR = new Cumulator() {

        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            ByteBuf buffer;
            // ??? MERGE_CUMULATOR ???????????????
            if (cumulation.refCnt() > 1) {
                // Expand cumulation (by replace it) when the refCnt is greater then 1 which may happen when the user
                // use slice().retain() or duplicate().retain().
                //
                // See:
                // - https://github.com/netty/netty/issues/2327
                // - https://github.com/netty/netty/issues/1764
                buffer = expandCumulation(alloc, cumulation, in.readableBytes());
                buffer.writeBytes(in);
                in.release();
            } else {
                CompositeByteBuf composite;
                // ????????? CompositeByteBuf ?????????????????????
                if (cumulation instanceof CompositeByteBuf) {
                    composite = (CompositeByteBuf) cumulation;
                // ???????????? CompositeByteBuf ????????????????????????????????????
                } else {
                    composite = alloc.compositeBuffer(Integer.MAX_VALUE);
                    composite.addComponent(true, cumulation);
                }
                // ?????? in ??? composite ???
                composite.addComponent(true, in);
                // ????????? buffer
                buffer = composite;
            }
            // ?????? buffer
            return buffer;
        }

    };

    private static final byte STATE_INIT = 0;
    private static final byte STATE_CALLING_CHILD_DECODE = 1;
    private static final byte STATE_HANDLER_REMOVED_PENDING = 2;

    /**
     * ???????????? ByteBuf ??????
     */
    ByteBuf cumulation;
    /**
     * ?????????
     */
    private Cumulator cumulator = MERGE_CUMULATOR;
    /**
     * ????????????????????????????????????????????? false ???
     *
     * ?????????????????? true ????????????Socks4ClientDecoder
     *
     * @see #callDecode(ChannelHandlerContext, ByteBuf, List)
     */
    private boolean singleDecode;
    /**
     * ????????????????????????
     *
     * WasNull ?????????????????????????????????
     *
     * @see #channelReadComplete(ChannelHandlerContext)
     */
    private boolean decodeWasNull;
    /**
     * ???????????????????????? {@link #cumulation} ??????
     */
    private boolean first;
    /**
     * A bitmask where the bits are defined as
     * <ul>
     *     <li>{@link #STATE_INIT}</li>
     *     <li>{@link #STATE_CALLING_CHILD_DECODE}</li>
     *     <li>{@link #STATE_HANDLER_REMOVED_PENDING}</li>
     * </ul>
     *
     * ????????????
     *
     * 0 - ?????????
     * 1 - ?????? {@link #decode(ChannelHandlerContext, ByteBuf, List)} ??????????????????????????????
     * 2 - ????????????
     */
    private byte decodeState = STATE_INIT;
    /**
     * ??????????????????
     */
    private int discardAfterReads = 16;
    /**
     * ??????????????????
     *
     * ????????? {@link #discardAfterReads} ????????????????????????????????????????????????????????????????????? OOM
     */
    private int numReads;

    protected ByteToMessageDecoder() {
        // ?????????????????????
        ensureNotSharable();
    }

    /**
     * If set then only one message is decoded on each {@link #channelRead(ChannelHandlerContext, Object)}
     * call. This may be useful if you need to do some protocol upgrade and want to make sure nothing is mixed up.
     *
     * Default is {@code false} as this has performance impacts.
     */
    public void setSingleDecode(boolean singleDecode) {
        this.singleDecode = singleDecode;
    }

    /**
     * If {@code true} then only one message is decoded on each
     * {@link #channelRead(ChannelHandlerContext, Object)} call.
     *
     * Default is {@code false} as this has performance impacts.
     */
    public boolean isSingleDecode() {
        return singleDecode;
    }

    /**
     * Set the {@link Cumulator} to use for cumulate the received {@link ByteBuf}s.
     */
    public void setCumulator(Cumulator cumulator) {
        if (cumulator == null) {
            throw new NullPointerException("cumulator");
        }
        this.cumulator = cumulator;
    }

    /**
     * Set the number of reads after which {@link ByteBuf#discardSomeReadBytes()} are called and so free up memory.
     * The default is {@code 16}.
     */
    public void setDiscardAfterReads(int discardAfterReads) {
        if (discardAfterReads <= 0) {
            throw new IllegalArgumentException("discardAfterReads must be > 0");
        }
        this.discardAfterReads = discardAfterReads;
    }

    /**
     * Returns the actual number of readable bytes in the internal cumulative
     * buffer of this decoder. You usually do not need to rely on this value
     * to write a decoder. Use it only when you must use it at your own risk.
     * This method is a shortcut to {@link #internalBuffer() internalBuffer().readableBytes()}.
     */
    protected int actualReadableBytes() {
        return internalBuffer().readableBytes();
    }

    /**
     * Returns the internal cumulative buffer of this decoder. You usually
     * do not need to access the internal buffer directly to write a decoder.
     * Use it only when you must use it at your own risk.
     */
    protected ByteBuf internalBuffer() {
        if (cumulation != null) {
            return cumulation;
        } else {
            return Unpooled.EMPTY_BUFFER;
        }
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        // ???????????? STATE_CALLING_CHILD_DECODE ????????????????????? STATE_HANDLER_REMOVED_PENDING
        if (decodeState == STATE_CALLING_CHILD_DECODE) {
            decodeState = STATE_HANDLER_REMOVED_PENDING;
            return; // ???????????????????????? `#decodeRemovalReentryProtection(...)` ?????????????????????
        }
        ByteBuf buf = cumulation;
        if (buf != null) {
            // ?????? cumulation
            // Directly set this to null so we are sure we not access it in any other method here anymore.
            cumulation = null;

            int readable = buf.readableBytes();
            // ???????????????
            if (readable > 0) {
                // ?????????????????????????????? buf
                ByteBuf bytes = buf.readBytes(readable);
                buf.release();
                // ?????? Channel Read ??????????????????
                ctx.fireChannelRead(bytes);
            // ???????????????
            } else {
                // ?????? buf
                buf.release();
            }

            // ?????? numReads
            numReads = 0;
            // ?????? Channel ReadComplete ??????????????????
            ctx.fireChannelReadComplete();
        }
        // ??????????????????
        handlerRemoved0(ctx);
    }

    /**
     * Gets called after the {@link ByteToMessageDecoder} was removed from the actual context and it doesn't handle
     * events anymore.
     */
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception { }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            // ?????? CodecOutputList ??????
            CodecOutputList out = CodecOutputList.newInstance();
            try {
                ByteBuf data = (ByteBuf) msg;
                // ??????????????????
                first = cumulation == null;
                // ????????????????????????????????? data
                if (first) {
                    cumulation = data;
                // ??????????????????????????? data ???????????? cumulation ???
                } else {
                    cumulation = cumulator.cumulate(ctx.alloc(), cumulation, data);
                }
                // ????????????
                callDecode(ctx, cumulation, out);
            } catch (DecoderException e) {
                throw e; // ????????????
            } catch (Exception e) {
                throw new DecoderException(e); // ????????? DecoderException ???????????????
            } finally {
                // cumulation ????????????????????????????????????????????????
                if (cumulation != null && !cumulation.isReadable()) {
                    numReads = 0; // ?????? numReads ??????
                    cumulation.release(); // ?????? cumulation
                    cumulation = null; // ?????? cumulation
                // ?????????????????? discardAfterReads ??????????????????????????????
                } else if (++ numReads >= discardAfterReads) {
                    // We did enough reads already try to discard some bytes so we not risk to see a OOME.
                    // See https://github.com/netty/netty/issues/4275
                    numReads = 0; // ?????? numReads ??????
                    discardSomeReadBytes(); // ?????????????????????
                }

                // ?????????????????????
                int size = out.size();
                // ?????????????????????
                decodeWasNull = !out.insertSinceRecycled();

                // ?????? Channel Read ??????????????????????????????
                fireChannelRead(ctx, out, size);

                // ?????? CodecOutputList ??????
                out.recycle();
            }
        } else {
            // ?????? Channel Read ??????
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * Get {@code numElements} out of the {@link List} and forward these through the pipeline.
     */
    static void fireChannelRead(ChannelHandlerContext ctx, List<Object> msgs, int numElements) {
        if (msgs instanceof CodecOutputList) { // ????????? CodecOutputList ?????????????????????
            fireChannelRead(ctx, (CodecOutputList) msgs, numElements);
        } else {
            for (int i = 0; i < numElements; i++) {
                ctx.fireChannelRead(msgs.get(i));
            }
        }
    }

    /**
     * Get {@code numElements} out of the {@link CodecOutputList} and forward these through the pipeline.
     */
    static void fireChannelRead(ChannelHandlerContext ctx, CodecOutputList msgs, int numElements) {
        for (int i = 0; i < numElements; i ++) {
            ctx.fireChannelRead(msgs.getUnsafe(i)); // getUnsafe ?????????????????????????????????????????????????????????
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // ?????? numReads
        numReads = 0;
        // ?????????????????????
        discardSomeReadBytes();
        // ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        if (decodeWasNull) {
            decodeWasNull = false; // ?????? decodeWasNull
            if (!ctx.channel().config().isAutoRead()) {
                ctx.read();
            }
        }
        // ?????? Channel ReadComplete ????????????????????????
        ctx.fireChannelReadComplete();
    }

    protected final void discardSomeReadBytes() {
        if (cumulation != null && !first
                && cumulation.refCnt() == 1) { // ????????????????????? slice().retain() ??? duplicate().retain() ??? refCnt > 1 ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
            // discard some bytes if possible to make more room in the
            // buffer but only if the refCnt == 1  as otherwise the user may have
            // used slice().retain() or duplicate().retain().
            //
            // See:
            // - https://github.com/netty/netty/issues/2327
            // - https://github.com/netty/netty/issues/1764
            // ????????????
            cumulation.discardSomeReadBytes();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        channelInputClosed(ctx, true);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof ChannelInputShutdownEvent) {
            // The decodeLast method is invoked when a channelInactive event is encountered.
            // This method is responsible for ending requests in some situations and must be called
            // when the input has been shutdown.
            channelInputClosed(ctx, false);
        }
        // ???????????? evt ??????????????????
        super.userEventTriggered(ctx, evt);
    }

    private void channelInputClosed(ChannelHandlerContext ctx, boolean callChannelInactive) throws Exception {
        // ?????? CodecOutputList ??????
        CodecOutputList out = CodecOutputList.newInstance();
        try {
            // ??? Channel ???????????????????????????????????????????????????
            channelInputClosed(ctx, out);
        } catch (DecoderException e) {
            throw e;
        } catch (Exception e) {
            throw new DecoderException(e);
        } finally {
            try {
                // ?????? cumulation
                if (cumulation != null) {
                    cumulation.release();
                    cumulation = null;
                }
                int size = out.size();
                // ?????? Channel Read ????????????????????????????????????????????????
                fireChannelRead(ctx, out, size);
                // ???????????????????????????????????? Channel ReadComplete ???????????????????????????
                if (size > 0) {
                    // Something was read, call fireChannelReadComplete()
                    ctx.fireChannelReadComplete();
                }
                // ??????????????????????????? `#channelInactive(...)` ???????????? Channel Inactive ????????????????????????
                if (callChannelInactive) {
                    ctx.fireChannelInactive();
                }
            } finally {
                // ?????? CodecOutputList ??????
                // Recycle in all cases
                out.recycle();
            }
        }
    }

    /**
     * Called when the input of the channel was closed which may be because it changed to inactive or because of
     * {@link ChannelInputShutdownEvent}.
     */
    void channelInputClosed(ChannelHandlerContext ctx, List<Object> out) throws Exception {
        if (cumulation != null) {
            // ????????????
            callDecode(ctx, cumulation, out);
            // ???????????????????????????
            decodeLast(ctx, cumulation, out);
        } else {
            // ???????????????????????????
            decodeLast(ctx, Unpooled.EMPTY_BUFFER, out);
        }
    }

    /**
     * Called once data should be decoded from the given {@link ByteBuf}. This method will call
     * {@link #decode(ChannelHandlerContext, ByteBuf, List)} as long as decoding should take place.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added
     */
    @SuppressWarnings("Duplicates")
    protected void callDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        try {
            // ??????????????????????????????
            while (in.isReadable()) {
                // ??????
                int outSize = out.size();
                // out ????????????????????????????????????????????????
                if (outSize > 0) {
                    // ?????? Channel Read ??????????????????????????????
                    fireChannelRead(ctx, out, outSize);
                    // ??????
                    out.clear();

                    // ????????????????????? Handler ??????????????? in ???????????????
                    // Check if this handler was removed before continuing with decoding.
                    // If it was removed, it is not safe to continue to operate on the buffer.
                    //
                    // See:
                    // - https://github.com/netty/netty/issues/4635
                    if (ctx.isRemoved()) {
                        break;
                    }
                    outSize = 0;
                }

                // ???????????????????????????
                int oldInputLength = in.readableBytes();

                // ????????????????????? Handler ???????????????????????????????????????????????????
                decodeRemovalReentryProtection(ctx, in, out);

                // ????????????????????? Handler ??????????????? in ???????????????
                // Check if this handler was removed before continuing the loop.
                // If it was removed, it is not safe to continue to operate on the buffer.
                //
                // See https://github.com/netty/netty/issues/1664
                if (ctx.isRemoved()) {
                    break;
                }

                // ???????????? `out.size() == 0` ?????????????????????????????? `outSize > 0` ???????????????????????? out ???
                if (outSize == out.size()) {
                    // ??????????????????????????????????????????
                    if (oldInputLength == in.readableBytes()) {
                        break;
                    // ?????????????????????????????????????????????
                    } else {
                        continue;
                    }
                }

                // ???????????????????????????????????????????????????????????? DecoderException ??????????????????????????????
                if (oldInputLength == in.readableBytes()) {
                    throw new DecoderException(StringUtil.simpleClassName(getClass()) + ".decode() did not read anything but decoded a message.");
                }

                // ???????????? singleDecode ???????????????????????????????????????
                if (isSingleDecode()) {
                    break;
                }
            }
        } catch (DecoderException e) {
            throw e;
        } catch (Exception cause) {
            throw new DecoderException(cause);
        }
    }

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
     * {@link ByteBuf} has nothing to read when return from this method or till nothing was read from the input
     * {@link ByteBuf}.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added
     * @throws Exception    is thrown if an error occurs
     */
    protected abstract void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception;

    /**
     * Decode the from one {@link ByteBuf} to an other. This method will be called till either the input
     * {@link ByteBuf} has nothing to read when return from this method or till nothing was read from the input
     * {@link ByteBuf}.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in            the {@link ByteBuf} from which to read data
     * @param out           the {@link List} to which decoded messages should be added
     * @throws Exception    is thrown if an error occurs
     */
    final void decodeRemovalReentryProtection(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // ??????????????? STATE_CALLING_CHILD_DECODE
        decodeState = STATE_CALLING_CHILD_DECODE;
        try {
            // ????????????
            decode(ctx, in, out);
        } finally {
            // ????????????????????????
            boolean removePending = decodeState == STATE_HANDLER_REMOVED_PENDING;
            // ??????????????? STATE_INIT
            decodeState = STATE_INIT;
            // ???????????? Handler
            if (removePending) {
                handlerRemoved(ctx);
            }
        }
    }

    /**
     * Is called one last time when the {@link ChannelHandlerContext} goes in-active. Which means the
     * {@link #channelInactive(ChannelHandlerContext)} was triggered.
     *
     * By default this will just call {@link #decode(ChannelHandlerContext, ByteBuf, List)} but sub-classes may
     * override this for some special cleanup operation.
     */
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.isReadable()) {
            // Only call decode() if there is something left in the buffer to decode.
            // See https://github.com/netty/netty/issues/4386
            decodeRemovalReentryProtection(ctx, in, out);
        }
    }

    static ByteBuf expandCumulation(ByteBufAllocator alloc, ByteBuf cumulation, int readable) {
        // ???????????? ByteBuf ??????
        ByteBuf oldCumulation = cumulation;
        // ???????????? ByteBuf ??????
        cumulation = alloc.buffer(oldCumulation.readableBytes() + readable);
        // ????????????????????????????????? ByteBuf ??????
        cumulation.writeBytes(oldCumulation);
        // ???????????? ByteBuf ??????
        oldCumulation.release();
        // ???????????? ByteBuf ??????
        return cumulation;
    }

    /**
     * ByteBuf ???????????????
     *
     * Cumulate {@link ByteBuf}s.
     */
    public interface Cumulator {

        /**
         * Cumulate the given {@link ByteBuf}s and return the {@link ByteBuf} that holds the cumulated bytes.
         * The implementation is responsible to correctly handle the life-cycle of the given {@link ByteBuf}s and so
         * call {@link ByteBuf#release()} if a {@link ByteBuf} is fully consumed.
         *
         * @param alloc ByteBuf ?????????
         * @param cumulation ByteBuf ??????????????????
         * @param in ????????????( ?????? ) ByteBuf
         * @return ByteBuf ??????????????????
         */
        ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in);

    }

}
