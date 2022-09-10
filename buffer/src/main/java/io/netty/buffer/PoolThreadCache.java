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

package io.netty.buffer;


import io.netty.buffer.PoolArena.SizeClass;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.ByteBuffer;
import java.util.Queue;

/**
 * 内存分配的线程缓存
 *
 * Acts a Thread cache for allocations. This implementation is moduled after
 * <a href="http://people.freebsd.org/~jasone/jemalloc/bsdcan2006/jemalloc.pdf">jemalloc</a> and the descripted
 * technics of
 * <a href="https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919">
 * Scalable memory allocation using jemalloc</a>.
 */
final class PoolThreadCache {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PoolThreadCache.class);

    /**
     * 对应的 Heap PoolArena 对象
     */
    final PoolArena<byte[]> heapArena;
    /**
     * 对应的 Direct PoolArena 对象
     */
    final PoolArena<ByteBuffer> directArena;

    // Hold the caches for the different size classes, which are tiny, small and normal.
    /**
     * Heap 类型的 tiny Subpage 内存块缓存数组
     */
    private final MemoryRegionCache<byte[]>[] tinySubPageHeapCaches;
    /**
     * Heap 类型的 small Subpage 内存块缓存数组
     */
    private final MemoryRegionCache<byte[]>[] smallSubPageHeapCaches;
    /**
     * Heap 类型的 normal 内存块缓存数组
     */
    private final MemoryRegionCache<byte[]>[] normalHeapCaches;
    /**
     * Direct 类型的 tiny Subpage 内存块缓存数组
     */
    private final MemoryRegionCache<ByteBuffer>[] tinySubPageDirectCaches;
    /**
     * Direct 类型的 small Subpage 内存块缓存数组
     */
    private final MemoryRegionCache<ByteBuffer>[] smallSubPageDirectCaches;
    /**
     * Direct 类型的 normal 内存块缓存数组
     */
    private final MemoryRegionCache<ByteBuffer>[] normalDirectCaches;

    // Used for bitshifting when calculate the index of normal caches later
    /**
     * 用于计算请求分配的 normal 类型的内存块，在 {@link #normalDirectCaches} 数组中的位置
     *
     * 默认为 log2(pageSize) = log2(8192) = 13
     */
    private final int numShiftsNormalDirect;
    /**
     * 用于计算请求分配的 normal 类型的内存块，在 {@link #normalHeapCaches} 数组中的位置
     *
     * 默认为 log2(pageSize) = log2(8192) = 13
     */
    private final int numShiftsNormalHeap;

    /**
     * 分配次数
     */
    private int allocations;
    /**
     * {@link #allocations} 到达该阀值，释放缓存。
     *
     * 默认为 8192 次
     *
     * @see #free()
     */
    private final int freeSweepAllocationThreshold;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolThreadCache(PoolArena<byte[]> heapArena, PoolArena<ByteBuffer> directArena,
                    int tinyCacheSize, int smallCacheSize, int normalCacheSize,
                    int maxCachedBufferCapacity, int freeSweepAllocationThreshold) {
        if (maxCachedBufferCapacity < 0) {
            throw new IllegalArgumentException("maxCachedBufferCapacity: "
                    + maxCachedBufferCapacity + " (expected: >= 0)");
        }
        this.freeSweepAllocationThreshold = freeSweepAllocationThreshold;
        this.heapArena = heapArena;
        this.directArena = directArena;

        // 初始化 Direct 类型的内存块缓存
        if (directArena != null) {
            // 创建 tinySubPageDirectCaches
            tinySubPageDirectCaches = createSubPageCaches(tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
            // 创建 smallSubPageDirectCaches
            smallSubPageDirectCaches = createSubPageCaches(smallCacheSize, directArena.numSmallSubpagePools, SizeClass.Small);

            // 计算 numShiftsNormalDirect
            numShiftsNormalDirect = log2(directArena.pageSize);
            // 创建 normalDirectCaches
            normalDirectCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, directArena);

            // 增加 directArena 的线程引用计数
            directArena.numThreadCaches.getAndIncrement();
        } else {
            // No directArea is configured so just null out all caches
            tinySubPageDirectCaches = null;
            smallSubPageDirectCaches = null;
            normalDirectCaches = null;
            numShiftsNormalDirect = -1;
        }
        // 初始化 Heap 类型的内存块缓存。同上面部分。
        if (heapArena != null) {
            // Create the caches for the heap allocations
            tinySubPageHeapCaches = createSubPageCaches(tinyCacheSize, PoolArena.numTinySubpagePools, SizeClass.Tiny);
            smallSubPageHeapCaches = createSubPageCaches(smallCacheSize, heapArena.numSmallSubpagePools, SizeClass.Small);

            numShiftsNormalHeap = log2(heapArena.pageSize);
            normalHeapCaches = createNormalCaches(normalCacheSize, maxCachedBufferCapacity, heapArena);

            heapArena.numThreadCaches.getAndIncrement();
        } else {
            // No heapArea is configured so just null out all caches
            tinySubPageHeapCaches = null;
            smallSubPageHeapCaches = null;
            normalHeapCaches = null;
            numShiftsNormalHeap = -1;
        }

        // 校验参数，保证 PoolThreadCache 可缓存内存块。
        // Only check if there are caches in use.
        if ((tinySubPageDirectCaches != null || smallSubPageDirectCaches != null || normalDirectCaches != null
                || tinySubPageHeapCaches != null || smallSubPageHeapCaches != null || normalHeapCaches != null)
                && freeSweepAllocationThreshold < 1) {
            throw new IllegalArgumentException("freeSweepAllocationThreshold: " + freeSweepAllocationThreshold + " (expected: > 0)");
        }
    }

    // tiny 类型，默认 cacheSize = PooledByteBufAllocator.DEFAULT_TINY_CACHE_SIZE = 512 , numCaches = PoolArena.numTinySubpagePools = 512 >>> 4 = 32
    // small 类型，默认 cacheSize = PooledByteBufAllocator.DEFAULT_SMALL_CACHE_SIZE = 256 , numCaches = pageSize - 9 = 13 - 9 = 4
    private static <T> MemoryRegionCache<T>[] createSubPageCaches(int cacheSize, int numCaches, SizeClass sizeClass) {
        if (cacheSize > 0 && numCaches > 0) {
            @SuppressWarnings("unchecked")
            MemoryRegionCache<T>[] cache = new MemoryRegionCache[numCaches];
            for (int i = 0; i < cache.length; i++) {
                // TODO: maybe use cacheSize / cache.length
                cache[i] = new SubPageMemoryRegionCache<T>(cacheSize, sizeClass);
            }
            return cache;
        } else {
            return null;
        }
    }

    // normal 类型，默认 cacheSize = PooledByteBufAllocator.DEFAULT_NORMAL_CACHE_SIZE = 64 , maxCachedBufferCapacity = PoolArena.DEFAULT_MAX_CACHED_BUFFER_CAPACITY = 32 * 1024 = 32KB
    private static <T> MemoryRegionCache<T>[] createNormalCaches(int cacheSize, int maxCachedBufferCapacity, PoolArena<T> area) {
        if (cacheSize > 0 && maxCachedBufferCapacity > 0) {
            // 计算数组大小
            int max = Math.min(area.chunkSize, maxCachedBufferCapacity);
            int arraySize = Math.max(1, log2(max / area.pageSize) + 1);

            @SuppressWarnings("unchecked")
            MemoryRegionCache<T>[] cache = new MemoryRegionCache[arraySize];
            for (int i = 0; i < cache.length; i++) {
                cache[i] = new NormalMemoryRegionCache<T>(cacheSize);
            }
            return cache;
        } else {
            return null;
        }
    }

    private static int log2(int val) {
        int res = 0;
        while (val > 1) {
            val >>= 1;
            res++;
        }
        return res;
    }

    /**
     * Try to allocate a tiny buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateTiny(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForTiny(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateSmall(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForSmall(area, normCapacity), buf, reqCapacity);
    }

    /**
     * Try to allocate a small buffer out of the cache. Returns {@code true} if successful {@code false} otherwise
     */
    boolean allocateNormal(PoolArena<?> area, PooledByteBuf<?> buf, int reqCapacity, int normCapacity) {
        return allocate(cacheForNormal(area, normCapacity), buf, reqCapacity);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean allocate(MemoryRegionCache<?> cache, PooledByteBuf buf, int reqCapacity) {
        if (cache == null) {
            // no cache found so just return false here
            return false;
        }
        // 分配内存块，并初始化到 MemoryRegionCache 中
        boolean allocated = cache.allocate(buf, reqCapacity);
        // 到达阀值，整理缓存
        if (++ allocations >= freeSweepAllocationThreshold) {
            allocations = 0;
            trim();
        }
        // 返回是否分配成功
        return allocated;
    }

    /**
     * Add {@link PoolChunk} and {@code handle} to the cache if there is enough room.
     * Returns {@code true} if it fit into the cache {@code false} otherwise.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    boolean add(PoolArena<?> area, PoolChunk chunk, long handle, int normCapacity, SizeClass sizeClass) {
        // 获得对应的 MemoryRegionCache 对象
        MemoryRegionCache<?> cache = cache(area, normCapacity, sizeClass);
        if (cache == null) {
            return false;
        }
        // 添加到 MemoryRegionCache 内存块中
        return cache.add(chunk, handle);
    }

    private MemoryRegionCache<?> cache(PoolArena<?> area, int normCapacity, SizeClass sizeClass) {
        switch (sizeClass) {
        case Normal:
            return cacheForNormal(area, normCapacity);
        case Small:
            return cacheForSmall(area, normCapacity);
        case Tiny:
            return cacheForTiny(area, normCapacity);
        default:
            throw new Error();
        }
    }

    /// TODO: In the future when we move to Java9+ we should use java.lang.ref.Cleaner.
    @Override
    protected void finalize() throws Throwable {
        try {
            // 调用父 finalize
            super.finalize();
        } finally {
            // 清空缓存
            free();
        }
    }

    /**
     *  Should be called if the Thread that uses this cache is about to exist to release resources out of the cache
     */
    void free() {
        // 清空缓存
        int numFreed = free(tinySubPageDirectCaches) +
                free(smallSubPageDirectCaches) +
                free(normalDirectCaches) +
                free(tinySubPageHeapCaches) +
                free(smallSubPageHeapCaches) +
                free(normalHeapCaches);

        if (numFreed > 0 && logger.isDebugEnabled()) {
            logger.debug("Freed {} thread-local buffer(s) from thread: {}", numFreed, Thread.currentThread().getName());
        }

        // 减小 directArena 的线程引用计数
        if (directArena != null) {
            directArena.numThreadCaches.getAndDecrement();
        }

        // 减小 heapArena 的线程引用计数
        if (heapArena != null) {
            heapArena.numThreadCaches.getAndDecrement();
        }
    }

    private static int free(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return 0;
        }

        int numFreed = 0;
        for (MemoryRegionCache<?> c: caches) {
            numFreed += free(c);
        }
        return numFreed;
    }

    private static int free(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return 0;
        }
        return cache.free();
    }

    void trim() {
        trim(tinySubPageDirectCaches);
        trim(smallSubPageDirectCaches);
        trim(normalDirectCaches);
        trim(tinySubPageHeapCaches);
        trim(smallSubPageHeapCaches);
        trim(normalHeapCaches);
    }

    private static void trim(MemoryRegionCache<?>[] caches) {
        if (caches == null) {
            return;
        }
        for (MemoryRegionCache<?> c: caches) {
            trim(c);
        }
    }

    private static void trim(MemoryRegionCache<?> cache) {
        if (cache == null) {
            return;
        }
        cache.trim();
    }

    private MemoryRegionCache<?> cacheForTiny(PoolArena<?> area, int normCapacity) {
        // 获得数组下标
        int idx = PoolArena.tinyIdx(normCapacity);
        if (area.isDirect()) {
            return cache(tinySubPageDirectCaches, idx);
        }
        return cache(tinySubPageHeapCaches, idx);
    }

    private MemoryRegionCache<?> cacheForSmall(PoolArena<?> area, int normCapacity) {
        // 获得数组下标
        int idx = PoolArena.smallIdx(normCapacity);
        if (area.isDirect()) {
            return cache(smallSubPageDirectCaches, idx);
        }
        return cache(smallSubPageHeapCaches, idx);
    }

    private MemoryRegionCache<?> cacheForNormal(PoolArena<?> area, int normCapacity) {
        if (area.isDirect()) {
            // 获得数组下标
            int idx = log2(normCapacity >> numShiftsNormalDirect);
            return cache(normalDirectCaches, idx);
        }
        // 获得数组下标
        int idx = log2(normCapacity >> numShiftsNormalHeap);
        return cache(normalHeapCaches, idx);
    }

    private static <T> MemoryRegionCache<T> cache(MemoryRegionCache<T>[] cache, int idx) {
        // 不在范围内，说明不缓存该容量的内存块
        if (cache == null || idx > cache.length - 1) {
            return null;
        }
        // 获得 MemoryRegionCache 对象
        return cache[idx];
    }

    /**
     * Cache used for buffers which are backed by TINY or SMALL size.
     */
    private static final class SubPageMemoryRegionCache<T> extends MemoryRegionCache<T> {

        SubPageMemoryRegionCache(int size, SizeClass sizeClass) {
            super(size, sizeClass);
        }

        @Override
        protected void initBuf(PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            // 初始化 Subpage 内存块到 PooledByteBuf 对象中
            chunk.initBufWithSubpage(buf, handle, reqCapacity);
        }

    }

    /**
     * Cache used for buffers which are backed by NORMAL size.
     */
    private static final class NormalMemoryRegionCache<T> extends MemoryRegionCache<T> {

        NormalMemoryRegionCache(int size) {
            super(size, SizeClass.Normal);
        }

        @Override
        protected void initBuf(PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity) {
            // 初始化 Page 内存块到 PooledByteBuf 对象中
            chunk.initBuf(buf, handle, reqCapacity);
        }

    }

    private abstract static class MemoryRegionCache<T> {

        /**
         * {@link #queue} 队列大小
         */
        private final int size;
        /**
         * 队列。里面存储内存块
         */
        private final Queue<Entry<T>> queue;
        /**
         * 内存类型
         */
        private final SizeClass sizeClass;
        /**
         * 分配次数计数器
         */
        private int allocations;

        MemoryRegionCache(int size, SizeClass sizeClass) {
            this.size = MathUtil.safeFindNextPositivePowerOfTwo(size);
            queue = PlatformDependent.newFixedMpscQueue(this.size); // MPSC
            this.sizeClass = sizeClass;
        }

        /**
         * Init the {@link PooledByteBuf} using the provided chunk and handle with the capacity restrictions.
         */
        protected abstract void initBuf(PoolChunk<T> chunk, long handle, PooledByteBuf<T> buf, int reqCapacity);

        /**
         * Add to cache if not already full.
         */
        @SuppressWarnings("unchecked")
        public final boolean add(PoolChunk<T> chunk, long handle) {
            // 创建 Entry 对象
            Entry<T> entry = newEntry(chunk, handle);
            // 添加到队列
            boolean queued = queue.offer(entry);
            // 若添加失败，说明队列已满，回收 Entry 对象
            if (!queued) {
                // If it was not possible to cache the chunk, immediately recycle the entry
                entry.recycle();
            }

            return queued; // 是否添加成功
        }

        /**
         * Allocate something out of the cache if possible and remove the entry from the cache.
         */
        public final boolean allocate(PooledByteBuf<T> buf, int reqCapacity) {
            // 获取并移除队列首个 Entry 对象
            Entry<T> entry = queue.poll();
            // 获取失败，返回 false
            if (entry == null) {
                return false;
            }
            // 初始化内存块到 PooledByteBuf 对象中
            initBuf(entry.chunk, entry.handle, buf, reqCapacity);
            // 回收 Entry 对象
            entry.recycle();

            // 增加 allocations 计数。因为分配总是在相同线程，所以不需要考虑线程安全的问题
            // allocations is not thread-safe which is fine as this is only called from the same thread all time.
            ++ allocations;
            return true; // 返回 true ，分配成功
        }

        /**
         * 清除队列中的全部
         *
         * Clear out this cache and free up all previous cached {@link PoolChunk}s and {@code handle}s.
         */
        public final int free() {
            return free(Integer.MAX_VALUE);
        }

        // 清除队列中的指定数量元素
        private int free(int max) {
            int numFreed = 0;
            for (; numFreed < max; numFreed++) {
                // 获取并移除首元素
                Entry<T> entry = queue.poll();
                if (entry != null) {
                    // 释放缓存的内存块回 Chunk 中
                    freeEntry(entry);
                } else {
                    // all cleared
                    return numFreed;
                }
            }
            return numFreed;
        }

        /**
         * Free up cached {@link PoolChunk}s if not allocated frequently enough.
         */
        public final void trim() {
            // allocations 表示已经重新分配出去的ByteBuf个数
            int free = size - allocations;
            allocations = 0;

            // 在一定阈值内还没被分配出去的空间将被释放
            // We not even allocated all the number that are
            if (free > 0) {
                free(free); // 释放队列中的节点
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private  void freeEntry(Entry entry) {
            PoolChunk chunk = entry.chunk;
            long handle = entry.handle;

            // 回收 Entry 对象
            // recycle now so PoolChunk can be GC'ed.
            entry.recycle();

            // 释放缓存的内存块回 Chunk 中
            chunk.arena.freeChunk(chunk, handle, sizeClass);
        }

        static final class Entry<T> {

            /**
             * Recycler 处理器，用于回收 Entry 对象
             */
            final Handle<Entry<?>> recyclerHandle;
            /**
             * PoolChunk 对象
             */
            PoolChunk<T> chunk;
            /**
             * 内存块在 {@link #chunk} 的位置
             */
            long handle = -1;

            Entry(Handle<Entry<?>> recyclerHandle) {
                this.recyclerHandle = recyclerHandle;
            }

            void recycle() {
                // 置空
                chunk = null;
                handle = -1;
                // 回收 Entry 对象
                recyclerHandle.recycle(this);
            }
        }

        @SuppressWarnings("rawtypes")
        private static Entry newEntry(PoolChunk<?> chunk, long handle) {
            // 从 Recycler 对象中，获得 Entry 对象
            Entry entry = RECYCLER.get();
            // 初始化属性
            entry.chunk = chunk;
            entry.handle = handle;
            return entry;
        }

        @SuppressWarnings("rawtypes")
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {

            @SuppressWarnings("unchecked")
            @Override
            protected Entry newObject(Handle<Entry> handle) {
                return new Entry(handle); // 创建 Entry 对象
            }

        };
    }
}
