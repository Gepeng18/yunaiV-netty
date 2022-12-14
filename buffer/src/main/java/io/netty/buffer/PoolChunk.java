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

/**
 * Description of algorithm for PageRun/PoolSubpage allocation from PoolChunk
 *
 * Notation: The following terms are important to understand the code
 * > page  - a page is the smallest unit of memory chunk that can be allocated
 * > chunk - a chunk is a collection of pages
 * > in this code chunkSize = 2^{maxOrder} * pageSize
 *
 * To begin we allocate a byte array of size = chunkSize
 * Whenever a ByteBuf of given size needs to be created we search for the first position
 * in the byte array that has enough empty space to accommodate the requested size and
 * return a (long) handle that encodes this offset information, (this memory segment is then
 * marked as reserved so it is always used by exactly one ByteBuf and no more)
 *
 * For simplicity all sizes are normalized according to PoolArena#normalizeCapacity method
 * This ensures that when we request for memory segments of size >= pageSize the normalizedCapacity
 * equals the next nearest power of 2
 *
 * To search for the first offset in chunk that has at least requested size available we construct a
 * complete balanced binary tree and store it in an array (just like heaps) - memoryMap
 *
 * The tree looks like this (the size of each node being mentioned in the parenthesis)
 *
 * depth=0        1 node (chunkSize)
 * depth=1        2 nodes (chunkSize/2)
 * ..
 * ..
 * depth=d        2^d nodes (chunkSize/2^d)
 * ..
 * depth=maxOrder 2^maxOrder nodes (chunkSize/2^{maxOrder} = pageSize)
 *
 * depth=maxOrder is the last level and the leafs consist of pages
 *
 * With this tree available searching in chunkArray translates like this:
 * To allocate a memory segment of size chunkSize/2^k we search for the first node (from left) at height k
 * which is unused
 *
 * Algorithm:
 * ----------
 * Encode the tree in memoryMap with the notation
 *   memoryMap[id] = x => in the subtree rooted at id, the first node that is free to be allocated
 *   is at depth x (counted from depth=0) i.e., at depths [depth_of_id, x), there is no node that is free
 *
 *  As we allocate & free nodes, we update values stored in memoryMap so that the property is maintained
 *
 * Initialization -
 *   In the beginning we construct the memoryMap array by storing the depth of a node at each node
 *     i.e., memoryMap[id] = depth_of_id
 *
 * Observations:
 * -------------
 * 1) memoryMap[id] = depth_of_id  => it is free / unallocated
 * 2) memoryMap[id] > depth_of_id  => at least one of its child nodes is allocated, so we cannot allocate it, but
 *                                    some of its children can still be allocated based on their availability
 * 3) memoryMap[id] = maxOrder + 1 => the node is fully allocated & thus none of its children can be allocated, it
 *                                    is thus marked as unusable
 *
 * Algorithm: [allocateNode(d) => we want to find the first node (from left) at height h that can be allocated]
 * ----------
 * 1) start at root (i.e., depth = 0 or id = 1)
 * 2) if memoryMap[1] > d => cannot be allocated from this chunk
 * 3) if left node value <= h; we can allocate from left subtree so move to left and repeat until found
 * 4) else try in right subtree
 *
 * Algorithm: [allocateRun(size)]
 * ----------
 * 1) Compute d = log_2(chunkSize/size)
 * 2) Return allocateNode(d)
 *
 * Algorithm: [allocateSubpage(size)]
 * ----------
 * 1) use allocateNode(maxOrder) to find an empty (i.e., unused) leaf (i.e., page)
 * 2) use this handle to construct the PoolSubpage object or if it already exists just call init(normCapacity)
 *    note that this PoolSubpage object is added to subpagesPool in the PoolArena when we init() it
 *
 * Note:
 * -----
 * In the implementation for improving cache coherence,
 * we store 2 pieces of information depth_of_id and x as two byte values in memoryMap and depthMap respectively
 *
 * memoryMap[id]= depth_of_id  is defined above
 * depthMap[id]= x  indicates that the first node which is free to be allocated is at depth x (from root)
 */
final class PoolChunk<T> implements PoolChunkMetric {

    private static final int INTEGER_SIZE_MINUS_ONE = Integer.SIZE - 1;

    /**
     * ?????? Arena ??????
     */
    final PoolArena<T> arena;
    /**
     * ???????????????
     *
     * @see PooledByteBuf#memory
     */
    final T memory;
    /**
     * ???????????????
     *
     * @see #PoolChunk(PoolArena, Object, int, int) ??????????????????????????????????????? Huge ??????????????????????????? Chunk ??????????????????????????? Page
     * @see #PoolChunk(PoolArena, Object, int, int, int, int, int) ??????
     */
    final boolean unpooled;
    /**
     * TODO ??????
     */
    final int offset;

    /**
     * ????????????????????????
     *
     * index ???????????????
     */
    private final byte[] memoryMap;
    /**
     * ????????????????????????
     *
     * index ???????????????
     */
    private final byte[] depthMap;
    /**
     * PoolSubpage ??????
     */
    private final PoolSubpage<T>[] subpages;
    /**
     * ????????????????????????????????? Tiny/Small ???????????? Subpage ????????????
     *
     * Used to determine if the requested capacity is equal to or greater than pageSize.
     */
    private final int subpageOverflowMask;
    /**
     * Page ??????????????? 8KB = 8192B
     */
    private final int pageSize;
    /**
     * ??? 1 ??????????????? {@link #pageSize} ?????????????????? 13 ???1 << 13 = 8192 ???
     *
     * ?????????????????? {@link #allocateRun(int)} ?????????????????????????????????????????????????????????
     */
    private final int pageShifts;
    /**
     * ????????????????????????????????? 11 ???
     */
    private final int maxOrder;
    /**
     * Chunk ????????????????????????????????? 16M = 16 * 1024  ???
     */
    private final int chunkSize;
    /**
     * log2 {@link #chunkSize} ????????????????????? log2( 16M ) = 24 ???
     */
    private final int log2ChunkSize;
    /**
     * ????????? {@link #subpages} ??????????????????????????????????????? 1 << maxOrder = 1 << 11 = 2048 ???
     */
    private final int maxSubpageAllocs;
    /**
     * ????????????????????????????????? maxOrder + 1 = 12 ???
     *
     * Used to mark memory as unusable
     */
    private final byte unusable;

    /**
     * ?????????????????????
     */
    private int freeBytes;

    /**
     * ?????? PoolChunkList ??????
     */
    PoolChunkList<T> parent;
    /**
     * ????????? Chunk ??????
     */
    PoolChunk<T> prev;
    /**
     * ????????? Chunk ??????
     */
    PoolChunk<T> next;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    PoolChunk(PoolArena<T> arena, T memory, int pageSize, int maxOrder, int pageShifts, int chunkSize, int offset) {
        // ??????
        unpooled = false;
        this.arena = arena;
        this.memory = memory;
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.maxOrder = maxOrder;
        this.chunkSize = chunkSize;
        this.offset = offset;
        unusable = (byte) (maxOrder + 1);
        log2ChunkSize = log2(chunkSize);
        subpageOverflowMask = ~(pageSize - 1);
        freeBytes = chunkSize;

        assert maxOrder < 30 : "maxOrder should be < 30, but is: " + maxOrder;
        maxSubpageAllocs = 1 << maxOrder;

        // ????????? memoryMap ??? depthMap
        // Generate the memory map.
        memoryMap = new byte[maxSubpageAllocs << 1];
        depthMap = new byte[memoryMap.length];
        int memoryMapIndex = 1;
        for (int d = 0; d <= maxOrder; ++ d) { // move down the tree one level at a time
            int depth = 1 << d;
            for (int p = 0; p < depth; ++ p) {
                // in each level traverse left to right and set value to the depth of subtree
                memoryMap[memoryMapIndex] = (byte) d;
                depthMap[memoryMapIndex] = (byte) d;
                memoryMapIndex ++;
            }
        }

        // ????????? subpages
        subpages = newSubpageArray(maxSubpageAllocs);
    }

    /** Creates a special chunk that is not pooled. */
    PoolChunk(PoolArena<T> arena, T memory, int size, int offset) {
        // ?????????
        unpooled = true;
        this.arena = arena;
        this.memory = memory;
        this.offset = offset;
        memoryMap = null;
        depthMap = null;
        subpages = null;
        subpageOverflowMask = 0;
        pageSize = 0;
        pageShifts = 0;
        maxOrder = 0;
        unusable = (byte) (maxOrder + 1);
        chunkSize = size;
        log2ChunkSize = log2(chunkSize);
        maxSubpageAllocs = 0;
    }

    @SuppressWarnings("unchecked")
    private PoolSubpage<T>[] newSubpageArray(int size) {
        return new PoolSubpage[size];
    }

    @Override
    public int usage() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }
        return usage(freeBytes);
    }

    private int usage(int freeBytes) {
        // ???????????????100%
        if (freeBytes == 0) {
            return 100;
        }

        // ????????????????????? 99%
        int freePercentage = (int) (freeBytes * 100L / chunkSize);
        if (freePercentage == 0) {
            return 99;
        }
        return 100 - freePercentage;
    }

    long allocate(int normCapacity) {
        // ???????????? Page ??????????????? Page ?????????
        if ((normCapacity & subpageOverflowMask) != 0) { // >= pageSize
            return allocateRun(normCapacity);
        // ?????? Page ??????????????? Subpage ?????????
        } else {
            return allocateSubpage(normCapacity);
        }
    }

    /**
     * Update method used by allocate
     * This is triggered only when a successor is allocated and all its predecessors
     * need to update their state
     * The minimal depth at which subtree rooted at id has some free space
     *
     * @param id id
     */
    private void updateParentsAlloc(int id) {
        while (id > 1) {
            // ????????????????????????
            int parentId = id >>> 1;
            // ?????????????????????
            byte val1 = value(id);
            // ??????????????????????????????
            byte val2 = value(id ^ 1);
            // ????????????????????????????????????????????????
            byte val = val1 < val2 ? val1 : val2;
            setValue(parentId, val);

            // ???????????????
            id = parentId;
        }
    }

    /**
     * Update method used by free
     * This needs to handle the special case when both children are completely free
     * in which case parent be directly allocated on request of size = child-size * 2
     *
     * @param id id
     */
    private void updateParentsFree(int id) {
        // ???????????????????????????????????????
        int logChild = depth(id) + 1;
        while (id > 1) {
            // ????????????????????????
            int parentId = id >>> 1;
            // ?????????????????????
            byte val1 = value(id);
            // ?????????????????????????????????
            byte val2 = value(id ^ 1);
            // ???????????????????????????
            logChild -= 1; // in first iteration equals log, subsequently reduce 1 from logChild as we traverse up

            // ????????????????????????????????????????????????????????????
            if (val1 == logChild && val2 == logChild) {
                setValue(parentId, (byte) (logChild - 1));
            // ?????????????????????????????????????????????????????????????????????????????????
            } else {
                byte val = val1 < val2 ? val1 : val2;
                setValue(parentId, val);
            }

            // ???????????????
            id = parentId;
        }
    }

    /**
     * Algorithm to allocate an index in memoryMap when we query for a free node
     * at depth d
     *
     * @param d depth
     * @return index in memoryMap
     */
    private int allocateNode(int d) {
        int id = 1;
        int initial = - (1 << d); // has last d bits = 0 and rest all = 1
        // ???????????????????????????
        // ?????????????????????????????? d ??????????????? d ??????????????????????????????????????? [0, d-1] ?????????????????????????????????????????? Chunk ????????????????????????
        byte val = value(id);
        if (val > d) { // unusable
            return -1;
        }
        // ????????? d ????????????????????????
        // id & initial ???????????????????????? d ???????????????
        while (val < d || (id & initial) == 0) { // id & initial == 1 << d for all ids at depth d, for < d it is 0
            // ???????????????
            // ????????????????????????
            id <<= 1;
            // ?????????????????????
            val = value(id);
            // ??????????????? d ???????????????????????????????????????????????????????????????????????????????????????????????????
            if (val > d) {
                // ????????????????????????
                id ^= 1;
                // ?????????????????????
                val = value(id);
            }
        }

        // ??????????????????????????????
        byte value = value(id);
        assert value == d && (id & initial) == 1 << d : String.format("val = %d, id & initial = %d, d = %d",
                value, id & initial, d);

        // ??????????????????????????????
        setValue(id, unusable); // mark as unusable
        // ??????????????????????????????????????????
        updateParentsAlloc(id);

        // ??????????????????
        return id;
    }

    /**
     * Allocate a run of pages (>=1)
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    private long allocateRun(int normCapacity) {
        // ????????????
        int d = maxOrder - (log2(normCapacity) - pageShifts);
        // ????????????
        int id = allocateNode(d);
        // ?????????????????????????????????
        if (id < 0) {
            return id;
        }
        // ???????????????????????????
        freeBytes -= runLength(id);
        return id;
    }

    /**
     * Create/ initialize a new PoolSubpage of normCapacity
     * Any PoolSubpage created/ initialized here is added to subpage pool in the PoolArena that owns this PoolChunk
     *
     * @param normCapacity normalized capacity
     * @return index in memoryMap
     */
    private long allocateSubpage(int normCapacity) {
        // ??????????????????????????? Subpage ??????????????? head ??????
        // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
        // This is need as we may add it back and so alter the linked-list structure.
        PoolSubpage<T> head = arena.findSubpagePoolHead(normCapacity);
        // ????????????????????????????????????????????????????????????????????????????????????
        synchronized (head) {
            // ?????????????????????????????????Subpage ?????????????????????????????????????????????
            int d = maxOrder; // subpages are only be allocated from pages i.e., leaves
            int id = allocateNode(d);
            // ???????????????????????????
            if (id < 0) {
                return id;
            }

            final PoolSubpage<T>[] subpages = this.subpages;
            final int pageSize = this.pageSize;

            // ???????????????????????????
            freeBytes -= pageSize;

            // ????????????????????? subpages ???????????????
            int subpageIdx = subpageIdx(id);
            // ????????????????????? subpages ????????? PoolSubpage ??????
            PoolSubpage<T> subpage = subpages[subpageIdx];
            // ????????? PoolSubpage ??????
            if (subpage == null) { // ??????????????????????????? PoolSubpage ??????
                subpage = new PoolSubpage<T>(head, this, id, runOffset(id), pageSize, normCapacity);
                subpages[subpageIdx] = subpage;
            } else { // ??????????????????????????? PoolSubpage ??????
                subpage.init(head, normCapacity);
            }
            // ?????? PoolSubpage ?????????
            return subpage.allocate();
        }
    }

    /**
     * Free a subpage or a run of pages
     * When a subpage is freed from PoolSubpage, it might be added back to subpage pool of the owning PoolArena
     * If the subpage pool in PoolArena has at least one other PoolSubpage of given elemSize, we can
     * completely free the owning Page so it is available for subsequent allocations
     *
     * @param handle handle to free
     */
    void free(long handle) {
        // ?????? memoryMap ???????????????( ?????? )
        int memoryMapIdx = memoryMapIdx(handle);
        // ?????? bitmap ???????????????( ?????? )????????????????????????????????????????????? bitmapIdx ?????????????????? `bitmapIdx & 0x3FFFFFFF` ?????????
        int bitmapIdx = bitmapIdx(handle);

        // ?????? Subpage begin ~

        if (bitmapIdx != 0) { // free a subpage bitmapIdx ??????????????????????????? Subpage
            // ?????? PoolSubpage ??????
            PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
            assert subpage != null && subpage.doNotDestroy;

            // ??????????????????????????? Subpage ??????????????? head ??????
            // Obtain the head of the PoolSubPage pool that is owned by the PoolArena and synchronize on it.
            // This is need as we may add it back and so alter the linked-list structure.
            PoolSubpage<T> head = arena.findSubpagePoolHead(subpage.elemSize);
            // ????????????????????????????????????????????????????????????????????????????????????
            synchronized (head) {
                // ?????? Subpage ???
                if (subpage.free(head, bitmapIdx & 0x3FFFFFFF)) {
                    return;
                }
                // ????????? ?????? false ????????? Page ??????????????????????????? Subpage ??????????????????????????????????????????????????? Page
            }
        }

        // ?????? Page begin ~

        // ???????????????????????????
        freeBytes += runLength(memoryMapIdx);
        // ?????? Page ?????????????????????
        setValue(memoryMapIdx, depth(memoryMapIdx));
        // ?????? Page ??????????????????????????????
        updateParentsFree(memoryMapIdx);
    }

    void initBuf(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        // ?????? memoryMap ???????????????( ?????? )
        int memoryMapIdx = memoryMapIdx(handle);
        // ?????? bitmap ???????????????( ?????? )????????????????????????????????????????????? bitmapIdx ?????????????????? `bitmapIdx & 0x3FFFFFFF` ?????????
        int bitmapIdx = bitmapIdx(handle);
        // ???????????? Page
        if (bitmapIdx == 0) {
            byte val = value(memoryMapIdx);
            assert val == unusable : String.valueOf(val);
            // ????????? Page ???????????? PooledByteBuf ???
            buf.init(this, handle, runOffset(memoryMapIdx) + offset, reqCapacity, runLength(memoryMapIdx), arena.parent.threadCache());
        // ???????????? SubPage
        } else {
            // ????????? Subpage ???????????? PooledByteBuf ???
            initBufWithSubpage(buf, handle, bitmapIdx, reqCapacity);
        }
    }

    void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int reqCapacity) {
        initBufWithSubpage(buf, handle, bitmapIdx(handle), reqCapacity);
    }

    private void initBufWithSubpage(PooledByteBuf<T> buf, long handle, int bitmapIdx, int reqCapacity) {
        assert bitmapIdx != 0;

        // ?????? memoryMap ???????????????( ?????? )
        int memoryMapIdx = memoryMapIdx(handle);
        // ?????? Subpage ??????
        PoolSubpage<T> subpage = subpages[subpageIdx(memoryMapIdx)];
        assert subpage.doNotDestroy;
        assert reqCapacity <= subpage.elemSize;

        // ????????? Subpage ???????????? PooledByteBuf ???
        buf.init(
            this, handle,
            runOffset(memoryMapIdx) + (bitmapIdx & 0x3FFFFFFF) * subpage.elemSize + offset,
                reqCapacity, subpage.elemSize, arena.parent.threadCache());
    }

    private byte value(int id) {
        return memoryMap[id];
    }

    private void setValue(int id, byte val) {
        memoryMap[id] = val;
    }

    private byte depth(int id) {
        return depthMap[id];
    }

    private static int log2(int val) {
        // compute the (0-based, with lsb = 0) position of highest set bit i.e, log2
        return INTEGER_SIZE_MINUS_ONE - Integer.numberOfLeadingZeros(val);
    }

    private int runLength(int id) {
        // represents the size in #bytes supported by node 'id' in the tree
        return 1 << log2ChunkSize - depth(id);
    }

    private int runOffset(int id) {
        // represents the 0-based offset in #bytes from start of the byte-array chunk
        int shift = id ^ 1 << depth(id);
        return shift * runLength(id);
    }

    private int subpageIdx(int memoryMapIdx) {
        return memoryMapIdx ^ maxSubpageAllocs; // remove highest set bit, to get offset
    }

    private static int memoryMapIdx(long handle) {
        return (int) handle;
    }

    private static int bitmapIdx(long handle) {
        return (int) (handle >>> Integer.SIZE);
    }

    @Override
    public int chunkSize() {
        return chunkSize;
    }

    @Override
    public int freeBytes() {
        synchronized (arena) {
            return freeBytes;
        }
    }

    @Override
    public String toString() {
        final int freeBytes;
        synchronized (arena) {
            freeBytes = this.freeBytes;
        }

        return new StringBuilder()
                .append("Chunk(")
                .append(Integer.toHexString(System.identityHashCode(this)))
                .append(": ")
                .append(usage(freeBytes))
                .append("%, ")
                .append(chunkSize - freeBytes)
                .append('/')
                .append(chunkSize)
                .append(')')
                .toString();
    }

    void destroy() {
        arena.destroyChunk(this);
    }

}
