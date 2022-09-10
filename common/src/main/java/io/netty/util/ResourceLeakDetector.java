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

package io.netty.util;

import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static io.netty.util.internal.StringUtil.NEWLINE;
import static io.netty.util.internal.StringUtil.simpleClassName;

public class ResourceLeakDetector<T> {

    private static final String PROP_LEVEL_OLD = "io.netty.leakDetectionLevel";
    private static final String PROP_LEVEL = "io.netty.leakDetection.level";
    /**
     * 默认内存检测级别
     */
    private static final Level DEFAULT_LEVEL = Level.SIMPLE;

    private static final String PROP_TARGET_RECORDS = "io.netty.leakDetection.targetRecords";
    /**
     * 默认每个 DefaultResourceLeak 记录的 Record 数量
     */
    private static final int DEFAULT_TARGET_RECORDS = 4;

    /**
     * 每个 DefaultResourceLeak 记录的 Record 数量
     */
    private static final int TARGET_RECORDS;

    /**
     * 内存检测级别枚举
     *
     * Represents the level of resource leak detection.
     */
    public enum Level {
        /**
         * Disables resource leak detection.
         */
        DISABLED,
        /**
         * Enables simplistic sampling resource leak detection which reports there is a leak or not,
         * at the cost of small overhead (default).
         */
        SIMPLE,
        /**
         * Enables advanced sampling resource leak detection which reports where the leaked object was accessed
         * recently at the cost of high overhead.
         */
        ADVANCED,
        /**
         * Enables paranoid resource leak detection which reports where the leaked object was accessed recently,
         * at the cost of the highest possible overhead (for testing purposes only).
         */
        PARANOID;

        /**
         * Returns level based on string value. Accepts also string that represents ordinal number of enum.
         *
         * @param levelStr - level string : DISABLED, SIMPLE, ADVANCED, PARANOID. Ignores case.
         * @return corresponding level or SIMPLE level in case of no match.
         */
        static Level parseLevel(String levelStr) {
            String trimmedLevelStr = levelStr.trim();
            for (Level l : values()) {
                if (trimmedLevelStr.equalsIgnoreCase(l.name()) || trimmedLevelStr.equals(String.valueOf(l.ordinal()))) {
                    return l;
                }
            }
            return DEFAULT_LEVEL;
        }
    }

    /**
     * 内存泄露检测等级
     */
    private static Level level;

    /**
     * 默认采集频率
     */
    // There is a minor performance benefit in TLR if this is a power of 2.
    static final int DEFAULT_SAMPLING_INTERVAL = 128;

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);

    static {
        // 获得是否禁用泄露检测
        final boolean disabled;
        if (SystemPropertyUtil.get("io.netty.noResourceLeakDetection") != null) {
            disabled = SystemPropertyUtil.getBoolean("io.netty.noResourceLeakDetection", false);
            logger.debug("-Dio.netty.noResourceLeakDetection: {}", disabled);
            logger.warn("-Dio.netty.noResourceLeakDetection is deprecated. Use '-D{}={}' instead.", PROP_LEVEL, DEFAULT_LEVEL.name().toLowerCase());
        } else {
            disabled = false;
        }

        // 获得默认级别
        Level defaultLevel = disabled? Level.DISABLED : DEFAULT_LEVEL;
        // 获得配置的级别字符串，从老版本的配置
        // First read old property name (兼容老版本）
        String levelStr = SystemPropertyUtil.get(PROP_LEVEL_OLD, defaultLevel.name());
        // 获得配置的级别字符串，从新版本的配置
        // If new property name is present, use it
        levelStr = SystemPropertyUtil.get(PROP_LEVEL, levelStr);
        // 获得最终的级别
        Level level = Level.parseLevel(levelStr);
        // 设置最终的级别
        ResourceLeakDetector.level = level;

        // TODO 芋艿
        TARGET_RECORDS = SystemPropertyUtil.getInt(PROP_TARGET_RECORDS, DEFAULT_TARGET_RECORDS);

        if (logger.isDebugEnabled()) {
            logger.debug("-D{}: {}", PROP_LEVEL, level.name().toLowerCase());
            logger.debug("-D{}: {}", PROP_TARGET_RECORDS, TARGET_RECORDS);
        }
    }

    /**
     * @deprecated Use {@link #setLevel(Level)} instead.
     */
    @Deprecated
    public static void setEnabled(boolean enabled) {
        setLevel(enabled? Level.SIMPLE : Level.DISABLED);
    }

    /**
     * Returns {@code true} if resource leak detection is enabled.
     */
    public static boolean isEnabled() {
        return getLevel().ordinal() > Level.DISABLED.ordinal();
    }

    /**
     * Sets the resource leak detection level.
     */
    public static void setLevel(Level level) {
        if (level == null) {
            throw new NullPointerException("level");
        }
        ResourceLeakDetector.level = level;
    }

    /**
     * Returns the current resource leak detection level.
     */
    public static Level getLevel() {
        return level;
    }

    /**
     * DefaultResourceLeak 集合
     *
     * the collection of active resources
     */
    private final ConcurrentMap<DefaultResourceLeak<?>, LeakEntry> allLeaks = PlatformDependent.newConcurrentHashMap();

    /**
     * 引用队列
     */
    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
    /**
     * 已汇报的内存泄露的资源类型的集合
     */
    private final ConcurrentMap<String, Boolean> reportedLeaks = PlatformDependent.newConcurrentHashMap();

    /**
     * 资源类型
     */
    private final String resourceType;
    /**
     * 采集评率
     */
    private final int samplingInterval;

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(Class<?> resourceType) {
        this(simpleClassName(resourceType));
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
    }

    /**
     * @deprecated Use {@link ResourceLeakDetector#ResourceLeakDetector(Class, int)}.
     * <p>
     * This should not be used directly by users of {@link ResourceLeakDetector}.
     * Please use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class)}
     * or {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}
     *
     * @param maxActive This is deprecated and will be ignored.
     */
    @Deprecated
    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        this(resourceType, samplingInterval);
    }

    /**
     * This should not be used directly by users of {@link ResourceLeakDetector}.
     * Please use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class)}
     * or {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}
     */
    @SuppressWarnings("deprecation")
    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
        this(simpleClassName(resourceType), samplingInterval, Long.MAX_VALUE);
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     * <p>
     * @param maxActive This is deprecated and will be ignored.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        if (resourceType == null) {
            throw new NullPointerException("resourceType");
        }

        this.resourceType = resourceType;
        this.samplingInterval = samplingInterval;
    }

    /**
     * Creates a new {@link ResourceLeak} which is expected to be closed via {@link ResourceLeak#close()} when the
     * related resource is deallocated.
     *
     * @return the {@link ResourceLeak} or {@code null}
     * @deprecated use {@link #track(Object)}
     */
    @Deprecated
    public final ResourceLeak open(T obj) {
        return track0(obj);
    }

    /**
     * Creates a new {@link ResourceLeakTracker} which is expected to be closed via
     * {@link ResourceLeakTracker#close(Object)} when the related resource is deallocated.
     *
     * @return the {@link ResourceLeakTracker} or {@code null}
     */
    @SuppressWarnings("unchecked")
    public final ResourceLeakTracker<T> track(T obj) {
        return track0(obj);
    }

    @SuppressWarnings("unchecked")
    private DefaultResourceLeak track0(T obj) {
        Level level = ResourceLeakDetector.level;
        // DISABLED 级别，不创建
        if (level == Level.DISABLED) {
            return null;
        }

        // SIMPLE 和 ADVANCED
        if (level.ordinal() < Level.PARANOID.ordinal()) {
            // 随机
            if ((PlatformDependent.threadLocalRandom().nextInt(samplingInterval)) == 0) {
                // 汇报内存是否泄漏
                reportLeak();
                // 创建 DefaultResourceLeak 对象
                return new DefaultResourceLeak(obj, refQueue, allLeaks);
            }
            return null;
        }

        // PARANOID 级别
        // 汇报内存是否泄漏
        reportLeak();
        // 创建 DefaultResourceLeak 对象
        return new DefaultResourceLeak(obj, refQueue, allLeaks);
    }

    private void clearRefQueue() {
        for (;;) {
            @SuppressWarnings("unchecked")
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }
            // 清理，并返回是否内存泄露
            ref.dispose();
        }
    }

    private void reportLeak() {
        // 如果不允许打印错误日志，则无法汇报，清理队列，并直接结束。
        if (!logger.isErrorEnabled()) {
            // 清理队列
            clearRefQueue();
            return;
        }

        // 循环引用队列，直到为空
        // Detect and report previous leaks.
        for (;;) {
            @SuppressWarnings("unchecked")
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }

            // 清理，并返回是否内存泄露
            if (!ref.dispose()) {
                continue;
            }

            // 获得 Record 日志
            String records = ref.toString();
            // 相同 Record 日志，只汇报一次
            if (reportedLeaks.putIfAbsent(records, Boolean.TRUE) == null) {
                if (records.isEmpty()) {
                    reportUntracedLeak(resourceType);
                } else {
                    reportTracedLeak(resourceType, records);
                }
            }
        }
    }

    /**
     * This method is called when a traced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportTracedLeak(String resourceType, String records) {
        logger.error(
                "LEAK: {}.release() was not called before it's garbage-collected. " +
                "See http://netty.io/wiki/reference-counted-objects.html for more information.{}",
                resourceType, records);
    }

    /**
     * This method is called when an untraced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportUntracedLeak(String resourceType) {
        logger.error("LEAK: {}.release() was not called before it's garbage-collected. " +
                "Enable advanced leak reporting to find out where the leak occurred. " +
                "To enable advanced leak reporting, " +
                "specify the JVM option '-D{}={}' or call {}.setLevel() " +
                "See http://netty.io/wiki/reference-counted-objects.html for more information.",
                resourceType, PROP_LEVEL, Level.ADVANCED.name().toLowerCase(), simpleClassName(this));
    }

    /**
     * @deprecated This method will no longer be invoked by {@link ResourceLeakDetector}.
     */
    @Deprecated
    protected void reportInstancesLeak(String resourceType) {
    }

    @SuppressWarnings("deprecation")
    private static final class DefaultResourceLeak<T>
            extends WeakReference<Object> implements ResourceLeakTracker<T>, ResourceLeak {

        /**
         * {@link #head} 的更新器
         */
        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicReferenceFieldUpdater<DefaultResourceLeak<?>, Record> headUpdater =
                (AtomicReferenceFieldUpdater)
                        AtomicReferenceFieldUpdater.newUpdater(DefaultResourceLeak.class, Record.class, "head");

        /**
         * {@link #droppedRecords} 的更新器
         */
        @SuppressWarnings("unchecked") // generics and updaters do not mix.
        private static final AtomicIntegerFieldUpdater<DefaultResourceLeak<?>> droppedRecordsUpdater =
                (AtomicIntegerFieldUpdater)
                        AtomicIntegerFieldUpdater.newUpdater(DefaultResourceLeak.class, "droppedRecords");

        /**
         * Record 链的头节点
         *
         * 看完 {@link #record()} 方法后，实际上，head 是尾节点，即最后( 新 )的一条 Record 。
         */
        @SuppressWarnings("unused")
        private volatile Record head;
        /**
         * 丢弃的 Record 计数
         */
        @SuppressWarnings("unused")
        private volatile int droppedRecords;

        /**
         * DefaultResourceLeak 集合。来自 {@link ResourceLeakDetector#allLeaks}
         */
        private final ConcurrentMap<DefaultResourceLeak<?>, LeakEntry> allLeaks;
        /**
         * hash 值
         *
         * 保证 {@link #close(Object)} 传入的对象，就是 {@link #referent} 对象
         */
        private final int trackedHash;

        DefaultResourceLeak(
                Object referent,
                ReferenceQueue<Object> refQueue,
                ConcurrentMap<DefaultResourceLeak<?>, LeakEntry> allLeaks) {
            // 父构造方法
            super(referent, refQueue);

            assert referent != null;

            // Store the hash of the tracked object to later assert it in the close(...) method.
            // It's important that we not store a reference to the referent as this would disallow it from
            // be collected via the WeakReference.
            trackedHash = System.identityHashCode(referent);
            allLeaks.put(this, LeakEntry.INSTANCE);
            // Create a new Record so we always have the creation stacktrace included.
            headUpdater.set(this, new Record(Record.BOTTOM));
            this.allLeaks = allLeaks;
        }

        @Override
        public void record() {
            record0(null);
        }

        @Override
        public void record(Object hint) {
            record0(hint);
        }

        /**
         * This method works by exponentially backing off as more records are present in the stack. Each record has a
         * 1 / 2^n chance of dropping the top most record and replacing it with itself. This has a number of convenient
         * properties:
         *
         * <ol>
         * <li>  The current record is always recorded. This is due to the compare and swap dropping the top most
         *       record, rather than the to-be-pushed record.
         * <li>  The very last access will always be recorded. This comes as a property of 1.
         * <li>  It is possible to retain more records than the target, based upon the probability distribution.
         * <li>  It is easy to keep a precise record of the number of elements in the stack, since each element has to
         *     know how tall the stack is.
         * </ol>
         *
         * In this particular implementation, there are also some advantages. A thread local random is used to decide
         * if something should be recorded. This means that if there is a deterministic access pattern, it is now
         * possible to see what other accesses occur, rather than always dropping them. Second, after
         * {@link #TARGET_RECORDS} accesses, backoff occurs. This matches typical access patterns,
         * where there are either a high number of accesses (i.e. a cached buffer), or low (an ephemeral buffer), but
         * not many in between.
         *
         * The use of atomics avoids serializing a high number of accesses, when most of the records will be thrown
         * away. High contention only happens when there are very few existing records, which is only likely when the
         * object isn't shared! If this is a problem, the loop can be aborted and the record dropped, because another
         * thread won the race.
         */
        private void record0(Object hint) {
            // Check TARGET_RECORDS > 0 here to avoid similar check before remove from and add to lastRecords
            if (TARGET_RECORDS > 0) {
                Record oldHead;
                Record prevHead;
                Record newHead;
                boolean dropped;
                do {
                    // 已经关闭，则返回
                    if ((prevHead = oldHead = headUpdater.get(this)) == null) {
                        // already closed.
                        return;
                    }
                    // 当超过 TARGET_RECORDS 数量时，随机丢到头节点。
                    final int numElements = oldHead.pos + 1;
                    if (numElements >= TARGET_RECORDS) {
                        final int backOffFactor = Math.min(numElements - TARGET_RECORDS, 30);
                        if (dropped = PlatformDependent.threadLocalRandom().nextInt(1 << backOffFactor) != 0) {
                            prevHead = oldHead.next;
                        }
                    } else {
                        dropped = false;
                    }
                    // 创建新的头节点
                    newHead = hint != null ? new Record(prevHead, hint) : new Record(prevHead);
                } while (!headUpdater.compareAndSet(this, oldHead, newHead)); // cas 修改头节点
                // 若丢弃，增加 droppedRecordsUpdater 计数
                if (dropped) {
                    droppedRecordsUpdater.incrementAndGet(this);
                }
            }
        }

        // 清理，并返回是否内存泄露
        boolean dispose() {
            // 清理 referent 的引用
            clear();
            // 移除出 allLeaks 。移除成功，意味着内存泄露。
            return allLeaks.remove(this, LeakEntry.INSTANCE);
        }

        @Override
        public boolean close() {
            // 移除出 allLeaks
            // Use the ConcurrentMap remove method, which avoids allocating an iterator.
            if (allLeaks.remove(this, LeakEntry.INSTANCE)) {
                // 清理 referent 的引用
                // Call clear so the reference is not even enqueued.
                clear();
                // 置空 head
                headUpdater.set(this, null);
                return true; // 返回成功
            }
            return false; // 返回失败
        }

        @Override
        public boolean close(T trackedObject) {
            // 校验一致
            // Ensure that the object that was tracked is the same as the one that was passed to close(...).
            assert trackedHash == System.identityHashCode(trackedObject);

            // 关闭
            // We need to actually do the null check of the trackedObject after we close the leak because otherwise
            // we may get false-positives reported by the ResourceLeakDetector. This can happen as the JIT / GC may
            // be able to figure out that we do not need the trackedObject anymore and so already enqueue it for
            // collection before we actually get a chance to close the enclosing ResourceLeak.
            return close() && trackedObject != null;
        }

        @Override
        public String toString() {
            // 获得 head 属性，并置空
            Record oldHead = headUpdater.getAndSet(this, null);
            // 若为空，说明已经关闭。
            if (oldHead == null) {
                // Already closed
                return EMPTY_STRING;
            }

            final int dropped = droppedRecordsUpdater.get(this);
            int duped = 0;

            int present = oldHead.pos + 1;
            // Guess about 2 kilobytes per stack trace
            StringBuilder buf = new StringBuilder(present * 2048).append(NEWLINE);
            buf.append("Recent access records: ").append(NEWLINE);

            // 拼接 Record 练
            int i = 1;
            Set<String> seen = new HashSet<String>(present);
            for (; oldHead != Record.BOTTOM; oldHead = oldHead.next) {
                String s = oldHead.toString();
                if (seen.add(s)) { // 是否重复
                    if (oldHead.next == Record.BOTTOM) {
                        buf.append("Created at:").append(NEWLINE).append(s);
                    } else {
                        buf.append('#').append(i++).append(':').append(NEWLINE).append(s);
                    }
                } else {
                    duped++;
                }
            }

            // 拼接 duped ( 重复 ) 次数
            if (duped > 0) {
                buf.append(": ")
                        .append(dropped)
                        .append(" leak records were discarded because they were duplicates")
                        .append(NEWLINE);
            }

            // 拼接 dropped (丢弃) 次数
            if (dropped > 0) {
                buf.append(": ")
                   .append(dropped)
                   .append(" leak records were discarded because the leak record count is targeted to ")
                   .append(TARGET_RECORDS)
                   .append(". Use system property ")
                   .append(PROP_TARGET_RECORDS)
                   .append(" to increase the limit.")
                   .append(NEWLINE);
            }

            buf.setLength(buf.length() - NEWLINE.length());
            return buf.toString();
        }

    }

    /**
     * 忽略的方法集合
     */
    private static final AtomicReference<String[]> excludedMethods = new AtomicReference<String[]>(EmptyArrays.EMPTY_STRINGS);

    public static void addExclusions(Class clz, String ... methodNames) {
        Set<String> nameSet = new HashSet<String>(Arrays.asList(methodNames));
        // Use loop rather than lookup. This avoids knowing the parameters, and doesn't have to handle
        // NoSuchMethodException.
        for (Method method : clz.getDeclaredMethods()) {
            if (nameSet.remove(method.getName()) && nameSet.isEmpty()) {
                break;
            }
        }
        if (!nameSet.isEmpty()) {
            throw new IllegalArgumentException("Can't find '" + nameSet + "' in " + clz.getName());
        }
        String[] oldMethods;
        String[] newMethods;
        do {
            oldMethods = excludedMethods.get();
            newMethods = Arrays.copyOf(oldMethods, oldMethods.length + 2 * methodNames.length);
            for (int i = 0; i < methodNames.length; i++) {
                newMethods[oldMethods.length + i * 2] = clz.getName();
                newMethods[oldMethods.length + i * 2 + 1] = methodNames[i];
            }
        } while (!excludedMethods.compareAndSet(oldMethods, newMethods));
    }

    private static final class Record extends Throwable {

        private static final long serialVersionUID = 6065153674892850720L;

        /**
         * 尾节点的单例
         */
        private static final Record BOTTOM = new Record();

        /**
         * hint 字符串
         */
        private final String hintString;
        /**
         * 下一个节点
         */
        private final Record next;
        /**
         * 位置
         */
        private final int pos;

        // =========== 构造方法 ===========

        Record(Record next, Object hint) {
            // This needs to be generated even if toString() is never called as it may change later on.
            hintString = hint instanceof ResourceLeakHint ? ((ResourceLeakHint) hint).toHintString() : hint.toString();
            this.next = next;
            this.pos = next.pos + 1;
        }

        Record(Record next) {
           hintString = null;
           this.next = next;
           this.pos = next.pos + 1;
        }

        // Used to terminate the stack
        private Record() {
            hintString = null;
            next = null;
            pos = -1;
        }

        // =========== toString ===========

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(2048);
            if (hintString != null) {
                buf.append("\tHint: ").append(hintString).append(NEWLINE);
            }

            // Append the stack trace.
            StackTraceElement[] array = getStackTrace();
            // Skip the first three elements.
            out: for (int i = 3; i < array.length; i++) {
                StackTraceElement element = array[i];
                // 跳过忽略的方法
                // Strip the noisy stack trace elements.
                String[] exclusions = excludedMethods.get();
                for (int k = 0; k < exclusions.length; k += 2) {
                    if (exclusions[k].equals(element.getClassName())
                            && exclusions[k + 1].equals(element.getMethodName())) {
                        continue out;
                    }
                }

                buf.append('\t');
                buf.append(element.toString());
                buf.append(NEWLINE);
            }
            return buf.toString();
        }

    }

    private static final class LeakEntry {

        /**
         * 单例
         */
        static final LeakEntry INSTANCE = new LeakEntry();

        /**
         * hash 值，避免重复计算
         */
        private static final int HASH = System.identityHashCode(INSTANCE);

        private LeakEntry() { // 禁止创建，仅使用 INSTANCE 单例
        }

        @Override
        public int hashCode() {
            return HASH;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }

    }
}
