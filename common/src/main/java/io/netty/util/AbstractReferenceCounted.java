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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.netty.util.internal.ObjectUtil.checkPositive;

/**
 * Abstract base class for classes wants to implement {@link ReferenceCounted}.
 */
public abstract class AbstractReferenceCounted implements ReferenceCounted {

    /**
     * {@link #refCnt} 的更新器
     */
    private static final AtomicIntegerFieldUpdater<AbstractReferenceCounted> refCntUpdater = AtomicIntegerFieldUpdater.newUpdater(AbstractReferenceCounted.class, "refCnt");

    /**
     * 引用计数
     */
    private volatile int refCnt = 1;

    @Override
    public final int refCnt() {
        return refCnt;
    }

    /**
     * 直接修改 refCnt
     *
     * An unsafe operation intended for use by a subclass that sets the reference count of the buffer directly
     */
    protected final void setRefCnt(int refCnt) {
        refCntUpdater.set(this, refCnt);
    }

    @Override
    public ReferenceCounted retain() {
        return retain0(1);
    }

    @Override
    public ReferenceCounted retain(int increment) {
        return retain0(checkPositive(increment, "increment"));
    }

    private ReferenceCounted retain0(int increment) {
        // 增加
        int oldRef = refCntUpdater.getAndAdd(this, increment);
        // 原有 refCnt 就是 <= 0 ；或者，increment 为负数
        if (oldRef <= 0 || oldRef + increment < oldRef) {
            // Ensure we don't resurrect (which means the refCnt was 0) and also that we encountered an overflow.
            // 加回去，负负得正。
            refCntUpdater.getAndAdd(this, -increment);
            // 抛出 IllegalReferenceCountException 异常
            throw new IllegalReferenceCountException(oldRef, increment);
        }
        return this;
    }

    @Override
    public ReferenceCounted touch() {
        return touch(null);
    }

    @Override
    public boolean release() {
        return release0(1);
    }

    @Override
    public boolean release(int decrement) {
        return release0(checkPositive(decrement, "decrement"));
    }

    @SuppressWarnings("Duplicates")
    private boolean release0(int decrement) {
        // 减少
        int oldRef = refCntUpdater.getAndAdd(this, -decrement);
        // 原有 oldRef 等于减少的值
        if (oldRef == decrement) {
            // 释放
            deallocate();
            return true;
        // 减少的值得大于 原有 oldRef ，说明“越界”；或者，increment 为负数
        } else if (oldRef < decrement || oldRef - decrement > oldRef) {
            // Ensure we don't over-release, and avoid underflow.
            // 加回去，负负得正。
            refCntUpdater.getAndAdd(this, decrement);
            // 抛出 IllegalReferenceCountException 异常
            throw new IllegalReferenceCountException(oldRef, -decrement);
        }
        return false;
    }

    /**
     * 释放
     *
     * Called once {@link #refCnt()} is equals 0.
     */
    protected abstract void deallocate();

}
