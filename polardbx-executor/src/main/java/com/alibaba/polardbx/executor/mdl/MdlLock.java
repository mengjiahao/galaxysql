/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mdl;

import com.alibaba.polardbx.common.utils.Assert;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.concurrent.locks.StampedLock;

/**
 * @author chenmo.cm
 * 拥有 StampedLock; 锁是上在 MdlKey 上;
 */
public abstract class MdlLock {

    protected final MdlKey key;

    protected final StampedLock latch;

    /** rStamp 是乐观锁的标记，CAS 校验 */
    protected volatile long rStamp;

    protected volatile long wStamp;

    public MdlLock(@NotNull MdlKey key) {
        this.key = key;
        this.latch = new StampedLock();
    }

    public abstract long writeLock();

    public abstract long readLock();

    public abstract void unlockWrite(long stamp);

    public abstract void unlockRead(long stamp);

    public abstract boolean isLocked();

    /**
     * 加悲观读锁;
     */
    public synchronized void latchRead() {
        rStamp = latch.readLock();
    }

    public synchronized void unlatchRead() {
        if (rStamp != 0) {
            latch.unlockRead(rStamp);
            rStamp = 0;
        }
    }

    /**
     * 加写锁；
     * @return
     */
    public synchronized boolean latchWrite() {
        if (0 != wStamp) {
            // Already holding the lock, validate it
            final long stamp = latch.tryConvertToWriteLock(wStamp);

            /** 先前的写锁仍在，则抛异常；*/
            Assert.assertTrue(wStamp != stamp, "Unknown write stamp");
            return true;
        }

        // 乐观锁试探 是否存在 写锁;
        final long orStamp = latch.tryOptimisticRead();

        if (0 == orStamp) {
            return false;
        }

        wStamp = latch.tryConvertToWriteLock(orStamp);

        return 0 != wStamp;
    }

    public synchronized void unlatchWrite() {
        if (wStamp != 0) {
            latch.unlockWrite(wStamp);
            wStamp = 0;
        }
    }

    public MdlKey getKey() {
        return key;
    }
}
