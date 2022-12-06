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

import com.google.common.base.Preconditions;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

/**
 * MDL锁的持有者
 * 通常是一个frontend connectionId。
 * 也可以是一个schemaName。
 * OPTIMIZER_LATCHES 乐观锁 并没有被使用；
 * TConnection 必须通过 MdlContext 来上锁， MdlContext 由 MdlManager 创建;
 *
 * @author chenmo.cm
 */
public abstract class MdlContext {

    public static final String MDL_CONTEXT = "MDL_CONTEXT";

    private static final int LATCH_COUNT = 16;
    /**
     * 全局 OPTIMIZER_LATCHES;
     * StampedLock 是读写锁的改进，有乐观锁/悲观锁/写锁，内部有等待队列进行公平排序;
     * 它的思想是读写锁中读不仅不阻塞读，同时也不应该阻塞写;
     * tryOptimisticRead 在读的时候如果发生了写，则应当重读（乐观锁转换为悲观锁）而不是在读的时候直接阻塞写, 类似CAS;
     * 解决 因为在读线程非常多而写线程比较少的情况下，写线程可能发生饥饿现象; */
    private static final StampedLock[] OPTIMIZER_LATCHES = new StampedLock[LATCH_COUNT];

    static {
        IntStream.range(0, LATCH_COUNT).forEach(i -> OPTIMIZER_LATCHES[i] = new StampedLock());
    }

    /**
     * Id of the owner of metadata locks. I.e. each server connection has
     * such an Id.
     */
    final String connId;

    protected MdlContext(String connId) {
        this.connId = connId;
    }

    /**
     * acquires the lock, blocking if necessary until available.
     *
     * @param request MdlRequest
     * @return MdlTicket
     */
    public abstract MdlTicket acquireLock(@NotNull MdlRequest request);

    public abstract List<MdlTicket> getWaitFor(MdlKey mdlKey);

    /**
     * release the lock
     *
     * @param trxId transaction id
     * @param ticket mdl ticket
     * @return MdlTicket
     */
    public abstract void releaseLock(@NotNull Long trxId, @NotNull MdlTicket ticket);

    /**
     * release MDL_TRANSACTION lock by transaction id
     */
    public abstract void releaseTransactionalLocks(@NotNull Long trxId);

    /**
     * release all MDL_TRANSACTION and MDL_STATEMENT locks
     */
    public abstract void releaseAllTransactionalLocks();

    public String getConnId() {
        return connId;
    }

    protected abstract MdlManager getMdlManager(String schema);

    protected void releaseTransactionalLocks(Map<MdlKey, MdlTicket> ticketMap) {
        Preconditions.checkNotNull(ticketMap);

        final Iterator<Entry<MdlKey, MdlTicket>> it = ticketMap.entrySet().iterator();
        while (it.hasNext()) {
            final Entry<MdlKey, MdlTicket> entry = it.next();
            final MdlTicket t = entry.getValue();
            final MdlKey k = entry.getKey();

            if (t.isValidate()) {
                if (!t.getDuration().transactional()) {
                    continue;
                }

                // release validate and transactional lock
                final MdlManager mdlManager = getMdlManager(k.getDbName());
                mdlManager.releaseLock(t);
            }

            it.remove();
        }

    }

    /**
     * 获取 OPTIMIZER_LATCHES  对应的 StampedLock；
     * @param schema
     * @param table
     * @return
     */
    private static StampedLock getLatch(String schema, String table) {
        final int latchIndex = getLatchIndex(schema, table);

        if (null == OPTIMIZER_LATCHES[latchIndex]) {
            synchronized (OPTIMIZER_LATCHES) {
                if (null == OPTIMIZER_LATCHES[latchIndex]) {
                    OPTIMIZER_LATCHES[latchIndex] = new StampedLock();
                }
            }
        }
        return OPTIMIZER_LATCHES[latchIndex];
    }

    private static int getLatchIndex(String schema, String table) {
        final int hash = (schema + table).toLowerCase().hashCode();
        return Math.floorMod(hash, LATCH_COUNT);
    }

    /**
     * 获取 OPTIMIZER_LATCHES StampedLock 乐观锁的 stamp;
     * 乐观锁 只用于 plan阶段？
     * @return
     */
    public static long[] snapshotMetaVersions() {
        long[] stamps = new long[LATCH_COUNT];
        IntStream.range(0, LATCH_COUNT).forEach(i -> stamps[i] = OPTIMIZER_LATCHES[i].tryOptimisticRead());
        return stamps;
    }

    public static boolean validateMetaVersion(String schema, String table, long[] stamps) {
        final int latchIndex = getLatchIndex(schema, table);

        return OPTIMIZER_LATCHES[latchIndex].validate(stamps[latchIndex]);
    }

    /**
     * 居然没有调用。。相当于没有使用;
     *
     * @param schema
     * @param table
     */
    public static void updateMetaVersion(String schema, String table) {
        final StampedLock latch = getLatch(schema, table);
        latch.unlockWrite(latch.writeLock());
    }

    @Override
    public String toString() {
        return "MdlContext{" +
            "connId='" + connId + '\'' +
            '}';
    }
}
