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

package com.alibaba.polardbx.executor.mdl.context;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * 锁的所有者的上下文，每个前端连接都有一个对应的上下文;
 * 单个连接对应的锁;
 * 没有 用到 乐观锁机制，用到的是 更公平的读写锁;
 *
 *
 * @author chenmo.cm
 */
public class MdlContextStamped extends MdlContext {
    private static final Logger logger = LoggerFactory.getLogger(MdlContextStamped.class);

    /**
     * (trxId, traceId, sql, frontend);
     * 只用到了 trx 的标志;
     */
    public class TransactionInfo {
        private final long trxId;
        private final String traceId;
        private final ByteString sql;
        private final String frontend;

        public TransactionInfo(long trxId) {
            this.trxId = trxId;
            this.traceId = null;
            this.sql = null;
            this.frontend = null;
        }

        public TransactionInfo(long trxId, String traceId, ByteString sql, String frontend) {
            this.trxId = trxId;
            this.traceId = traceId;
            this.sql = sql;
            this.frontend = frontend;
        }

        public long getTrxId() {
            return trxId;
        }

        public String getTraceId() {
            return traceId;
        }

        public ByteString getSql() {
            return sql;
        }

        public String getFrontend() {
            return frontend;
        }

        @Override
        public int hashCode() {
            return (int) trxId;
        }

        @Override
        public boolean equals(Object obj) {
            return ((TransactionInfo) obj).trxId == trxId;
        }
    }

    /**
     * 当前上下文中已经获取到的锁, 按照事务 ID 分组；
     * 注意 MdlTicket 是引用 MdlManagerStamped 的;
     */
    private final Map<TransactionInfo, Map<MdlKey, MdlTicket>> tickets;
    /**
     * lock for protection of local field tickets;
     * 当读写锁 保护 tickets，因为 遍历 tickets 操作不是线程安全的;
     */
    private final StampedLock lock = new StampedLock();

    public MdlContextStamped(String connId) {
        super(connId);
        this.tickets = new ConcurrentHashMap<>();
    }

    // For show metadata lock.
    public Map<TransactionInfo, Map<MdlKey, MdlTicket>> getTickets() {
        return tickets;
    }

    /**
     * trx 有对应的 MdlRequest，根据 MdlRequest 上悲观读写锁;
     *
     * schema -> MdlManagerStamped -> request.getKey() -> MdlTicket;
     *
     * @param request MdlRequest
     * @return
     */
    @Override
    public MdlTicket acquireLock(@NotNull final MdlRequest request) {
        final MdlManager mdlManager = getMdlManager(request.getKey().getDbName());

        /** 为啥先要上 读锁？ tickets 本身可并发修改，但 遍历 tickets 操作不是线程安全的 */
        final long l = lock.readLock();
        try {
            final Map<MdlKey, MdlTicket> ticketMap = tickets.computeIfAbsent(
                new TransactionInfo(request.getTrxId(), request.getTraceId(), request.getSql(), request.getFrontend()),
                tid -> new ConcurrentHashMap<>());

            // 注意 mdlManager 中也有一份tickets
            return ticketMap.compute(request.getKey(), (key, ticket) -> {
                if (null == ticket || !ticket.isValidate()) {
                    /** 不存在才要重新创建 */
                    ticket = mdlManager.acquireLock(request, this);
                }
                return ticket;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    @Override
    public List<MdlTicket> getWaitFor(MdlKey mdlKey) {
        final MdlManager mdlManager = getMdlManager(mdlKey.getDbName());
        List<MdlTicket> waitForList = mdlManager.getWaitFor(mdlKey);
        waitForList.removeIf(e->e!=null && e.getContext()==this);
        return waitForList;
    }

    /**
     * 释放 MdlTicket key 相关的所有锁;
     * @param trxId transaction id
     * @param ticket mdl ticket
     */
    @Override
    public void releaseLock(@NotNull Long trxId, @NotNull final MdlTicket ticket) {
        final MdlManager mdlManager = getMdlManager(ticket.getLock().getKey().getDbName());

        final long l = lock.readLock();
        try {
            tickets.computeIfPresent(new TransactionInfo(trxId), (tid, ticketMap) -> {
                ticketMap.computeIfPresent(ticket.getLock().getKey(), (k, t) -> {
                    if (t.isValidate()) {
                        mdlManager.releaseLock(ticket);
                    }
                    return null;
                });

                return ticketMap.isEmpty() ? null : ticketMap;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    /**
     * 释放 trxId 对应的所有锁；
     * 用于 finally 中保护;
     *
     * @param trxId
     */
    @Override
    public void releaseTransactionalLocks(Long trxId) {
        final long l = lock.readLock();
        try {
            tickets.computeIfPresent(new TransactionInfo(trxId), (tid, ticketMap) -> {
                releaseTransactionalLocks(ticketMap);

                return ticketMap.isEmpty() ? null : ticketMap;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    /**
     * 清空上下文相关的所有 tickets；
     * 唯一 lock 上写锁的地方，主要是 遍历 tickets 操作不是线程安全的;
     */
    @Override
    public void releaseAllTransactionalLocks() {

        // make sure no more mdl acquired during releasing all of current mdl
        final long l = lock.writeLock();
        try {
            final Iterator<Entry<TransactionInfo, Map<MdlKey, MdlTicket>>> it = tickets.entrySet().iterator();
            while (it.hasNext()) {
                final Map<MdlKey, MdlTicket> v = it.next().getValue();

                releaseTransactionalLocks(v);

                if (v.isEmpty()) {
                    it.remove();
                }
            }
        } finally {
            lock.unlockWrite(l);
        }
    }

    @Override
    protected MdlManager getMdlManager(String schema) {
        return MdlManager.getInstance(schema);
    }

    @Override
    public String toString() {
        return "MdlContextStamped{" +
            "tickets: " + tickets.size() +
            ", lock=" + lock +
            '}';
    }
}
