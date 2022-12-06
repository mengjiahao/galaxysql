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

package com.alibaba.polardbx.gms.listener.impl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.scheduler.MetaDbCleanManager;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.topology.ConfigListenerRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manager all listener for opVersion update of dataId;
 *
 * @author chenghui.lch
 *
 * 做轮洵与定时任务 通过 ConfigListenerAccessor 探测 metadb 系统表 config_listener, 通知调用 onHandleConfig;
 * metadb 通过 config_listener 实现 通知与订阅 功能;
 * 类似于 ZK 通知订阅机制，比如路径 /a/b 下删除 b，要通知 /a;
 *
 * DDL job 中 TableMetaChanger 会 register;
 */
public class MetaDbConfigManager extends AbstractLifecycle implements ConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(MetaDbConfigManager.class);

    protected final static int MAX_QUEUE_LEN = 100;

    public final static int DEFAULT_NOTIFY_INTERVAL = 1000;
    public final static int DEFAULT_SCAN_INTERVAL = 1000;
    public final static int DEFAULT_CLEAN_INTERVAL = 10000;

    // time interval for scaning the gmtModified of dataId, unit: min
    protected static int TIME_INTERVAL_FOR_SCAN_MODIFIED_DATA_ID = 120;

    /** 业务线程注册 (dataId, record.opVersion, ConfigListener) 到 dataIdContextMap; */
    protected Map<String, DataIdContext> dataIdContextMap = new ConcurrentHashMap<>();

    protected volatile Date lastScanTimestamp = null;
    protected final Scanner scanner = new Scanner(this);
    protected final Notifier notifier = new Notifier(this);

    /** 每秒轮询一次; dataIdScanTaskExecutor 产生事件; dataIdNotifyTaskExecutor 分发事件; listenerTaskExecutor 处理事件; */
    protected final ScheduledExecutorService dataIdScanTaskExecutor = Executors
        .newSingleThreadScheduledExecutor(new NamedThreadFactory("DataId-Scanner-Executor", true));
    protected final ScheduledExecutorService dataIdNotifyTaskExecutor = Executors
        .newSingleThreadScheduledExecutor(new NamedThreadFactory("DataId-Notifier-Executor", true));
    protected BlockingQueue<DataIdContext> completeListenTaskQueue = new ArrayBlockingQueue<DataIdContext>(MAX_QUEUE_LEN * 10);
    protected final ScheduledExecutorService cleanTaskExecutor = Executors
        .newSingleThreadScheduledExecutor(new NamedThreadFactory("DataId-Scanner-Executor", true));

    protected int listenerTaskExecutorPoolSize = 4;
    protected ThreadPoolExecutor listenerTaskExecutor =
        ExecutorUtil.createExecutor("ListenerTaskExecutor", listenerTaskExecutorPoolSize);

    protected static MetaDbConfigManager instance = new MetaDbConfigManager();

    /**
     * DataIdContext maintains its config listener list and the tasks from config listeners.
     * 本质上 DataIdContext 相当于一个通知要处理 dataId 的信号，不包含具体要做的事件;
     */
    protected static class DataIdContext {
        protected String dataId;
        /** 有序版本号，被 onHandleConfig 处理后会增加;
         * 更新内存中 dataIdContext.currOpVersion 标志本节点已处理;
         * 可能被 listenerTaskExecutor 多线程并发修改; */
        protected volatile long currOpVersion = -1;
        /** 事件队列; 队列事件可并行执行, 只要确保处理最新opVersion事件; */
        protected Deque<OpVersionChangeEvent> changeEventQueue = new LinkedBlockingDeque<>(MAX_QUEUE_LEN);

        protected volatile ConfigListener dataIdListener = null;
        /** 感觉用处不大; */
        protected volatile Future listenerTaskFuture = null;
        protected volatile boolean isRemoved = false;
        /** 保护 listenerTaskExecutor 多线程 处理同一个dataId的事件 onHandleConfig */
        protected ReentrantLock handlingLock = new ReentrantLock();

        public DataIdContext(String dataId, long opVer, ConfigListener configListener) {
            this.dataId = dataId;
            this.currOpVersion = opVer;
            this.dataIdListener = configListener;
        }
    }

    /**
     * OpVersionChangeEvent means (dataId, opVersion, changeTimestamp);
     */
    protected static class OpVersionChangeEvent {
        protected String dataId;
        protected long opVersion;
        protected Timestamp changeTimestamp;

        public OpVersionChangeEvent(String dataId, long opVersion, Timestamp changeTimestamp) {
            this.dataId = dataId;
            this.opVersion = opVersion;
            this.changeTimestamp = changeTimestamp;
        }
    }

    /**
     * 从 meta config_listener 表中扫描事件信息，加入相应 record.opVersion 事件到队列，与其他节点 定时同步事件 opVersion;
     * 每1s 轮询scan;
     */
    protected static class Scanner implements Runnable {

        MetaDbConfigManager manager;

        public Scanner(MetaDbConfigManager manager) {
            this.manager = manager;
        }

        @Override
        public void run() {
            runInner();
        }

        protected void runInner() {
            fetchOpVersionChangeEvents();
        }

        protected void fetchOpVersionChangeEvents() {
            try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                configListenerAccessor.setConnection(conn);
                List<ConfigListenerRecord> datas = null;
                // 从 config_listener 表中拉取信息
                if (manager.lastScanTimestamp == null) {
                    // First scan, scan all dataId
                    datas = configListenerAccessor.getAllDataIds();
                } else {
                    /**
                     * scan all the data_id list that their op_version  are modified in last 2 hours (default)
                     */
                    datas =
                        configListenerAccessor.getDataIds(MetaDbConfigManager.TIME_INTERVAL_FOR_SCAN_MODIFIED_DATA_ID);
                }

                // 扫描 config_listener 表中记录
                for (int i = 0; i < datas.size(); i++) {
                    ConfigListenerRecord record = datas.get(i);
                    String dataId = record.dataId;
                    int dataIdStatus = record.status;
                    long newOpVersion = record.opVersion;

                    /** 只看在 dataIdContextMap 已经存在的? **/
                    if (manager.dataIdContextMap.containsKey(dataId)) {

                        if (dataIdStatus == ConfigListenerRecord.DATA_ID_STATUS_REMOVED) {
                            //manager.disableListenerByDataId(dataId);
                            continue;
                        }

                        Timestamp gmtModified = record.gmtModified;
                        DataIdContext dataIdContext = manager.dataIdContextMap.get(dataId);

                        // Check if newOpVer has already exist in  dataIdInfo.changeEventQueue
                        // if exists, should ignored
                        synchronized (dataIdContext) {
                            /** dataIdContext.currOpVersion < newOpVersion 代表存在还未处理的事件(远程节点通知的)*/
                            // 需要确保 dataIdContext.changeEventQueue 最后的事件的版本号 >= 持久化的newOpVersion
                            if (dataIdContext.currOpVersion < newOpVersion) {
                                if (dataIdContext.dataIdListener != null) {
                                    OpVersionChangeEvent lastChangeEvent = dataIdContext.changeEventQueue.peekLast();
                                    int queueSize = dataIdContext.changeEventQueue.size();
                                    boolean needAddNewEvent = true;
                                    // lastChangeEvent.opVersion >= newOpVersion 表示事件继续要处理
                                    boolean lastEventOpVerHigherNewOpVer = lastChangeEvent == null ? false : lastChangeEvent.opVersion >= newOpVersion;
                                    if (lastChangeEvent != null && lastEventOpVerHigherNewOpVer) {
                                        // 保留 >= newOpVersion 的事件
                                        needAddNewEvent = false;
                                    }
                                    if (needAddNewEvent) {
                                        /** LastChangeEvent 事件版本号更新为 newOpVersion */
                                        boolean needRemoveLastChangeEvent = false;
                                        if (queueSize > 1 && !lastEventOpVerHigherNewOpVer) {
                                            needRemoveLastChangeEvent = true;
                                        }
                                        if (needRemoveLastChangeEvent) {
                                            /**
                                             * remove the last event because new opVer is higher
                                             */
                                            dataIdContext.changeEventQueue.removeLast();
                                        }
                                        // 注册record.opVersion事件
                                        dataIdContext.changeEventQueue
                                            .offerLast(new OpVersionChangeEvent(dataId, newOpVersion, gmtModified));
                                    }
                                }
                            }
                        }
                    }
                }
                manager.lastScanTimestamp = new Date();
            } catch (Throwable ex) {
                logger.warn(ex);
                MetaDbLogUtil.META_DB_LOG.warn(ex);
            }
        }
    }

    /**
     * 每 1s 扫描本地内存 dataIdContextMap 分发事件处理，调用 listenerTaskExecutor 工作线程池处理 每个 DataIdContext 的事件;
     */
    protected static class Notifier implements Runnable {

        MetaDbConfigManager manager;

        public Notifier(MetaDbConfigManager manager) {
            this.manager = manager;
        }

        @Override
        public void run() {
            submitTaskForOpVersionChangeEvents();
        }

        protected void submitTaskForOpVersionChangeEvents() {

            while (true) {

                try {
                    Map<String, DataIdContext> dataIdInfoMap = manager.dataIdContextMap;
                    for (Map.Entry<String, DataIdContext> dataIdInfoItem : dataIdInfoMap.entrySet()) {
                        DataIdContext dataIdInfo = dataIdInfoItem.getValue();
                        if (dataIdInfo.dataIdListener == null || dataIdInfo.changeEventQueue.isEmpty()) {
                            continue;
                        }
                        if (dataIdInfo.listenerTaskFuture == null) {
                            // listenerTaskExecutor 工作线程池处理 每个 DataIdContext 的事件;
                            Future taskFuture =
                                manager.listenerTaskExecutor
                                    .submit(new ListenerTask(dataIdInfo, manager.completeListenTaskQueue));
                            dataIdInfo.listenerTaskFuture = taskFuture;
                        }
                    }

                    // 处理所有工作线程执行的结果
                    BlockingQueue<DataIdContext> queue = manager.completeListenTaskQueue;
                    while (!queue.isEmpty()) {
                        DataIdContext dataIdInfo = queue.take();
                        if (!dataIdInfo.isRemoved && !dataIdInfo.changeEventQueue.isEmpty()) {
                            // 继续处理 DataIdContext.changeEventQueue 中剩下的事件;
                            Future taskFuture =
                                manager.listenerTaskExecutor
                                    .submit(new ListenerTask(dataIdInfo, manager.completeListenTaskQueue));
                            dataIdInfo.listenerTaskFuture = taskFuture;
                        } else {
                            dataIdInfo.listenerTaskFuture = null;
                        }
                    }
                    Thread.sleep(DEFAULT_NOTIFY_INTERVAL);

                } catch (Throwable ex) {
                    logger.warn(ex);
                    MetaDbLogUtil.META_DB_LOG.warn(ex);
                }

            }
        }
    }

    /**
     * 处理 dataIdContext.changeEventQueue 中头部的 OpVersionChangeEvent, 调用 dataIdListener.onHandleConfig;
     * completeListenTaskQueue 共享队列 保存返回值;
     */
    protected static class ListenerTask implements Callable<Boolean> {
        DataIdContext dataIdContext;
        protected BlockingQueue completeListenTaskQueue;

        public ListenerTask(DataIdContext dataIdInfo, BlockingQueue completeListenTaskQueue) {
            this.dataIdContext = dataIdInfo;
            this.completeListenTaskQueue = completeListenTaskQueue;
        }

        @Override
        public Boolean call() throws Exception {
            OpVersionChangeEvent opVersionChangeEvent = dataIdContext.changeEventQueue.peek();
            long opVersionToBeRefresh = opVersionChangeEvent.opVersion;
            String dataId = dataIdContext.dataId;
            try {
                // 处理事件 dataIdListener.onHandleConfig;
                boolean result = handleListenerAndRefreshOpVersion(dataId, opVersionToBeRefresh, false);
                if (result) {
                    // clear op event
                    synchronized (dataIdContext) {
                        dataIdContext.changeEventQueue.pollFirst();
                    }
                    completeListenTaskQueue.add(dataIdContext);

                }
                return result;
            } catch (Throwable ex) {
                logger.warn(ex);
                MetaDbLogUtil.META_DB_LOG.warn(ex);
                return false;
            }
        }
    }

    protected MetaDbConfigManager() {
    }

    /**
     * 多线程访问单例模式;
     * @return
     */
    public static MetaDbConfigManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    @Override
    protected void doInit() {
        dataIdNotifyTaskExecutor.submit(notifier);
        dataIdScanTaskExecutor
            .scheduleAtFixedRate(scanner, DEFAULT_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL, TimeUnit.MILLISECONDS);
        //reuse the lock to start cleaner
        MetaDbCleanManager.getInstance();
    }

    /**
     * 业务线程绑定 (dataId, metadb.opVersion, ConfigListener)事件订阅 到内存态 dataIdContextMap;
     * 注意是使用 metadb 内 dataId相关的opVersion 注册 listener;
     *
     * @param dataId a dataId that the caller defines
     * @param listener a config listener that the caller uses to handle its own config data,
     */
    @Override
    public void bindListener(String dataId, ConfigListener listener) {

        ConfigListenerRecord record = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            // 单条语句就直接设置自动提交
            conn.setAutoCommit(true);
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(conn);
            /** 注意使用的是 metadb opVersion */
            record = configListenerAccessor.getDataId(dataId, false);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        if (record != null) {
            // enable listener and subscribe the change of the dataId
            enableListenerByDataId(dataId, record.opVersion, listener);
        }
    }

    /**
     * 注册 (dataId, opVersion, ConfigListener) 到内存态 dataIdContextMap;
     * @param dataId a dataId that the caller defines
     * @param opVersion the verison
     * @param listener dataId 的事件处理方法;
     */
    @Override
    public void bindListener(String dataId, long opVersion, ConfigListener listener) {
        // enable listener and subscribe the change of the dataId
        enableListenerByDataId(dataId, opVersion, listener);
    }

    @Override
    public void bindListeners(String dataIdPrefix, Map<String, ConfigListener> listeners) {
        List<ConfigListenerRecord> records = null;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(true);
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(conn);
            records = configListenerAccessor.getDataIdsByPrefix(dataIdPrefix);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        for (int i = 0; i < records.size(); i++) {
            ConfigListenerRecord record = records.get(i);
            ConfigListener listener = listeners.get(record.dataId);
            if (listener != null) {
                // enable listener and subscribe the change of the dataId
                enableListenerByDataId(record.dataId, record.opVersion, listener);
            }
        }
    }

    /**
     * 注册需要关注的新的 dataId 到 metadb config_listener，确保注册成功;
     * @param dataId
     * @param trxConn
     */
    @Override
    public void register(String dataId, Connection trxConn) {
        // add dataId into MetaDB
        addDataIdInfoIntoDb(dataId, trxConn);
    }

    @Override
    public void unbindListener(String dataId) {
        // disable the listener and ignored the change of the dataId
        disableListenerByDataId(dataId);
    }

    /**
     * 在 metadb.config_listener 中删除 dataId;
     * @param dataId polardbx.meta.table.test.tb2
     * @param trxConn
     */
    @Override
    public void unregister(String dataId, Connection trxConn) {
        removeDataId(dataId, trxConn);
    }

    /**
     * 通过事务 自增metadb的 config_listener 中 opVersion，后续其他节点可异步scan到;
     *
     * @param dataId polardbx.meta.tables.test
     * @param conn
     * @return
     */
    @Override
    public long notify(String dataId, Connection conn) {

        long opVer = -1;
        if (conn == null) {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                metaDbConn.setAutoCommit(true);
                ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                configListenerAccessor.setConnection(metaDbConn);
                opVer = configListenerAccessor.updateOpVersion(dataId);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        } else {
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(conn);
            opVer = configListenerAccessor.updateOpVersion(dataId);
        }

        logger.info("dataid: " + dataId + " version updated to: " + opVer);
        return opVer;
    }

    @Override
    public void notifyMultiple(List<String> dataIds, Connection conn, boolean ignoreCntError) {
        if (conn == null) {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                metaDbConn.setAutoCommit(true);
                ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                configListenerAccessor.setConnection(metaDbConn);
                configListenerAccessor.updateMultipleOpVersion(dataIds, ignoreCntError);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        } else {
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(conn);
            configListenerAccessor.updateMultipleOpVersion(dataIds, ignoreCntError);
        }
    }

    /**
     * 通过 GmsSyncManagerHelper 进行多节点sync广播;
     * 通知从 metadb 中获取 opVersion 相应的事件进行处理;
     *
     * @param dataId
     */
    @Override
    public void sync(String dataId) {
        doConfigListenerBySync(dataId, SystemDbHelper.DEFAULT_DB_NAME);
        return;
    }

    /**
     * 通过事务 注册新的 (dataId, DATA_ID_STATUS_NORMAL, DEFAULT_OP_VERSION) 初始事件到 config_listener 表， 如果存在则返回信息;
     * @param dataId
     * @param metaDbConn
     * @return 如果存在则直接返回，不存在则不断尝试直至加入成功
     */
    protected ConfigListenerRecord addDataIdInfoIntoDb(String dataId, Connection metaDbConn) {
        try {
            ConfigListenerRecord dataIdInfo = null;
            if (metaDbConn == null) {
                try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                    ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                    configListenerAccessor.setConnection(conn);
                    try {
                        // 注意是手动事务
                        conn.setAutoCommit(false);

                        // add dataId into metaDB
                        while (true) {
                            try {
                                // select for update config_listener 表上锁;
                                dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                                if (dataIdInfo != null
                                    && dataIdInfo.status == ConfigListenerRecord.DATA_ID_STATUS_NORMAL) {
                                    // dataId has already exists, so ignore
                                    break;
                                }

                                // 初始化 (dataId, DATA_ID_STATUS_NORMAL, DEFAULT_OP_VERSION) 到 config_listener;
                                configListenerAccessor
                                    .addDataId(dataId, ConfigListenerRecord.DATA_ID_STATUS_NORMAL,
                                        ConfigListenerAccessor.DEFAULT_OP_VERSION);
                                break;
                            } catch (Exception e) {
                                if (e.getMessage().toLowerCase().contains("deadlock found")) {
                                    // 锁冲突则随机回退继续尝试加入
                                    Random rnd = new Random();
                                    int randWaitTime = Math.abs(rnd.nextInt(500) + 1);
                                    try {
                                        Thread.sleep(randWaitTime);
                                    } catch (Throwable ex) {
                                        // ignore
                                    }
                                    dataIdInfo = configListenerAccessor.getDataId(dataId, false);
                                    if (dataIdInfo != null) {
                                        break;
                                    }
                                } else {
                                    throw e;
                                }
                            }
                        }
                        conn.commit();

                        // Select only
                        if (dataIdInfo == null) {
                            dataIdInfo = configListenerAccessor.getDataId(dataId, false);
                        }
                        return dataIdInfo;
                    } catch (Throwable e) {
                        conn.rollback();
                        throw e;
                    } finally {
                        conn.setAutoCommit(true);
                    }
                } catch (Throwable ex) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed to add dataId[%s] into metaDb", dataId), ex);
                }
            } else {
                ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                configListenerAccessor.setConnection(metaDbConn);
                dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                if (dataIdInfo != null && dataIdInfo.status == ConfigListenerRecord.DATA_ID_STATUS_NORMAL) {
                    return dataIdInfo;
                }
                configListenerAccessor
                    .addDataId(dataId, ConfigListenerRecord.DATA_ID_STATUS_NORMAL,
                        ConfigListenerAccessor.DEFAULT_OP_VERSION);
                // Select and lock after insert
                dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                return dataIdInfo;
            }

        } catch (Throwable ex) {
            throw ex;
        }
    }

    protected void removeDataId(String dataId, Connection trxConn) {
        ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
        if (trxConn != null) {
            configListenerAccessor.setConnection(trxConn);
            configListenerAccessor.deleteDataId(dataId);
        } else {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                configListenerAccessor.setConnection(metaDbConn);
                configListenerAccessor.deleteDataId(dataId);
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
        }
    }

    /**
     * 注册 (dataId, opVersion, ConfigListener) 到内存态 dataIdContextMap;
     * @param dataId
     * @param opVersion
     * @param listener
     */
    protected void enableListenerByDataId(String dataId, long opVersion, ConfigListener listener) {
        DataIdContext dataIdContext = dataIdContextMap.get(dataId);
        if (dataIdContext == null) {
            dataIdContext = new DataIdContext(dataId, opVersion, listener);
            dataIdContextMap.put(dataId, dataIdContext);
        } else {
            // use the last registered listener as the dataId listener
            // 存在dataId 则修改 ConfigListener
            dataIdContext.dataIdListener = listener;
        }
    }

    protected void disableListenerByDataId(String dataId) {
        dataIdContextMap.remove(dataId);
    }

    /**
     * 直接同步元数据，从 metadb 中获取 (dataId, opVersion) 相应的事件进行处理;
     */
    public static class MetaDbConfigSyncAction implements IGmsSyncAction {

        private String dataId;

        public MetaDbConfigSyncAction() {
        }

        public MetaDbConfigSyncAction(String dataId) {
            this.dataId = dataId;
        }

        @Override
        public Object sync() {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                configListenerAccessor.setConnection(metaDbConn);
                ConfigListenerRecord record = configListenerAccessor.getDataId(dataId, false);
                if (record != null) {
                    long opVersion = record.opVersion;
                    handleListenerAndRefreshOpVersion(dataId, opVersion, true);
                }
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
            return null;
        }

        public String getDataId() {
            return dataId;
        }

        public void setDataId(String dataId) {
            this.dataId = dataId;
        }
    }

    /**
     * 通过 GmsSyncManagerHelper 进行多节点同步广播处理 MetaDbConfigSyncAction;
     * @param dataId
     * @param schemaName
     */
    protected void doConfigListenerBySync(String dataId, String schemaName) {
        DataIdContext dataIdContext = dataIdContextMap.get(dataId);
        if (dataIdContext != null) {
            GmsSyncManagerHelper.sync(new MetaDbConfigSyncAction(dataId), schemaName);
        }
    }

    /**
     *
     * 处理dataIdContextMap 中单个事件 dataIdListener.onHandleConfig(dataIdContext.dataId, opVersionToBeRefresh);
     * 更新内存中 dataIdContext.currOpVersion 标志本节点已处理;
     *
     * @param dataId 需要在 dataIdContextMap 存在
     * @param opVersionToBeRefresh 事件的版本号; dataIdContext.currOpVersion < opVersionToBeRefresh 才会被处理
     * @param isSync 是否是 sync 还是 notify 方法调用
     * @return 一般都是true
     */
    protected static boolean handleListenerAndRefreshOpVersion(String dataId, long opVersionToBeRefresh,
                                                               boolean isSync) {

        DataIdContext dataIdContext = MetaDbConfigManager.getInstance().dataIdContextMap.get(dataId);
        if (dataIdContext == null) {
            // ignore
            logDynamicConfig(dataId, null, 0L, opVersionToBeRefresh, isSync, true, false, true, 0);
            return true;
        }

        // Handle the listener is registered
        ConfigListener listener = dataIdContext.dataIdListener;
        long currOpVersion = -1;
        long st = 0;
        long et = 0;

        // 保护 listenerTaskExecutor 多线程 处理同一个dataId的事件 onHandleConfig
        dataIdContext.handlingLock.lock();
        try {
            // Check opVersion
            currOpVersion = dataIdContext.currOpVersion;
            if (currOpVersion >= opVersionToBeRefresh) {
                // Ignore to update version
                // dataIdContext.currOpVersion >= opVersionToBeRefresh 的被处理过，忽略;
                logDynamicConfig(dataId, listener, currOpVersion, opVersionToBeRefresh, isSync, true, true, true, 0);
                return true;
            }

            if (listener == null) {
                // Update op version to latest version
                dataIdContext.currOpVersion = opVersionToBeRefresh;
                logDynamicConfig(dataId, null, currOpVersion, opVersionToBeRefresh, isSync, true, true, true, 0);
                return true;
            }
            st = System.nanoTime();
            /**
             * 核心触发 listener.onHandleConfig；
             * 新加table 会调用 TableListListener, 订阅新的table 到 CONFIG_MANAGER;
             **/
            listener.onHandleConfig(dataIdContext.dataId, opVersionToBeRefresh);

            // Update op version to latest version
            /** 标志已通知 **/
            dataIdContext.currOpVersion = opVersionToBeRefresh;

            et = System.nanoTime();
            logDynamicConfig(dataId, listener, currOpVersion, opVersionToBeRefresh, isSync, false, true, true, et - st);
        } catch (Throwable ex) {
            logDynamicConfig(dataId, listener, currOpVersion, opVersionToBeRefresh, isSync, false, true, false,
                System.nanoTime() - st);
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw ex;
        } finally {
            dataIdContext.handlingLock.unlock();
        }
        return true;
    }

    private static void logDynamicConfig(String dataId, ConfigListener listener, long oldVer, long newVer,
                                         boolean isFromSync, boolean isIgnored, boolean isBound,
                                         boolean isSucc, long callListenerCostTime) {
        String listenerClassName = listener != null ? listener.getClass().getSimpleName() : "None";
        String logMsgTemplate =
            "[MetaDB Config Change] DataId[%s]-Version[old:%s, new:%s]-Listener(%s)[isSync:%s, isIgnored:%s, isBound:%s, isSucc:%s, handleTime: %.3f ms]";
        String logMsg =
            String.format(logMsgTemplate, dataId, oldVer, newVer, listenerClassName, isFromSync, isIgnored, isBound,
                isSucc,
                callListenerCostTime * 1.0 / 1000000.0);
        MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(logMsg);
    }

    private static void logLocalSyncConfig(String dataId, ConfigListener listener, long oldVer, long newVer,
                                           boolean isSucc, long callListenerCostTime) {
        String listenerClassName = listener != null ? listener.getClass().getSimpleName() : "None";
        String logMsgTemplate =
            "[MetaDB Local Sync] DataId[%s]-Version[old:%s, new:%s]-Listener(%s)[isSucc:%s, handleTime: %.3f ms]";
        String logMsg =
            String.format(logMsgTemplate, dataId, oldVer, newVer, listenerClassName, isSucc,
                callListenerCostTime * 1.0 / 1000000.0);
        MetaDbLogUtil.META_DB_DYNAMIC_CONFIG.info(logMsg);
    }

    /**
     * 本节点直接调用 onHandleConfig 处理单个 dataId 相关的事件;
     * 注意不增加 currOpVersion， 也不检查 metadb 的 opVersion;
     *
     * @param dataId
     * @return
     */
    @Override
    public boolean localSync(String dataId) {
        DataIdContext dataIdContext = dataIdContextMap.get(dataId);
        if (dataIdContext == null) {
            logLocalSyncConfig(dataId, null, 0, 0, false, 0);
            return false;
        }

        long st = System.nanoTime();
        // Handle the listener is registered
        ConfigListener listener = dataIdContext.dataIdListener;
        long currOpVersion = dataIdContext.currOpVersion;
        dataIdContext.handlingLock.lock();
        try {
            long newOpVersion = dataIdContext.currOpVersion;
            if (listener == null) {
                logLocalSyncConfig(dataId, null, 0, 0, false, 0);
                return false;
            }

            if (newOpVersion > currOpVersion) {
                // currOpVersion 可能突然被多线程并发修改了
                logLocalSyncConfig(dataId, listener, currOpVersion, newOpVersion, false, 0);
                return false;
            }

            listener.onHandleConfig(dataIdContext.dataId, newOpVersion);

            long et = System.nanoTime();
            logLocalSyncConfig(dataId, listener, currOpVersion, newOpVersion, true, et - st);
        } catch (Throwable ex) {
            logLocalSyncConfig(dataId, listener, currOpVersion, currOpVersion, false,
                System.nanoTime() - st);
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw ex;
        } finally {
            dataIdContext.handlingLock.unlock();
        }
        return true;
    }
}
