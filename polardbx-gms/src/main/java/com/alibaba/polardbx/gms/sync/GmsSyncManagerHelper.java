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

package com.alibaba.polardbx.gms.sync;

import java.util.List;
import java.util.Map;

/**
 * 通过 SYNC_MANAGER(ClusterSyncManager) 单例实现 Worker Node到Leader Node的节点间通信;
 * 这种节点间通信机制不仅存在于DDL引擎加载DDL job过程中，也存在于DDL job执行时节点间的元数据同步过程中（见后文），
 * 这两种情境下封装的syncAction均会触发接收方从MetaDB中拉取实际的通信内容（DDL job或者元信息），
 * 这种通信机制以数据库服务作为通信中介保证了通信内容传递的高可用和一致性;
 */
public class GmsSyncManagerHelper {

    private static IGmsSyncManager SYNC_MANAGER;

    public static void setSyncManager(IGmsSyncManager syncManager) {
        SYNC_MANAGER = syncManager;
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName) {
        return sync(action, schemaName, false);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName,
                                                       boolean throwExceptions) {
        return SYNC_MANAGER.sync(action, schemaName, throwExceptions);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope) {
        return sync(action, schemaName, scope, false);
    }

    public static List<List<Map<String, Object>>> sync(IGmsSyncAction action, String schemaName, SyncScope scope,
                                                       boolean throwExceptions) {
        return SYNC_MANAGER.sync(action, schemaName, scope, throwExceptions);
    }

    public static void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler) {
        sync(action, schemaName, handler, false);
    }

    public static void sync(IGmsSyncAction action, String schemaName, ISyncResultHandler handler,
                            boolean throwExceptions) {
        SYNC_MANAGER.sync(action, schemaName, handler, throwExceptions);
    }

    public static void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler) {
        sync(action, schemaName, scope, handler, false);
    }

    public static void sync(IGmsSyncAction action, String schemaName, SyncScope scope, ISyncResultHandler handler,
                            boolean throwExceptions) {
        SYNC_MANAGER.sync(action, schemaName, scope, handler, throwExceptions);
    }

    /**
     * 集群间同步；
     *
     * @param action
     * @param schemaName
     * @param serverKey 一般用于发给 leader;
     * @return
     */
    public static List<Map<String, Object>> sync(IGmsSyncAction action, String schemaName, String serverKey) {
        return SYNC_MANAGER.sync(action, schemaName, serverKey);
    }
}
