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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;

/**
 * For PolarDB-X only.
 * 调用 SchemaManager.tonewversion 更新 schame缓存;
 */
public class TableMetaChangeSyncAction implements ISyncAction {
    protected final static Logger logger = LoggerFactory.getLogger(TableMetaChangeSyncAction.class);

    private String schemaName;
    private String primaryTableName;

    public TableMetaChangeSyncAction() {

    }

    public TableMetaChangeSyncAction(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.primaryTableName = tableName;
    }

    @Override
    public ResultCursor sync() {
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        ((GmsTableMetaManager) oldSchemaManager).tonewversion(primaryTableName);
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }
}
