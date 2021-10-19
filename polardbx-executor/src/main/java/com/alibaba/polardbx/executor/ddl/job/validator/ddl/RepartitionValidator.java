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

package com.alibaba.polardbx.executor.ddl.job.validator.ddl;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.gms.TableRuleManager;
import com.alibaba.polardbx.executor.gms.util.TableMetaUtil;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DDL_JOB_UNSUPPORTED;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_PARTITION_WITH_NON_PUBLIC_GSI;

public class RepartitionValidator {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    /**
     * 校验：
     * 1. 必须变成某种类型的表，不可能既不是拆分表，也不是广播表/单表
     * 2. 如果主键是auto_increment且没有sequence，则报错。提示用户手动创建sequence
     * 3. 所有GSI必须包含主表的主键和拆分列
     * 4. 禁止带GSI的拆分表变成单表/广播表
     * <p>
     * check:
     * 1. the target table must be one of: [partition table、broadcast table、single table]
     * 2. if the primary key is auto_increment, but there's no sequence, an error should be thrown.
     * 3. all GSIs should contain the pk&sk of the primary table
     * 4. altering a GSI-with-partition-table to single/broadcast table is forbidden
     */
    public static void validate(String schemaName,
                                String sourceTableName,
                                SqlNode dbPartitionBy,
                                boolean isBroadcast,
                                boolean isSingle) {

        //必须变成某种类型的表，不可能既不是拆分表，也不是广播表/单表
        if (dbPartitionBy == null && isBroadcast == false && isSingle == false) {
            LOGGER.warn("repartition rule unspecified, primary table name: " + sourceTableName);
            throw new TddlNestableRuntimeException("syntax error");
        }

        //不支持带有GSI的拆分表做拆分键变更（因为目前GSI中的数据无法随着新的拆分规则做更新）
        boolean hasAutoIncrement = false;
        TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(sourceTableName);
        for (ColumnMeta columnMeta : primaryTableMeta.getPrimaryKey()) {
            if (columnMeta.isAutoIncrement()) {
                hasAutoIncrement = true;
                break;
            }
        }

        /**
         * 如果没有自增列，用户一定是主动设置主键值的。所以直接允许做拆分变更
         * 如果有自增列，但是没有sequence，则必须要求先建sequence
         * if there's no auto_increment column, then user must specify the column value themselves, therefore we can allow
         * re-partition for this situation.
         * But if there's an auto_increment column without sequence, then a new sequence must be created before re-partition
         */
        if (hasAutoIncrement) {
            //source table must have sequence
            String seqName = AUTO_SEQ_PREFIX + sourceTableName;
            SequenceAttribute.Type existingSeqType =
                SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
            if (existingSeqType == SequenceAttribute.Type.NA) {
                LOGGER.warn(
                    String.format("unable to find sequence for auto_increment column in table %s", sourceTableName));
                final String errMsg =
                    String.format("Missing sequence for auto increment primary key. "
                        + "If you'd like to use a sequence, try: \n\t"
                        + "CREATE SEQUENCE AUTO_SEQ_%s", sourceTableName);
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MISSING_SEQUENCE, errMsg);
            }
        }

        List<TableMeta> gsiTableMeta =
            GlobalIndexMeta.getIndex(sourceTableName, schemaName, IndexStatus.ALL, null);

        //make sure there's no GSI before altering primary table to single or broadcast table
        if (CollectionUtils.isNotEmpty(gsiTableMeta)) {
            if (isBroadcast || isSingle) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_TO_SINGLE_OR_BROADCAST_WITH_GSI,
                    "Please drop all Global Indexes before altering any table to single or broadcast");
            }
        }
    }

    /**
     * check whether All the GSI contain the pk&sk after re-partitioning
     * cause, gsi is consist of [pk, sk of primary table, sk of gsi table]
     */
    public static void validateGsiColumns(
        String schemaName,
        String sourceTableName,
        boolean isSingle,
        boolean isBroadcast,
        SqlNode dbPartitionBy,
        SqlNode tablePartitionBy,
        ExecutionContext executionContext) {

        List<TableMeta> gsiTableMeta =
            GlobalIndexMeta.getIndex(sourceTableName, schemaName, IndexStatus.ALL, executionContext);
        if (CollectionUtils.isNotEmpty(gsiTableMeta)) {
            Set<String> expectedPkSkList =
                getExpectedPrimaryAndShardingKeys(
                    schemaName,
                    sourceTableName,
                    isSingle,
                    isBroadcast,
                    dbPartitionBy,
                    tablePartitionBy
                );
            for (TableMeta tableMeta : gsiTableMeta) {
                if (!GlobalIndexMeta.isPublished(executionContext, tableMeta)) {
                    String errMsg = "Please make sure all the Global Indexes are public";
                    LOGGER.warn(errMsg);
                    throw new TddlRuntimeException(ERR_PARTITION_WITH_NON_PUBLIC_GSI, errMsg);
                }
                Set<String> gsiColumnNameSet =
                    tableMeta.getAllColumns().stream().map(e -> e.getName().toLowerCase()).collect(Collectors.toSet());
                if (!CollectionUtils.isSubCollection(expectedPkSkList, gsiColumnNameSet)) {
                    String columnsThatGsiMustHave = String.join(",", expectedPkSkList);
                    String errMsg = String
                        .format("Please make sure all the Global Indexes contain column: [%s]", columnsThatGsiMustHave);
                    LOGGER.warn(errMsg);
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_GSI_MISSING_COLUMN, errMsg);
                }
            }
        }
    }

    /**
     * return the pk&sk after re-partitioning
     * 返回拆分键变更后的PK和SK
     */
    public static Set<String> getExpectedPrimaryAndShardingKeys(
        String schema,
        String logicalTableName,
        boolean isSingle,
        boolean isBroadcast,
        SqlNode dbPartitionBy,
        SqlNode tablePartitionBy
    ) {
        Set<String> result = new HashSet<>();
        TableMeta tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTableName);
        List<String> pkList = GlobalIndexMeta.getPrimaryKeys(tableMeta);
        Set<String> partitionColumnSet = new HashSet<>();
        if (!isSingle && !isBroadcast) {
            SqlNode[] dbPartitionColumns = ((SqlBasicCall) dbPartitionBy).getOperands();
            partitionColumnSet.addAll(
                Stream.of(dbPartitionColumns)
                    .filter(e -> e instanceof SqlIdentifier)
                    .map(e -> RelUtils.stringValue(e))
                    .collect(Collectors.toSet()));
            if (tablePartitionBy != null) {
                SqlNode[] tbPartitionColumns = ((SqlBasicCall) tablePartitionBy).getOperands();
                partitionColumnSet.addAll(
                    Stream.of(tbPartitionColumns)
                        .filter(e -> e instanceof SqlIdentifier)
                        .map(e -> RelUtils.stringValue(e))
                        .collect(Collectors.toSet()));
            }
        }

        result.addAll(pkList);
        result.addAll(partitionColumnSet);

        result = result.stream().map(String::toLowerCase).collect(Collectors.toSet());
        return result;
    }

    /**
     * 如果拆分规则与原先相同，则直接返回成功
     * return true is the partition rule is the same as before
     */
    public static boolean checkPartitionRuleUnchanged(String schemaName,
                                                      String sourceTableName,
                                                      boolean isSingle,
                                                      boolean isBroadcast,
                                                      TableRule targetTableRule) {
        TableRule primaryTableRule = TableRuleManager.getTableRules(schemaName).get(sourceTableName);

        if (checkPartitionRuleEquals(primaryTableRule, targetTableRule) && isBroadcast == false
            && isSingle == false) {
            return true;
        }
        TddlRuleManager ruleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        if (isBroadcast && ruleManager.isBroadCast(sourceTableName)) {
            return true;
        }
        if (isSingle && ruleManager.isTableInSingleDb(sourceTableName)) {
            return true;
        }
        return false;
    }

    /**
     * 判断新的拆分规则是否跟原先相同，如果相同则没有必要执行拆分键变更语句，直接拒绝
     */
    public static boolean checkPartitionRuleEquals(TableRule primaryRule, TableRule targetRule) {

        TablesExtRecord
            primary = TableMetaUtil.convertToTablesExtRecord(primaryRule, null, null, false, false);
        TablesExtRecord
            target = TableMetaUtil.convertToTablesExtRecord(targetRule, null, null, false, false);

        if (!StringUtils.equalsIgnoreCase(primary.dbPartitionKey, target.dbPartitionKey)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(primary.dbPartitionPolicy, target.dbPartitionPolicy)) {
            return false;
        }
        if (primary.dbPartitionCount != target.dbPartitionCount) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(primary.dbRule, target.dbRule)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(primary.tbPartitionKey, target.tbPartitionKey)) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(primary.tbPartitionPolicy, target.tbPartitionPolicy)) {
            return false;
        }
        if (primary.tbPartitionCount != target.tbPartitionCount) {
            return false;
        }
        if (!StringUtils.equalsIgnoreCase(primary.tbRule, target.tbRule)) {
            return false;
        }

        return true;
    }
}