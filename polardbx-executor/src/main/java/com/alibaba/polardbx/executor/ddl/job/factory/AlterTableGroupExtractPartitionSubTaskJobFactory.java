package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupExtractPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableGroupExtractPartitionPreparedData parentPrepareData;

    public AlterTableGroupExtractPartitionSubTaskJobFactory(DDL ddl,
                                                            AlterTableGroupExtractPartitionPreparedData parentPrepareData,
                                                            AlterTableGroupItemPreparedData preparedData,
                                                            List<PhyDdlTableOperation> phyDdlTableOperations,
                                                            Map<String, List<List<String>>> tableTopology,
                                                            Map<String, Set<String>> targetTableTopology,
                                                            Map<String, Set<String>> sourceTableTopology,
                                                            List<Pair<String, String>> orderedTargetTableLocations,
                                                            String targetPartition,
                                                            boolean skipBackfill,
                                                            ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForExtractType(curPartitionInfo, parentPrepareData,
                orderedTargetTableLocations, executionContext);
        //checkPartitionCount(newPartInfo);
        return newPartInfo;
    }

}