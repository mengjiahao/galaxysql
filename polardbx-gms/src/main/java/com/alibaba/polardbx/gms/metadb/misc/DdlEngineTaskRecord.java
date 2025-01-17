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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 单库单表：
 * {"description":"","logicalTableName":"tb1","name":"CreateTablePhyDdlTask","physicalPlanData":{"createTablePhysicalSql":"CREATE TABLE tb1 (\n\tid INTEGER NOT NULL,\n\tname VARCHAR(120),\n\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n\tPRIMARY KEY (_drds_implicit_id_)\n)","defaultDbIndex":"TEST_SINGLE_GROUP","defaultPhyTableName":"tb1_Nhdu","explain":false,"flashbackRename":false,"ifExists":false,"ifNotExists":false,"kind":"CREATE_TABLE","localityDesc":{"balanceSingleTable":false,"dnList":[],"dnString":""},"logicalTableName":"tb1","paramsList":[{1:{"args":[1,"`tb1_Nhdu`"],"parameterMethod":"setTableName","value":"`tb1_Nhdu`"}}],"partitioned":false,"schemaName":"test","sequence":{"innerStep":100000,"new":true,"type":"GROUP","unitCount":1,"unitIndex":0},"sqlTemplate":"CREATE TABLE ? (\n\tid INTEGER NOT NULL,\n\tname VARCHAR(120),\n\t_drds_implicit_id_ bigint AUTO_INCREMENT,\n\tPRIMARY KEY (_drds_implicit_id_)\n)","tableTopology":{"TEST_SINGLE_GROUP":[["tb1_Nhdu"]]},"tablesExtRecord":{"autoPartition":false,"broadcast":0,"dbMetaMap":"","dbNamePattern":"TEST_SINGLE_GROUP","dbPartitionCount":1,"dbPartitionKey":"","dbPartitionPolicy":"","dbRule":"","extPartitions":"","flag":0,"fullTableScan":0,"locked":false,"newTableName":"","status":0,"tableName":"tb1","tableSchema":"test","tableType":0,"tbMetaMap":"","tbNamePattern":"tb1_Nhdu","tbPartitionCount":1,"tbPartitionKey":"","tbPartitionPolicy":"","tbRule":"","version":1},"temporary":false,"truncatePartition":false,"withHint":false},"schemaName":"test"}；
 *
 */
public class DdlEngineTaskRecord implements SystemTableRecord {

    public long jobId;
    public long taskId;
    public String schemaName;
    public String name;
    public String state;
    public String exceptionAction;
    public String value;
    public String extra;
    public String cost;
    public long rootJobId;

    @Override
    public DdlEngineTaskRecord fill(ResultSet rs) throws SQLException {
        this.jobId = rs.getLong("job_id");
        this.taskId = rs.getLong("task_id");
        this.schemaName = rs.getString("schema_name");
        this.name = rs.getString("name");
        this.state = rs.getString("state");
        this.exceptionAction = rs.getString("exception_action");
        this.value = rs.getString("value");
        this.extra = rs.getString("extra");
        this.cost = rs.getString("cost");
        this.rootJobId = rs.getLong("root_job_id");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(16);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.jobId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.taskId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schemaName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.state);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.exceptionAction);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.value);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.extra);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.cost);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, this.rootJobId);
        return params;
    }

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(final long jobId) {
        this.jobId = jobId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public void setTaskId(final long taskId) {
        this.taskId = taskId;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getState() {
        return this.state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public String getExceptionAction() {
        return this.exceptionAction;
    }

    public void setExceptionAction(final String exceptionAction) {
        this.exceptionAction = exceptionAction;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getCost() {
        return cost;
    }

    public void setCost(String cost) {
        this.cost = cost;
    }

    public long getRootJobId() {
        return rootJobId;
    }

    public void setRootJobId(long rootJobId) {
        this.rootJobId = rootJobId;
    }
}
