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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.ICommandHandlerFactory;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.PlanHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

/**
 * 调用 CN->DN 执行操作;
 */
public abstract class AbstractGroupExecutor extends AbstractLifecycle implements IGroupExecutor {

    private final IRepository repo;
    private static final Logger logger = LoggerFactory.getLogger(AbstractGroupExecutor.class);
    /** Group[name=TEST_SINGLE_GROUP,appName=test@polardbx-polardbx,schemaName=test,type=MYSQL_JDBC,atoms=[],properties={},unitName=<null>,enforceMaster=false] */
    private Group group;

    public AbstractGroupExecutor(IRepository repo) {
        this.repo = repo;
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    @Override
    public Cursor execByExecPlanNode(RelNode relNode, ExecutionContext executionContext) {
        return executeInner(relNode, executionContext);
    }

    @Override
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext) {
        return ExecutorContext.getContext(
            executionContext.getSchemaName()).getTopologyExecutor().execByExecPlanNodeFuture(relNode, executionContext);
    }

    @Override
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext,
                                                   BlockingQueue completionQueue) {
        return ExecutorContext.getContext(
            executionContext.getSchemaName()).getTopologyExecutor().execByExecPlanNodeFuture(relNode,
            executionContext,
            completionQueue);
    }

    /**
     * 不同阶段 planHandler 不一样, 根据 relNode 是逻辑执行计划 还是 物理执行计划 调用不同 handler，看是操作 CN 还是 DN：
     * DDL: CN 定义 DdlJob，或者 执行DdlJob时 CN 调用 DN 执行物理执行计划;
     *
     * create table: LogicalCreateTable -> MySingleTableScanHandler;
     * drop table: LogicalDropTable -> MySingleTableScanHandler;
     *
     * @param relNode LogicalCreateTable/PhyDdlTableOperation
     * @param executionContext
     * @return
     */
    private Cursor executeInner(RelNode relNode, ExecutionContext executionContext) {
        // CommandHandlerFactoryMyImp
        ICommandHandlerFactory commandExecutorFactory = this.repo.getCommandExecutorFactory();
        // 根据当前executor,拿到对应的处理Handler
        PlanHandler planHandler = commandExecutorFactory.getCommandHandler(relNode, executionContext);
        return planHandler.handlePlan(relNode, executionContext);
    }

    @Override
    public Group getGroupInfo() {
        return this.group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    @Override
    public String toString() {
        return "GroupExecutor [groupName=" + group.getName() + ", type=" + group.getType()
            + ", dataSource=" + getDataSource() + "]";
    }

}
