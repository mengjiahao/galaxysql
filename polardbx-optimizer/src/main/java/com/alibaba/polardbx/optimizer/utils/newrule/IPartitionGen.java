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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.parse.bean.DBPartitionDefinition;
import com.alibaba.polardbx.rule.TableRule;

import java.util.List;

/**
 * Created by simiao on 14-12-3.
 */
public interface IPartitionGen {

    void fillDbRule(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta,
                    int group_count,
                    int tablesPerGroup, DBPartitionDefinition dBPartitionDefinition);

    void fillDbRuleStandAlone(TableRule tableRule, List<String> partitionParams, TableMeta tableMeta, int group_count,
                              int tablesPerGroup, DBPartitionDefinition dBPartitionDefinition);

}