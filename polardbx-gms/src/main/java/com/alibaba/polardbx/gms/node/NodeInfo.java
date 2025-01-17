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

package com.alibaba.polardbx.gms.node;

import javax.sql.DataSource;

/**
 * 节点信息(host, port, connection, cpu-core)
 */
public interface NodeInfo {

    String getHost();

    String getHostPort();

    /**
     * 相当于 connection
     * @return
     */
    DataSource getManagerDataSource();

    String getServerKey();

    String getManagerKey();

    int getCpuCore();

}
