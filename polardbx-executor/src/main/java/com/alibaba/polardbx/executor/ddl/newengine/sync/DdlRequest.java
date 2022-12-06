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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import java.util.List;

/**
 * 分发给 Leader CN 的 DdlRequest 消息；
 */
public class DdlRequest {

    private String schemaName;
    /** 这里存储 jobId 即可，因为实际内容已经存储在 metadb中；*/
    private List<Long> jobIds;

    public DdlRequest() {
    }

    public DdlRequest(final String schemaName, final List<Long> jobIds) {
        this.schemaName = schemaName;
        this.jobIds = jobIds;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<Long> getJobIds() {
        return jobIds;
    }

    public void setJobIds(List<Long> jobIds) {
        this.jobIds = jobIds;
    }

}
