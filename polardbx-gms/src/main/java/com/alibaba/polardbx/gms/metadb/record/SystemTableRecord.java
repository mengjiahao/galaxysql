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

package com.alibaba.polardbx.gms.metadb.record;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet.row -> SystemTableRecord;
 * 通过sql返回值 ResultSet next() 遍历下一行，getXXX(columnIndex) 获取列值;
 */
public interface SystemTableRecord {

    /**
     * Fill a single record.
     *
     * @param rs JDBC result set
     * @param <T> A class implementation of SystemTableRecord
     * @return A object of the class implementation
     * @throws SQLException Thrown exception
     */
    <T extends SystemTableRecord> T fill(ResultSet rs) throws SQLException;

}
