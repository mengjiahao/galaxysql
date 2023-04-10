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

package com.alibaba.polardbx.gms.metadb.delegate;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 包装连接open&close，子类实现 invoke() 进行sql调用；
 *
 * @param <T>
 */
public abstract class MetaDbAccessorWrapper<T> {

    /**
     * The method that will be really implemented.
     *
     * @return A generic result
     */
    protected abstract T invoke();

    protected abstract void open(Connection metaDbConn);

    protected abstract void close();

    /**
     * Execute a simple operation with a generic result returned.
     *
     * @return A generic result
     */
    public T execute() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            open(metaDbConn);
            T r = invoke();
            return r;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            close();
        }
    }

}
