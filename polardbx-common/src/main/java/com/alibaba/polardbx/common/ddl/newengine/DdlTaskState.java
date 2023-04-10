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

package com.alibaba.polardbx.common.ddl.newengine;

public enum DdlTaskState {

    READY,

    DIRTY, // 准备执行

    SUCCESS,

    ROLLBACK_SUCCESS;

    public static boolean needToCallExecute(DdlTaskState state) {
        return state == READY || state == DIRTY;
    }

    public static boolean needToCallRollback(DdlTaskState state) {
        return state == DIRTY || state == SUCCESS;
    }

    public static boolean success(DdlTaskState state) {
        return state == SUCCESS;
    }

}