/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.storm.streams.checkpoint;

public final class CheckpointConstants {
    private CheckpointConstants() {

    }

    public static final long COORDINATOR_TRANSACTION_ID = 0L;
    public static final String STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID = "lastSuccessCheckpointId";
    public static final String STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP = "lastSuccessCheckpointTimestamp";

    public static final String COORDINATOR_COMPONENT_ID = "$checkpointcoordinator";
    public static final String CHECKPOINT_STREAM_ID = "$checkpoint";
    public static final String CHECKPOINT_FIELD_TXID = "txid";
    public static final String CHECKPOINT_FIELD_ACTION = "action";
    public static final String CHECKPOINT_FIELD_CHECKPOINT_ID = "checkpointid";

    public static final int MINIMUM_CHECKPOINT_INTERVAL_MS = 1000;
    public static final int MINIMUM_CHECKPOINT_OPERATION_TIMEOUT_MS = 10000;

    public static final String ROLLBACK_REQUEST_DUMMY_TXID = "dummy";
    public static final Long ROLLBACK_REQUEST_DUMMY_CHECKPOINT_ID = -1L;

    public static final long INITIAL_CHECKPOINT_ID = 0L;
    public static final long INITIAL_LAST_SUCCESS_CHECKPOINT_TIMESTAMP = 0L;
}
