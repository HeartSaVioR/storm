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

package org.apache.storm.streams.checkpoint.states;

import org.apache.storm.streams.checkpoint.CheckpointStateFactory;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadyCheckpointState extends BaseCheckpointState {
    private static final Logger LOG = LoggerFactory.getLogger(ReadyCheckpointState.class);

    public ReadyCheckpointState(CheckpointStateFactory stateFactory, long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        super(stateFactory, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    @Override
    public CheckpointState requestRollback(int taskId) {
        LOG.debug("Rollback request queued.");

        return initiateRollbackRequest();
    }

    @Override
    public CheckpointState tick() {
        if (readyToCheckpoint()) {
            LOG.info("Ready to checkpoint.");
            return initiateCheckpoint();
        }

        return this;
    }

    @Override
    public CheckpointState checkpoint(int taskId, String txId, long checkpointId) {
        // This represents that coordinator fails while checkpoint is in progress.
        LOG.warn("Received checkpoint for tx {} but no action is in progress. Coordinator may be crashed and restarted. Queueing rollback...", txId);
        return initiateRollbackRequest();
    }

    @Override
    protected CheckpointState doCheckpoint(int taskId, String txId, long checkpointId) {
        throw new IllegalStateException("Should not be called here.");
    }

    @Override
    public CheckpointState rollback(int taskId, String txId, long checkpointId) {
        // This represents that coordinator fails while rollback is in progress.
        LOG.warn("Received rollback for tx {} but no action is in progress. Coordinator may be crashed and restarted. Queueing rollback...", txId);
        return initiateRollbackRequest();
    }

    @Override
    protected CheckpointState doRollback(int taskId, String txId, long checkpointId) {
        throw new IllegalStateException("Should not be called here.");
    }

    private boolean readyToCheckpoint() {
        return (Time.currentTimeMillis() - getCurrentTxStartTimestamp()) >= getCheckpointIntervalMs();
    }

    private CheckpointState initiateCheckpoint() {
        CheckpointState newState = stateFactory.checkpoint(lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }

    private CheckpointState initiateRollbackRequest() {
        CheckpointState newState = stateFactory.requestRollback(lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }
}
