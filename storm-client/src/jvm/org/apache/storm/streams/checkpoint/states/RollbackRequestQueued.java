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

public class RollbackRequestQueued extends BaseCheckpointState {
    private static final Logger LOG = LoggerFactory.getLogger(RollbackRequestQueued.class);

    public RollbackRequestQueued(CheckpointStateFactory stateFactory, long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        super(stateFactory, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    @Override
    public void initialize() {
        // it doesn't actually initialize new transaction, but it is easier to leverage the information from there
        initiateNewTransaction();
    }

    @Override
    public CheckpointState checkpoint(int taskId, String txId, long checkpointId) {
        LOG.warn("Received rollback for tx {} while another rollback request is queued... Skipping.", txId);
        return this;
    }

    @Override
    public CheckpointState rollback(int taskId, String txId, long checkpointId) {
        LOG.warn("Received rollback for tx {} while another rollback request is queued... Skipping.");
        return this;
    }

    @Override
    protected CheckpointState doCheckpoint(int taskId, String txId, long checkpointId) {
        throw new IllegalStateException("Should not be called here.");
    }

    @Override
    protected CheckpointState doRollback(int taskId, String txId, long checkpointId) {
        throw new IllegalStateException("Should not be called here.");
    }

    @Override
    public CheckpointState requestRollback(int taskId) {
        LOG.debug("Rollback request received while rollback request is in queue. Skipping...");
        return this;
    }

    @Override
    public CheckpointState tick() {
        if (readyToHandleRollbackRequest()) {
            LOG.debug("Ready to handle rollback request.");

            return initiateRollback();
        }

        return this;
    }

    private boolean readyToHandleRollbackRequest() {
        return (Time.currentTimeMillis() - currentTxStartTimestamp) >= stateFactory.getCheckpointIntervalMs();
    }

    private CheckpointState initiateRollback() {
        CheckpointState newState = stateFactory.rollback(lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }
}
