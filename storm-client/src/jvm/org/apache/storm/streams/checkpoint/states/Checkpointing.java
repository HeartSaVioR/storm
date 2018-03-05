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

import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.storm.streams.checkpoint.CheckpointAction;
import org.apache.storm.streams.checkpoint.CheckpointStateFactory;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Checkpointing extends BaseCheckpointState {
    private static final Logger LOG = LoggerFactory.getLogger(Checkpointing.class);

    private final Set<Integer> waitingTasks;

    public Checkpointing(CheckpointStateFactory stateFactory, long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        super(stateFactory, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        this.waitingTasks = new HashSet<>(getUpstreamTasks().size());
    }

    @Override
    public void initialize() {
        initiateNewTransaction();

        long newCheckpointId = lastSuccessCheckpointId + 1;
        LOG.info("Initiating checkpoint for txid {}, checkpoint id {}", currentTxId, newCheckpointId);

        emit(CheckpointAction.CHECKPOINT, newCheckpointId);
    }

    @Override
    public CheckpointState requestRollback(int taskId) {
        LOG.warn("Received rollback request while checkpointing tx {} is in progress...", currentTxId);
        CheckpointState newState = stateFactory.requestRollback(lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }

    @Override
    public CheckpointState tick() {
        if (isLastOperationTimeout()) {
            LOG.info("Last checkpoint tx {} is timed out. Rolling back.", currentTxId);
            return initiateRollback();
        }

        return this;
    }

    @Override
    protected CheckpointState doCheckpoint(int taskId, String txId, long checkpointId) {
        waitingTasks.add(taskId);

        Set<Integer> upstreamTasks = stateFactory.getUpstreamTasks();
        LOG.debug("Add task {} to waiting tasks... upstream tasks are {}", waitingTasks, upstreamTasks);

        if (waitingTasks.equals(upstreamTasks)) {
            long lastSuccessCheckpointTimestamp = Time.currentTimeMillis();
            stateFactory.storeNewCheckpointState(checkpointId, lastSuccessCheckpointTimestamp);

            LOG.info("Checkpoint to tx {} / checkpoint id {} Finished.", txId, checkpointId);

            CheckpointState newState = stateFactory.ready(checkpointId, lastSuccessCheckpointTimestamp);
            newState.initialize();
            return newState;
        }

        return this;
    }

    @Override
    protected CheckpointState doRollback(int taskId, String txId, long checkpointId) {
        String msg = String.format("Received rollback for tx %s but checkpoint action for tx %s is in progress, which should not happen!",
                txId, currentTxId);
        LOG.error(msg);
        throw new IllegalStateException(msg);
    }

    @VisibleForTesting
    public Set<Integer> getWaitingTasks() {
        return waitingTasks;
    }

    private boolean isLastOperationTimeout() {
        return (Time.currentTimeMillis() - currentTxStartTimestamp) >= stateFactory.getOperationTimeoutMs();
    }

    private CheckpointState initiateRollback() {
        CheckpointState newState = stateFactory.rollback(lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }

}
