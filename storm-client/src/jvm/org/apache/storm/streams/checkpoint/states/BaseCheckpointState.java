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

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.streams.checkpoint.CheckpointConstants;
import org.apache.storm.streams.checkpoint.CheckpointAction;
import org.apache.storm.streams.checkpoint.CheckpointStateFactory;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCheckpointState implements CheckpointState {
    private static final Logger LOG = LoggerFactory.getLogger(BaseCheckpointState.class);

    protected final CheckpointStateFactory stateFactory;

    protected long lastSuccessCheckpointId;
    protected long lastSuccessCheckpointTimestamp;
    protected String currentTxId;
    protected long currentTxStartTimestamp;

    public BaseCheckpointState(CheckpointStateFactory stateFactory, long lastSuccessCheckpointId,
                               long lastSuccessCheckpointTimestamp) {
        this.stateFactory = stateFactory;
        this.currentTxId = UUID.randomUUID().toString();
        this.currentTxStartTimestamp = Time.currentTimeMillis();
        this.lastSuccessCheckpointId = lastSuccessCheckpointId;
        this.lastSuccessCheckpointTimestamp = lastSuccessCheckpointTimestamp;
    }

    @Override
    public String getCurrentTxId() {
        return currentTxId;
    }

    @Override
    public long getCurrentTxStartTimestamp() {
        return currentTxStartTimestamp;
    }

    public long getCheckpointIntervalMs() {
        return stateFactory.getCheckpointIntervalMs();
    }

    public long getOperationTimeoutMs() {
        return stateFactory.getOperationTimeoutMs();
    }

    @Override
    public long getLastSuccessCheckpointId() {
        return lastSuccessCheckpointId;
    }

    @Override
    public long getLastSuccessCheckpointTimestamp() {
        return lastSuccessCheckpointTimestamp;
    }

    protected Set<Integer> getUpstreamTasks() {
        return stateFactory.getUpstreamTasks();
    }

    @Override
    public CheckpointState checkpoint(int taskId, String txId, long checkpointId) {
        if (!checkForSameTransactionId(txId)) {
            return this;
        }

        return doCheckpoint(taskId, txId, checkpointId);
    }

    @Override
    public CheckpointState rollback(int taskId, String txId, long checkpointId) {
        if (!checkForSameTransactionId(txId)) {
            return this;
        }

        return doRollback(taskId, txId, checkpointId);
    }

    @Override
    public void initialize() {
        // no-op
    }

    protected void initiateNewTransaction() {
        this.currentTxId = UUID.randomUUID().toString();
        this.currentTxStartTimestamp = Time.currentTimeMillis();
    }

    protected void emit(CheckpointAction action, long checkpointId) {
        LOG.debug("Emitting txid {}, action {}, checkpointid {}", currentTxId, action, checkpointId);
        stateFactory.getCollector().emit(CheckpointConstants.CHECKPOINT_STREAM_ID, new Values(currentTxId, action, checkpointId));
    }

    protected abstract CheckpointState doCheckpoint(int taskId, String txId, long checkpointId);

    protected abstract CheckpointState doRollback(int taskId, String txId, long checkpointId);

    private boolean checkForSameTransactionId(String txId) {
        if (StringUtils.isBlank(currentTxId) || !currentTxId.equals(txId)) {
            LOG.warn("Checkpoint message for obsoleted tx received: tx for ongoing action {} / received tx {}. Ignoring...",
                    currentTxId, txId);
            return false;
        }

        return true;
    }
}
