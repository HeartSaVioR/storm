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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.streams.checkpoint.states.CheckpointState;
import org.apache.storm.streams.checkpoint.states.Checkpointing;
import org.apache.storm.streams.checkpoint.states.ReadyCheckpointState;
import org.apache.storm.streams.checkpoint.states.RollbackRequestQueued;
import org.apache.storm.streams.checkpoint.states.RollingBack;
import org.apache.storm.streams.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointStateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointStateFactory.class);

    private final int checkpointIntervalMs;
    private final int operationTimeoutMs;
    private final OutputCollector collector;
    private final KeyValueState<String, String> state;
    private final Set<Integer> upstreamTasks;

    public CheckpointStateFactory(Map<String, Object> topoConf, TopologyContext topologyContext, OutputCollector collector,
                                  KeyValueState<String, String> state) {
        this.checkpointIntervalMs = loadCheckpointInterval(topoConf);
        this.operationTimeoutMs = loadCheckpointOperationTimeout(topoConf);
        this.collector = collector;
        this.state = state;
        this.upstreamTasks = calculateUpstreamTasks(topologyContext);
    }

    public int getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public int getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    public OutputCollector getCollector() {
        return collector;
    }

    public KeyValueState<String, String> getState() {
        return state;
    }

    public Set<Integer> getUpstreamTasks() {
        return upstreamTasks;
    }

    public void storeNewCheckpointState(long newCheckpointId, long newSuccessCheckpointTimestamp) {
        if (state == null) {
            throw new IllegalStateException("State is not yet initialized.");
        }

        state.put(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID, String.valueOf(newCheckpointId));
        state.put(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP, String.valueOf(newSuccessCheckpointTimestamp));

        state.snapshot(CheckpointConstants.COORDINATOR_TRANSACTION_ID);
    }

    public CheckpointState initialize() {
        if (state == null) {
            throw new IllegalStateException("State is not yet initialized.");
        }

        state.restore(CheckpointConstants.COORDINATOR_TRANSACTION_ID);

        long lastSuccessCheckpointId = readLastSuccessCheckpointId(state);
        long lastSuccessCheckpointTimestamp = readLastSuccessCheckpointTimestamp(state);

        // initiate rollback to last success checkpoint when coordinator is starting / restarting
        collector.emit(CheckpointConstants.CHECKPOINT_STREAM_ID, new Values(CheckpointConstants.ROLLBACK_REQUEST_DUMMY_TXID,
                CheckpointAction.ROLLBACK_REQUEST, CheckpointConstants.ROLLBACK_REQUEST_DUMMY_CHECKPOINT_ID));

        CheckpointState newState = new ReadyCheckpointState(this, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
        newState.initialize();
        return newState;
    }

    public CheckpointState ready(long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        return new ReadyCheckpointState(this, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    public CheckpointState checkpoint(long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        return new Checkpointing(this, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    public CheckpointState rollback(long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        return new RollingBack(this, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    public CheckpointState requestRollback(long lastSuccessCheckpointId, long lastSuccessCheckpointTimestamp) {
        return new RollbackRequestQueued(this, lastSuccessCheckpointId, lastSuccessCheckpointTimestamp);
    }

    private int loadCheckpointInterval(Map<String, Object> topoConf) {
        int interval = 0;
        if (topoConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
            interval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        }

        // minimum 1 second
        interval = interval > CheckpointConstants.MINIMUM_CHECKPOINT_INTERVAL_MS ? interval : CheckpointConstants.MINIMUM_CHECKPOINT_INTERVAL_MS;
        LOG.info("Checkpoint interval is {} millis", interval);
        return interval;
    }

    private int loadCheckpointOperationTimeout(Map<String, Object> topoConf) {
        int interval = 0;
        if (topoConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_OPERATION_TIMEOUT)) {
            interval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_OPERATION_TIMEOUT)).intValue();
        }

        // minimum 10 seconds
        interval = interval > CheckpointConstants.MINIMUM_CHECKPOINT_OPERATION_TIMEOUT_MS ? interval :
                CheckpointConstants.MINIMUM_CHECKPOINT_OPERATION_TIMEOUT_MS;
        LOG.info("Checkpoint operation timeout is {} millis", interval);
        return interval;
    }

    private Set<Integer> calculateUpstreamTasks(TopologyContext context) {
        Set<Integer> ret = new HashSet<>();
        for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
            if (CheckpointConstants.CHECKPOINT_STREAM_ID.equals(inputStream.get_streamId())) {
                ret.addAll(context.getComponentTasks(inputStream.get_componentId()));
            }
        }
        return Collections.unmodifiableSet(ret);
    }

    private long readLastSuccessCheckpointId(KeyValueState<String, String> state) {
        return Long.valueOf(state.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID, String.valueOf(CheckpointConstants.INITIAL_CHECKPOINT_ID)));
    }

    private long readLastSuccessCheckpointTimestamp(KeyValueState<String, String> state) {
        return Long.valueOf(state.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP, String.valueOf(CheckpointConstants.INITIAL_LAST_SUCCESS_CHECKPOINT_TIMESTAMP)));
    }
}

