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

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.storm.streams.checkpoint.states.CheckpointState;
import org.apache.storm.streams.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

public class CheckpointCoordinator implements IRichBolt {
    public static final int TICK_FREQ_MS = 100;

    // FIXME: store to Zookeeper for coordinator?
    private KeyValueState<String, String> state;

    private CheckpointStateFactory stateFactory;
    private CheckpointState currentState;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        // initialize state
        stateFactory = new CheckpointStateFactory(topoConf, context, collector, state);
        currentState = stateFactory.initialize();
    }

    // FIXME: how to inject state?
    @VisibleForTesting
    void injectState(KeyValueState<String, String> coordinatorState) {
        this.state = coordinatorState;
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            handleTickTuple();
            return;
        }

        if (!isCheckpointAction(input)) {
            throw new IllegalStateException("Non-checkpoint related tuples should never be sent to Coordinator.");
        }

        handleCheckpointAction(input);
    }

    private void handleTickTuple() {
        currentState = currentState.tick();
    }

    private void handleCheckpointAction(Tuple input) {
        int taskId = input.getSourceTask();
        CheckpointAction action = (CheckpointAction) input.getValueByField(CheckpointConstants.CHECKPOINT_FIELD_ACTION);
        String txId = input.getStringByField(CheckpointConstants.CHECKPOINT_FIELD_TXID);
        long checkpointId = input.getLongByField(CheckpointConstants.CHECKPOINT_FIELD_CHECKPOINT_ID);

        switch (action) {
            case CHECKPOINT:
                currentState = currentState.checkpoint(taskId, txId, checkpointId);
                break;

            case ROLLBACK:
                currentState = currentState.rollback(taskId, txId, checkpointId);
                break;

            case ROLLBACK_REQUEST:
                currentState = currentState.requestRollback(taskId);
                break;

            default:
                throw new IllegalStateException("Unknown checkpoint action.");
        }
    }

    private boolean isCheckpointAction(Tuple input) {
        return input.getSourceStreamId().equals(CheckpointConstants.CHECKPOINT_STREAM_ID);
    }

    @Override
    public void cleanup() {
        // FIXME: clean up state
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CheckpointConstants.CHECKPOINT_STREAM_ID,
                new Fields(CheckpointConstants.CHECKPOINT_FIELD_TXID, CheckpointConstants.CHECKPOINT_FIELD_ACTION, CheckpointConstants.CHECKPOINT_FIELD_CHECKPOINT_ID));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(new HashMap<>(), TICK_FREQ_MS);
    }

    @VisibleForTesting
    CheckpointStateFactory getStateFactory() {
        return stateFactory;
    }

    @VisibleForTesting
    CheckpointState getCurrentState() {
        return currentState;
    }

    @VisibleForTesting
    KeyValueState<String, String> getState() {
        return state;
    }
}
