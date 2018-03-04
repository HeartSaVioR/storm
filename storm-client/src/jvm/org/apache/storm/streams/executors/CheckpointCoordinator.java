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

package org.apache.storm.streams.executors;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointCoordinator implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

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
    public static final int TICK_FREQ_MS = 100;
    public static final String ROLLBACK_REQUEST_DUMMY_TXID = "dummy";
    public static final Long ROLLBACK_REQUEST_DUMMY_CHECKPOINT_ID = -1L;

    public static final long INITIAL_CHECKPOINT_ID = 0L;
    public static final long INITIAL_LAST_SUCCESS_CHECKPOINT_TIMESTAMP = 0L;

    private int checkpointIntervalMs;
    private int operationTimeoutMs;

    private String currentTransactionUuid = "";
    private long currentTransactionStartTimestamp = 0L;

    private long lastSuccessCheckpointId = INITIAL_CHECKPOINT_ID;
    private long lastSuccessCheckpointTimestamp = 0L;

    private boolean checkpointInProgress = false;
    private boolean rollbackInProgress = false;
    private boolean rollbackRequested = false;

    private long firstRollbackRequestedTimestamp = 0L;

    // FIXME: store to Zookeeper for coordinator?
    private KeyValueState<String, String> state;

    private Set<Integer> waitingTasks;
    private Set<Integer> upstreamTasks;
    private OutputCollector collector;

    enum Action {
        CHECKPOINT, ROLLBACK, ROLLBACK_REQUEST
    }

    // FIXME: dummy class... should be stabilized and extracted
    interface KeyValueState<K, V> {
        void put(K key, V value);
        V get(K key);
        V get(K key, V defaultValue);
        void delete(K key);
        void snapshot(long txId);
        void restore(long txId);
    }

    static class DummyKeyValueState<K, V> implements KeyValueState<K, V> {
        private final StateStorage stateStorage;
        private final byte[] namespace;
        private final DefaultStateEncoder<K, V> encoder;

        public DummyKeyValueState(StateStorage stateStorage, byte[] namespace,
                                  Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.stateStorage = stateStorage;
            this.namespace = namespace;
            this.encoder = new DefaultStateEncoder<K, V>(keySerializer, valueSerializer);
        }

        @Override
        public void put(K key, V value) {
            stateStorage.put(namespace, encoder.encodeKey(key), encoder.encodeValue(value));
        }

        @Override
        public V get(K key) {
            byte[] value = stateStorage.get(namespace, encoder.encodeKey(key));
            if (value == null) {
                return null;
            }

            return encoder.decodeValue(value);
        }

        @Override
        public V get(K key, V defaultValue) {
            byte[] value = stateStorage.get(namespace, encoder.encodeKey(key));
            if (value == null) {
                return defaultValue;
            }

            return encoder.decodeValue(value);
        }

        @Override
        public void delete(K key) {
            stateStorage.delete(namespace, encoder.encodeKey(key));
        }

        @Override
        public void snapshot(long txId) {
            stateStorage.snapshot(txId);
        }

        @Override
        public void restore(long txId) {
            stateStorage.restore(txId);
        }
    }

    static interface StateStorage {
        // FIXME: initialize

        void put(byte[] namespace, byte[] key, byte[] value);
        byte[] get(byte[] namespace, byte[] key);
        void delete(byte[] namespace, byte[] key);
        void snapshot(long txId);
        void restore(long txId);

        // FIXME: iterate
        // FIXME: cleanup
    }

    static class DummyStateStorage implements StateStorage {
        private final Map<Long, ImmutableSortedMap<byte[], ImmutableSortedMap<byte[], byte[]>>> snapshots;
        private final NavigableMap<byte[], NavigableMap<byte[], byte[]>> storage;

        public DummyStateStorage() {
            snapshots = new HashMap<>();
            storage = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
        }

        public void clear() {
            storage.clear();
        }

        @Override
        public void put(byte[] namespace, byte[] key, byte[] value) {
            NavigableMap<byte[], byte[]> table = storage.get(namespace);
            if (table == null) {
                table = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
                storage.put(namespace, table);
            }

            table.put(key, value);
        }

        @Override
        public byte[] get(byte[] namespace, byte[] key) {
            NavigableMap<byte[], byte[]> table = storage.get(namespace);
            if (table == null) {
                return null;
            }

            return table.get(key);
        }

        @Override
        public void delete(byte[] namespace, byte[] key) {
            NavigableMap<byte[], byte[]> table = storage.get(namespace);
            if (table != null) {
                table.remove(key);
            }
        }

        @Override
        public void snapshot(long txId) {
            LOG.info("Storing snapshot... tx {}", txId);

            NavigableMap<byte[], ImmutableSortedMap<byte[], byte[]>> newStorage = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> tableToKeyValueMap : storage.entrySet()) {
                byte[] table = tableToKeyValueMap.getKey();
                NavigableMap<byte[], byte[]> keyToValueMap = tableToKeyValueMap.getValue();

                NavigableMap<byte[], byte[]> newKeyToValueMap = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
                for (Map.Entry<byte[], byte[]> keyToValueEntry : keyToValueMap.entrySet()) {
                    byte[] key = keyToValueEntry.getKey();
                    byte[] value = keyToValueEntry.getValue();

                    newKeyToValueMap.put(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
                }

                newStorage.put(Arrays.copyOf(table, table.length), ImmutableSortedMap.copyOfSorted(newKeyToValueMap));
            }

            snapshots.put(txId, ImmutableSortedMap.copyOfSorted(newStorage));

            LOG.info("Snapshot stored... tx {}", txId);
        }

        @Override
        public void restore(long txId) {
            LOG.info("Restoring snapshot... tx {}", txId);

            storage.clear();

            ImmutableSortedMap<byte[], ImmutableSortedMap<byte[], byte[]>> snapshot = snapshots.get(txId);
            if (snapshot != null && !snapshot.isEmpty()) {
                for (Map.Entry<byte[], ImmutableSortedMap<byte[], byte[]>> tableToKeyValueMap : snapshot.entrySet()) {
                    byte[] table = tableToKeyValueMap.getKey();
                    ImmutableSortedMap<byte[], byte[]> keyToValueMap = tableToKeyValueMap.getValue();

                    NavigableMap<byte[], byte[]> newKeyToValueMap = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
                    for (Map.Entry<byte[], byte[]> keyToValueEntry : keyToValueMap.entrySet()) {
                        byte[] key = keyToValueEntry.getKey();
                        byte[] value = keyToValueEntry.getValue();

                        newKeyToValueMap.put(Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
                    }

                    storage.put(Arrays.copyOf(table, table.length), newKeyToValueMap);
                }
            }

            LOG.info("Snapshot restored... tx {}", txId);
        }

        public Map<Long, ImmutableSortedMap<byte[], ImmutableSortedMap<byte[], byte[]>>> getSnapshots() {
            return snapshots;
        }

        public ImmutableSortedMap<byte[], ImmutableSortedMap<byte[], byte[]>> getSnapshot(long txId) {
            return snapshots.get(txId);
        }

        public NavigableMap<byte[], NavigableMap<byte[], byte[]>> getStorage() {
            return storage;
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.checkpointIntervalMs = loadCheckpointInterval(topoConf);
        this.operationTimeoutMs = loadCheckpointOperationTimeout(topoConf);

        this.waitingTasks = new HashSet<>();
        this.upstreamTasks = getUpstreamTasks(context);
        this.collector = collector;

        // FIXME: how to put state in executor?

        // initialize state
        restoreStatesFromLastSuccessfulCheckpoint();

        // initiate rollback to last success checkpoint when coordinator is starting / restarting
        collector.emit(CHECKPOINT_STREAM_ID, new Values(-1, Action.ROLLBACK_REQUEST));
    }

    // FIXME: how to inject state?
    @VisibleForTesting
    public void injectState(KeyValueState<String, String> coordinatorState) {
        this.state = coordinatorState;
    }

    private Set<Integer> getUpstreamTasks(TopologyContext context) {
        Set<Integer> ret = new HashSet<>();
        for (GlobalStreamId inputStream : context.getThisSources().keySet()) {
            if (CHECKPOINT_STREAM_ID.equals(inputStream.get_streamId())) {
                ret.addAll(context.getComponentTasks(inputStream.get_componentId()));
            }
        }
        return Collections.unmodifiableSet(ret);
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

    private int loadCheckpointInterval(Map<String, Object> topoConf) {
        int interval = 0;
        if (topoConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
            interval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
        }

        // minimum 1 second
        interval = interval > MINIMUM_CHECKPOINT_INTERVAL_MS ? interval : MINIMUM_CHECKPOINT_INTERVAL_MS;
        LOG.info("Checkpoint interval is {} millis", interval);
        return interval;
    }

    private int loadCheckpointOperationTimeout(Map<String, Object> topoConf) {
        int interval = 0;
        if (topoConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_OPERATION_TIMEOUT)) {
            interval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_OPERATION_TIMEOUT)).intValue();
        }

        // minimum 10 seconds
        interval = interval > MINIMUM_CHECKPOINT_OPERATION_TIMEOUT_MS ? interval : MINIMUM_CHECKPOINT_OPERATION_TIMEOUT_MS;
        LOG.info("Checkpoint operation timeout is {} millis", interval);
        return interval;
    }

    private void handleCheckpointAction(Tuple input) {
        if (isRollbackRequest(input)) {
            handleRollbackRequest();
            return;
        }

        String txId = input.getStringByField(CHECKPOINT_FIELD_TXID);
        if (StringUtils.isBlank(currentTransactionUuid) || !currentTransactionUuid.equals(txId)) {
            LOG.warn("Checkpoint message for obsoleted tx received: tx for ongoing action {} / received tx {}. Ignoring...",
                    currentTransactionUuid, txId);
            return;
        }

        if (isCheckpoint(input)) {
            handleCheckpoint(input);
        } else if (isRollback(input)) {
            handleRollback(input);
        } else {
            throw new IllegalStateException("Unknown checkpoint action.");
        }
    }

    private boolean isCheckpointAction(Tuple input) {
        return input.getSourceStreamId().equals(CHECKPOINT_STREAM_ID);
    }

    private void handleTickTuple() {
        if (isRollbackRequested()) {
            if (readyToHandleRollbackRequest()) {
                LOG.debug("Ready to handle rollback request.");
                initiateRollback();
            }

            // wait... also don't checkpoint
        } else if (isCheckpointInProgress() || isRollbackInProgress()) {
            if (isLastOperationTimeout()) {
                // rollback whatever the operation was: checkpoint / rollback
                LOG.info("Last operation is timed out. Rolling back.");
                initiateRollback();
            } else {
                LOG.debug("Last operation is still in progress. Skipping...");
            }
        } else if (readyToCheckpoint()) {
            LOG.info("Ready to checkpoint.");
            initiateCheckpoint();
        }
    }

    private boolean readyToHandleRollbackRequest() {
        return (Time.currentTimeMillis() - firstRollbackRequestedTimestamp) >= checkpointIntervalMs;
    }

    private boolean isLastOperationTimeout() {
        return (Time.currentTimeMillis() - currentTransactionStartTimestamp) >= operationTimeoutMs;
    }

    private boolean readyToCheckpoint() {
        return (Time.currentTimeMillis() - lastSuccessCheckpointTimestamp) >= checkpointIntervalMs;
    }

    private void initiateRollback() {
        // FIXME: log/action if checkpoint in progress
        // FIXME: log/action if rollback in progress

        rollbackRequested = false;
        firstRollbackRequestedTimestamp = 0L;

        String newTxId = UUID.randomUUID().toString();
        LOG.info("Rolling back to checkpoint {} with tx {}...", lastSuccessCheckpointId, newTxId);

        initializeAction(Action.ROLLBACK, newTxId);
        emit(newTxId, Action.ROLLBACK, lastSuccessCheckpointId);
    }

    private void storeCheckpointState(long txId) {
        if (state == null) {
            throw new IllegalStateException("State is not yet initialized.");
        }

        state.put(STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID, String.valueOf(txId));
        state.put(STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP, String.valueOf(Time.currentTimeMillis()));

        state.snapshot(COORDINATOR_TRANSACTION_ID);
    }

    private void restoreStatesFromLastSuccessfulCheckpoint() {
        if (state == null) {
            throw new IllegalStateException("State is not yet initialized.");
        }

        state.restore(COORDINATOR_TRANSACTION_ID);

        lastSuccessCheckpointId = Long.valueOf(state.get(STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID, String.valueOf(INITIAL_CHECKPOINT_ID)));
        lastSuccessCheckpointTimestamp = Long.valueOf(state.get(STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP, String.valueOf(INITIAL_LAST_SUCCESS_CHECKPOINT_TIMESTAMP)));
    }

    private void initiateCheckpoint() {
        // FIXME: log/action if checkpoint in progress
        // FIXME: log/action if rollback in progress

        String newTxId = UUID.randomUUID().toString();
        long newCheckpointId = lastSuccessCheckpointId + 1;
        LOG.info("Initiating checkpoint for txid {}, checkpoint id {}", newTxId, newCheckpointId);
        initializeAction(Action.CHECKPOINT, newTxId);
        emit(newTxId, Action.CHECKPOINT, newCheckpointId);
    }

    private void emit(String txId, Action action, long checkpointId) {
        LOG.debug("Emitting txid {}, action {}, checkpointid {}", txId, action, checkpointId);
        collector.emit(CHECKPOINT_STREAM_ID, new Values(txId, action, checkpointId));
    }

    private void initializeAction(Action action, String txId) {
        switch (action) {
            case CHECKPOINT:
                checkpointInProgress = true;
                rollbackInProgress = false;
                break;

            case ROLLBACK:
                rollbackInProgress = true;
                checkpointInProgress = false;
                break;

            default:
                throw new IllegalArgumentException("Invalid Action to initialize: " + action);
        }

        waitingTasks.clear();
        currentTransactionStartTimestamp = Time.currentTimeMillis();
        currentTransactionUuid = txId;
    }

    private void handleCheckpoint(Tuple input) {
        int taskId = input.getSourceTask();
        String txId = input.getStringByField(CHECKPOINT_FIELD_TXID);
        long checkpointId = input.getLongByField(CHECKPOINT_FIELD_CHECKPOINT_ID);

        if (rollbackRequested) {
            LOG.warn("Received checkpoint for tx {} while another rollback request is queued... Skipping.", txId);
            return;
        }

        if (rollbackInProgress) {
            LOG.debug("Received checkpoint for tx {} but rollback action for tx {} is in progress. Skipping.", txId, currentTransactionUuid);
            return;
        }

        if (!checkpointInProgress) {
            // This represents that coordinator fails while checkpoint is in progress.
            LOG.warn("Received checkpoint for tx {} but no action is in progress. Coordinator may be crashed and restarted. Queueing rollback...", txId);
            handleRollbackRequest();
            return;
        }

        waitingTasks.add(taskId);

        LOG.debug("Add task {} to waiting tasks... upstream tasks are {}", waitingTasks, upstreamTasks);

        if (waitingTasks.equals(upstreamTasks)) {
            storeCheckpointState(checkpointId);
            finalizeAction(Action.CHECKPOINT, checkpointId);
            LOG.info("Checkpoint to tx {} / checkpoint id {} Finished.", txId, checkpointId);
        }
    }

    private void handleRollback(Tuple input) {
        if (rollbackRequested) {
            LOG.warn("Received rollback for tx {} while another rollback request is queued...");
        }

        int taskId = input.getSourceTask();
        String txId = input.getStringByField(CHECKPOINT_FIELD_TXID);
        long checkpointId = input.getLongByField(CHECKPOINT_FIELD_CHECKPOINT_ID);

        if (checkpointInProgress) {
            String msg = String.format("Received rollback for tx %s but checkpoint action for tx %s is in progress, which should not happen!",
                    txId, currentTransactionUuid);
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        if (!rollbackInProgress) {
            LOG.warn("Received rollback for tx {} but no action is in progress. Coordinator may be crashed and restarted. Queueing rollback...", txId);
            handleRollbackRequest();
            return;
        }

        waitingTasks.add(taskId);

        LOG.debug("Add task {} to waiting tasks... upstream tasks are {}", waitingTasks, upstreamTasks);

        if (waitingTasks.equals(upstreamTasks)) {
            finalizeAction(Action.ROLLBACK, checkpointId);
            LOG.info("Rollback to tx {} Finished.", txId);
        }
    }

    private void finalizeAction(Action action, long checkpointId) {
        switch (action) {
            case CHECKPOINT:
                checkpointInProgress = false;
                break;

            case ROLLBACK:
                rollbackInProgress = false;
                break;

            default:
                throw new IllegalArgumentException("Invalid Action to finalize: " + action);
        }

        waitingTasks.clear();
        currentTransactionStartTimestamp = 0L;
        lastSuccessCheckpointId = checkpointId;
        if (action == Action.CHECKPOINT) {
            lastSuccessCheckpointTimestamp = Time.currentTimeMillis();
        }
    }

    private void handleRollbackRequest() {
        if (checkpointInProgress) {
            LOG.warn("Received rollback request while checkpointing tx {} is in progress...", currentTransactionUuid);
            checkpointInProgress = false;
            waitingTasks.clear();
        }

        if (rollbackInProgress) {
            LOG.warn("Received rollback request while rolling back to tx {} is in progress...", currentTransactionUuid);
            rollbackInProgress = false;
            waitingTasks.clear();
        }

        if (!rollbackRequested) {
            LOG.debug("Rollback request queued.");
            rollbackRequested = true;
            firstRollbackRequestedTimestamp = Time.currentTimeMillis();
            currentTransactionUuid = "";
            currentTransactionStartTimestamp = 0;
        } else {
            LOG.debug("Rollback request received while rollback request is in queue. Skipping...");
        }
    }

    private boolean isCheckpoint(Tuple input) {
        Action action = (Action) input.getValueByField(CHECKPOINT_FIELD_ACTION);
        return action == Action.CHECKPOINT;
    }

    private boolean isRollback(Tuple input) {
        Action action = (Action) input.getValueByField(CHECKPOINT_FIELD_ACTION);
        return action == Action.ROLLBACK;
    }

    private boolean isRollbackRequest(Tuple input) {
        Action action = (Action) input.getValueByField(CHECKPOINT_FIELD_ACTION);
        return action == Action.ROLLBACK_REQUEST;
    }

    @Override
    public void cleanup() {
        // FIXME: clean up state
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CHECKPOINT_STREAM_ID, new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(new HashMap<>(), TICK_FREQ_MS);
    }

    @VisibleForTesting
    int getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    @VisibleForTesting
    int getOperationTimeoutMs() {
        return operationTimeoutMs;
    }

    @VisibleForTesting
    long getLastSuccessCheckpointId() {
        return lastSuccessCheckpointId;
    }

    @VisibleForTesting
    long getLastSuccessCheckpointTimestamp() {
        return lastSuccessCheckpointTimestamp;
    }

    @VisibleForTesting
    String getCurrentTransactionUuid() {
        return currentTransactionUuid;
    }

    @VisibleForTesting
    long getCurrentTransactionStartTimestamp() {
        return currentTransactionStartTimestamp;
    }

    @VisibleForTesting
    boolean isCheckpointInProgress() {
        return checkpointInProgress;
    }

    @VisibleForTesting
    boolean isRollbackInProgress() {
        return rollbackInProgress;
    }

    @VisibleForTesting
    boolean isRollbackRequested() {
        return rollbackRequested;
    }

    @VisibleForTesting
    long getFirstRollbackRequestedTimestamp() {
        return firstRollbackRequestedTimestamp;
    }

    @VisibleForTesting
    KeyValueState<String, String> getState() {
        return state;
    }

    @VisibleForTesting
    Set<Integer> getWaitingTasks() {
        return waitingTasks;
    }

    @VisibleForTesting
    Set<Integer> getUpstreamTasks() {
        return upstreamTasks;
    }
}
