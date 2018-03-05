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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.state.DefaultStateSerializer;
import org.apache.storm.streams.checkpoint.states.CheckpointState;
import org.apache.storm.streams.checkpoint.states.Checkpointing;
import org.apache.storm.streams.checkpoint.states.ReadyCheckpointState;
import org.apache.storm.streams.checkpoint.states.RollbackRequestQueued;
import org.apache.storm.streams.checkpoint.states.RollingBack;
import org.apache.storm.streams.state.KeyValueState;
import org.apache.storm.streams.state.StateStorage;
import org.apache.storm.streams.testutil.DummyKeyValueState;
import org.apache.storm.streams.testutil.DummyStateStorage;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

public class CheckpointCoordinatorTest {

    private static final String NAMESPACE = "testNamespace";
    private static final byte[] NAMESPACE_BYTES = NAMESPACE.getBytes(Charset.forName("UTF-8"));

    private static final int TEST_CHECKPOINT_INTERVAL = 10;
    private static final int TEST_CHECKPOINT_OPERATION_TIMEOUT = 100;

    private static final long TEST_LAST_SUCCESS_CHECKPOINT_ID = 11L;

    public static final int START_TERMINAL_TASK_INDEX = 1001;
    public static final int TEST_COORDINATOR_TASK_ID = -2;

    private StateStorage testStateStorage;
    private KeyValueState<String, String> testKeyValueState;
    private DefaultStateSerializer<String> serializer;

    @Before
    public void setUp() {
        testStateStorage = new DummyStateStorage();
        serializer = new DefaultStateSerializer<>();
        testKeyValueState = new DummyKeyValueState<>(testStateStorage, NAMESPACE_BYTES, serializer, serializer);
    }

    @FunctionalInterface
    interface CoordinatorTestFunction {
        void runTest(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                  TopologyContext mockedContext, OutputCollector mockedCollector);
    }

    private void runTestWithTestCoordinatorEnvironment(CoordinatorTestFunction func) {
        try (SimulatedTime time = new SimulatedTime()) {
            initializeStateForCoordinator();

            Map<String, Object> topoConf = getTestTopologyConfiguration();
            Map<String, List<Integer>> terminalTasksMap = ComponentToTasksMapGenerator.createTerminalTasks("sink", 2, 3);
            List<Integer> terminalTasks = ComponentToTasksMapGenerator.flattenTasks(terminalTasksMap);

            TopologyContext mockedContext = mockContext(topoConf, terminalTasksMap);
            OutputCollector mockedCollector = mock(OutputCollector.class);

            CheckpointCoordinator coordinator = initializeCheckpointCoordinator(topoConf, mockedContext, mockedCollector);

            func.runTest(coordinator, terminalTasks, mockedContext, mockedCollector);
        }
    }

    @Test
    public void testCheckpointBasicCase() {
        // checkpoint is being triggered and finished successfully
        runTestWithTestCoordinatorEnvironment(this::verifyCheckpointSuccessCase);
    }

    @Test
    public void testCheckpointFailingCase() {
        // checkpoint is being triggered and receive failure on one task
        runTestWithTestCoordinatorEnvironment(this::verifyCheckpointFailCase);
    }

    @Test
    public void testCheckpointTimedOut() {
        // checkpoint is being triggered and timed out
        runTestWithTestCoordinatorEnvironment(this::verifyCheckpointTimedOut);
    }

    @Test
    public void testHandleCoordinatorFailureWhileCheckpointInProgress() {
        // checkpoint is being triggered and coordinator crashed and restarted
        runTestWithTestCoordinatorEnvironment(this::verifyCoordinatorFailureWhileCheckpointInProgressCase);
    }

    @Test
    public void testRollbackBasicCase() {
        // rollback is being triggered and finished successfully
        runTestWithTestCoordinatorEnvironment(this::verifyRollbackSuccessCase);
    }

    @Test
    public void testRollbackFailCase() {
        // rollback is being triggered and receive failure on one task
        runTestWithTestCoordinatorEnvironment(this::verifyRollbackFailCase);
    }

    @Test
    public void testHandleConcurrentFailuresFromOtherTasks() {
        // checkpoint is being triggered and receive failure on multiple task
        runTestWithTestCoordinatorEnvironment(this::verifyCheckpointFailFromMultipleTasksCase);
    }

    @Test
    public void testRollbackTimedOut() {
        // rollback is being triggered and timed out
        runTestWithTestCoordinatorEnvironment(this::verifyRollbackTimeout);
    }

    @Test
    public void testHandleCoordinatorFailureWhileRollbackInProgress() {
        // rollback is being triggered and coordinator crashed and restarted
        runTestWithTestCoordinatorEnvironment(this::verifyCoordinatorFailureWhileRollbackInProgressCase);
    }

    private CheckpointCoordinator initializeCheckpointCoordinator(Map<String, Object> topoConf, TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointCoordinator coordinator = new CheckpointCoordinator();
        coordinator.injectState(testKeyValueState);
        coordinator.prepare(topoConf, mockedContext, mockedCollector);

        // check rollback is triggered from the start of coordinator
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(mockedCollector).emit(ArgumentMatchers.eq(CheckpointConstants.CHECKPOINT_STREAM_ID), valuesCaptor.capture());
        Values emittedValue = valuesCaptor.getValue();
        Assert.assertEquals(CheckpointAction.ROLLBACK_REQUEST, emittedValue.get(1));

        // ignore initializing rollback request for simplicity
        reset(mockedCollector);

        return coordinator;
    }

    private void initializeStateForCoordinator() {
        testKeyValueState.put(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID, String.valueOf(TEST_LAST_SUCCESS_CHECKPOINT_ID));
        // 1 second before current
        testKeyValueState.put(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP, String.valueOf(Time.currentTimeMillis() - 1000));
        testKeyValueState.snapshot(CheckpointConstants.COORDINATOR_TRANSACTION_ID);
    }

    private void verifyCheckpointSuccessCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                             TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyCheckpointRequested(coordinator, mockedCollector);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long checkpointStartedTimestamp = currentState.getCurrentTxStartTimestamp();
        long newCheckpointId = currentState.getLastSuccessCheckpointId() + 1;

        BulkCheckpointActionTuplesProvider.successCheckpointTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                checkpointStartedTimestamp, currentTxId, newCheckpointId);

        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        // it should still be in checkpointing, not triggering new checkpoint
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof Checkpointing);
        Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
        Assert.assertEquals(checkpointStartedTimestamp, currentState.getCurrentTxStartTimestamp());

        Time.advanceTime(10);

        long expectOperationFinishedTimestamp = Time.currentTimeMillis();

        // provide success checkpoint tuple from last task
        coordinator.execute(MockTupleUtils.mockCheckpointTuple(terminalTasks.get(terminalTasks.size() - 1), currentTxId, newCheckpointId));

        // checkpoint finished
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof ReadyCheckpointState);
        Assert.assertEquals(newCheckpointId, currentState.getLastSuccessCheckpointId());
        Assert.assertEquals(expectOperationFinishedTimestamp, currentState.getLastSuccessCheckpointTimestamp());

        // checkpoint information is being stored to state storage
        String lastSuccessTransactionId = testKeyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID);
        String lastSuccessTransactionTimestamp = testKeyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP);
        Assert.assertEquals(Long.valueOf(newCheckpointId), Long.valueOf(lastSuccessTransactionId));
        Assert.assertEquals(Long.valueOf(expectOperationFinishedTimestamp), Long.valueOf(lastSuccessTransactionTimestamp));
    }

    private void verifyCheckpointFailCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks, TopologyContext mockedContext,
                                          OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyCheckpointRequested(coordinator, mockedCollector);

        CheckpointState currentState = coordinator.getCurrentState();
        long newCheckpointId = currentState.getLastSuccessCheckpointId() + 1;
        BulkCheckpointActionTuplesProvider.successCheckpointTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                currentState.getCurrentTxStartTimestamp(), currentState.getCurrentTxId(), newCheckpointId);

        Time.advanceTime(10);

        long expectFirstRollbackRequestTimestamp = Time.currentTimeMillis();

        // received failure from last task
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(terminalTasks.get(terminalTasks.size() - 1)));

        verifyNoMoreInteractions(mockedCollector);

        CheckpointActionTriggeredVerifier.verifyRollbackRequestWithTriggeringRollback(coordinator, mockedCollector, checkpointIntervalMs, newCheckpointId,
                expectFirstRollbackRequestTimestamp);
    }

    private void verifyCheckpointFailFromMultipleTasksCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                                           TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyCheckpointRequested(coordinator, mockedCollector);

        CheckpointState currentState = coordinator.getCurrentState();
        long newCheckpointId = currentState.getLastSuccessCheckpointId() + 1;
        long expectFirstRollbackRequestTimestamp = BulkCheckpointActionTuplesProvider.halfSuccessCheckpointAndHalfRollbackTuples(coordinator,
                terminalTasks, mockedCollector, currentState.getCurrentTxId(), newCheckpointId);

        verifyNoMoreInteractions(mockedCollector);

        CheckpointActionTriggeredVerifier.verifyRollbackRequestWithTriggeringRollback(coordinator, mockedCollector, checkpointIntervalMs, newCheckpointId,
                expectFirstRollbackRequestTimestamp);
    }

    private void verifyCheckpointTimedOut(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                          TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyCheckpointRequested(coordinator, mockedCollector);

        CheckpointState currentState = coordinator.getCurrentState();
        BulkCheckpointActionTuplesProvider.successCheckpointTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                currentState.getCurrentTxStartTimestamp(), currentState.getCurrentTxId(), currentState.getLastSuccessCheckpointId() + 1);

        reset(mockedCollector);

        // let checkpoint being timed out
        nextTickWithForwardingTime(coordinator, stateFactory.getOperationTimeoutMs());

        // checkpoint failed - rollback triggered
        long rollbackTxStarted = Time.currentTimeMillis();
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof RollingBack);
        Assert.assertEquals(rollbackTxStarted, currentState.getCurrentTxStartTimestamp());

        // handle rollback request
        CheckpointActionTriggeredVerifier.verifyRollbackTupleEmitted(mockedCollector, currentState.getCurrentTxId(), currentState.getLastSuccessCheckpointId());
    }

    private void verifyCoordinatorFailureWhileCheckpointInProgressCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                                                       TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyCheckpointRequested(coordinator, mockedCollector);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long currentTxStartTimestamp = currentState.getCurrentTxStartTimestamp();
        long newCheckpointId = currentState.getLastSuccessCheckpointId() + 1;

        BulkCheckpointActionTuplesProvider.successCheckpointTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                currentTxStartTimestamp, currentTxId, newCheckpointId);

        reset(mockedCollector);

        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        // it should still be in checkpointing, not triggering new checkpoint
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof Checkpointing);
        Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
        Assert.assertEquals(currentTxStartTimestamp, currentState.getCurrentTxStartTimestamp());

        Time.advanceTime(10);

        // assuming previous coordinator crashed and restarted
        CheckpointCoordinator newCoordinator = initializeCheckpointCoordinator(getTestTopologyConfiguration(), mockedContext, mockedCollector);

        // last success checkpoint message arrived to next coordinator
        newCoordinator.execute(MockTupleUtils.mockCheckpointTuple(terminalTasks.get(terminalTasks.size() - 1), currentTxId, newCheckpointId));

        // relaunched coordinator is not in progress of checkpoint
        // checkpoint message being ignored
        String lastSuccessTransactionIdFromStateStore = testKeyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID);

        currentState = newCoordinator.getCurrentState();
        Assert.assertFalse(currentState instanceof Checkpointing);
        Assert.assertTrue(currentState instanceof RollbackRequestQueued);
        Assert.assertEquals(Long.valueOf(lastSuccessTransactionIdFromStateStore), Long.valueOf(currentState.getLastSuccessCheckpointId()));
    }

    private void verifyRollbackSuccessCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                           TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(TEST_COORDINATOR_TASK_ID));
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        long rollbackStartedTimestamp = Time.currentTimeMillis();

        CheckpointActionTriggeredVerifier.verifyRollbackRequested(coordinator, mockedCollector, testKeyValueState);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long checkpointId = currentState.getLastSuccessCheckpointId();
        BulkCheckpointActionTuplesProvider.successRollbackTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                rollbackStartedTimestamp, currentTxId, checkpointId);

        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        // it should still be in rolling back, not triggering new checkpoint
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof RollingBack);
        Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
        Assert.assertEquals(rollbackStartedTimestamp, currentState.getCurrentTxStartTimestamp());

        Time.advanceTime(10);

        // provide rollback tuple from last task
        coordinator.execute(MockTupleUtils.mockRollbackTuple(terminalTasks.get(terminalTasks.size() - 1), currentTxId, checkpointId));

        // rollback finished
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof ReadyCheckpointState);
        Assert.assertEquals(checkpointId, currentState.getLastSuccessCheckpointId());
    }

    private void verifyRollbackFailCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                        TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(TEST_COORDINATOR_TASK_ID));
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        long rollbackStartedTimestamp = Time.currentTimeMillis();

        CheckpointActionTriggeredVerifier.verifyRollbackRequested(coordinator, mockedCollector, testKeyValueState);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long checkpointId = currentState.getLastSuccessCheckpointId();
        BulkCheckpointActionTuplesProvider.successRollbackTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                rollbackStartedTimestamp, currentTxId, checkpointId);

        Time.advanceTime(10);

        long expectFirstRollbackRequestTimestamp = Time.currentTimeMillis();

        // received failure from last task
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(terminalTasks.get(terminalTasks.size() - 1)));

        CheckpointActionTriggeredVerifier.verifyRollbackRequestWithTriggeringRollback(coordinator, mockedCollector, checkpointIntervalMs,
                checkpointId + 1, expectFirstRollbackRequestTimestamp);

        // verify another rollback is triggered
        currentState = coordinator.getCurrentState();
        Assert.assertNotEquals(currentTxId, currentState.getCurrentTxId());
    }

    private void verifyRollbackTimeout(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                       TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(TEST_COORDINATOR_TASK_ID));
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        CheckpointActionTriggeredVerifier.verifyRollbackRequested(coordinator, mockedCollector, testKeyValueState);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long checkpointId = currentState.getLastSuccessCheckpointId();
        long expectFirstRollbackRequestTimestamp = BulkCheckpointActionTuplesProvider.halfSuccessRollbackAndHalfRollbackTuples(coordinator,
                terminalTasks, mockedCollector, currentTxId, checkpointId);

        CheckpointActionTriggeredVerifier.verifyRollbackRequestWithTriggeringRollback(coordinator, mockedCollector, checkpointIntervalMs,
                checkpointId + 1, expectFirstRollbackRequestTimestamp);

        // another rollback is started
        currentState = coordinator.getCurrentState();
        Assert.assertNotEquals(currentTxId, currentState.getCurrentTxId());
    }

    private void verifyCoordinatorFailureWhileRollbackInProgressCase(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                                                     TopologyContext mockedContext, OutputCollector mockedCollector) {
        CheckpointStateFactory stateFactory = coordinator.getStateFactory();
        int checkpointIntervalMs = stateFactory.getCheckpointIntervalMs();
        coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(TEST_COORDINATOR_TASK_ID));
        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        long rollbackStartedTimestamp = Time.currentTimeMillis();

        CheckpointActionTriggeredVerifier.verifyRollbackRequested(coordinator, mockedCollector, testKeyValueState);

        CheckpointState currentState = coordinator.getCurrentState();
        String currentTxId = currentState.getCurrentTxId();
        long checkpointId = currentState.getLastSuccessCheckpointId();
        BulkCheckpointActionTuplesProvider.successRollbackTuples(coordinator, terminalTasks, terminalTasks.size() - 1, mockedCollector,
                rollbackStartedTimestamp, currentTxId, checkpointId);

        reset(mockedCollector);

        nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

        // it should still be in rolling back, not triggering new checkpoint
        currentState = coordinator.getCurrentState();
        Assert.assertTrue(currentState instanceof RollingBack);
        Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
        Assert.assertEquals(rollbackStartedTimestamp, currentState.getCurrentTxStartTimestamp());

        Time.advanceTime(10);

        // assuming previous coordinator crashed and restarted
        CheckpointCoordinator newCoordinator = initializeCheckpointCoordinator(getTestTopologyConfiguration(), mockedContext, mockedCollector);

        // last success rollback message arrived to next coordinator
        coordinator.execute(MockTupleUtils.mockRollbackTuple(terminalTasks.get(terminalTasks.size() - 1), currentTxId, checkpointId));

        // relaunched coordinator is not in progress of checkpoint
        // rollback message being ignored
        String lastSuccessCheckpointIdFromStateStore = testKeyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID);

        currentState = newCoordinator.getCurrentState();
        Assert.assertFalse(currentState instanceof RollingBack);
        Assert.assertEquals(Long.valueOf(lastSuccessCheckpointIdFromStateStore), Long.valueOf(currentState.getLastSuccessCheckpointId()));
    }

    private void nextTickWithForwardingTime(CheckpointCoordinator coordinator, int forwardingTimeMs) {
        Time.advanceTime(forwardingTimeMs + 1);
        coordinator.execute(MockTupleUtils.mockTickTuple());
    }

    private Map<String, Object> getTestTopologyConfiguration() {
        Map<String, Object> topoConf = new HashMap<>();
        topoConf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, TEST_CHECKPOINT_INTERVAL);
        topoConf.put(Config.TOPOLOGY_STATE_CHECKPOINT_OPERATION_TIMEOUT, TEST_CHECKPOINT_OPERATION_TIMEOUT);
        return topoConf;
    }

    private TopologyContext mockContext(Map<String, Object> topoConf, Map<String, List<Integer>> terminalTasks) {
        TopologyContext context = mock(TopologyContext.class);
        when(context.getConf()).thenReturn(topoConf);

        Map<GlobalStreamId, Grouping> sources = new HashMap<>();

        for (Map.Entry<String, List<Integer>> entry : terminalTasks.entrySet()) {
            String componentName = entry.getKey();

            sources.put(new GlobalStreamId(componentName, CheckpointConstants.CHECKPOINT_STREAM_ID), Grouping.shuffle(new NullStruct()));
            sources.put(new GlobalStreamId(componentName, Utils.DEFAULT_STREAM_ID), Grouping.shuffle(new NullStruct()));

            List<Integer> tasks = entry.getValue();
            when(context.getComponentTasks(componentName)).thenReturn(tasks);
        }

        when(context.getThisSources()).thenReturn(sources);

        return context;
    }

    public static class BulkCheckpointActionTuplesProvider {
        public static void successCheckpointTuples(CheckpointCoordinator coordinator, List<Integer> terminalTasks, int count,
                                                   OutputCollector mockedCollector, long checkpointStartedTimestamp,
                                                   String currentTxId, long newCheckpointId) {
            for (int i = 0 ; i < count ; i++) {
                // provide success checkpoint tuple
                Integer taskId = terminalTasks.get(i);
                coordinator.execute(MockTupleUtils.mockCheckpointTuple(taskId, currentTxId, newCheckpointId));

                CheckpointState currentState = coordinator.getCurrentState();
                Assert.assertTrue(currentState instanceof Checkpointing);
                Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
                Assert.assertEquals(checkpointStartedTimestamp, currentState.getCurrentTxStartTimestamp());

                Assert.assertTrue(((Checkpointing) currentState).getWaitingTasks().contains(taskId));
            }

            verifyNoMoreInteractions(mockedCollector);
        }

        public static void successRollbackTuples(CheckpointCoordinator coordinator, List<Integer> terminalTasks, int count,
                                                 OutputCollector mockedCollector, long rollbackStartedTimestamp, String currentTxId, long checkpointId) {
            for (int i = 0 ; i < count ; i++) {
                // provide success rollback tuple
                Integer taskId = terminalTasks.get(i);
                coordinator.execute(MockTupleUtils.mockRollbackTuple(taskId, currentTxId, checkpointId));

                CheckpointState currentState = coordinator.getCurrentState();
                Assert.assertTrue(currentState instanceof RollingBack);
                Assert.assertEquals(currentTxId, currentState.getCurrentTxId());
                Assert.assertEquals(rollbackStartedTimestamp, currentState.getCurrentTxStartTimestamp());

                Assert.assertTrue(((RollingBack) currentState).getWaitingTasks().contains(taskId));
            }

            verifyNoMoreInteractions(mockedCollector);
        }

        public static long halfSuccessCheckpointAndHalfRollbackTuples(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                                                      OutputCollector mockedCollector, String currentTxId, long checkpointId) {
            long expectFirstRollbackRequestTimestamp = -1;
            for (int i = 0 ; i < terminalTasks.size() ; i++) {
                // provide checkpoint tuples from half of tasks
                // provide rollback request tuples from another half of tasks

                Integer taskId = terminalTasks.get(i);
                if (i % 2 == 0) {
                    coordinator.execute(MockTupleUtils.mockCheckpointTuple(taskId, currentTxId, checkpointId));
                    Time.advanceTime(1);
                } else {
                    coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(taskId));
                    if (expectFirstRollbackRequestTimestamp < 0) {
                        expectFirstRollbackRequestTimestamp = Time.currentTimeMillis();
                    }
                    Time.advanceTime(1);
                }
            }

            verifyNoMoreInteractions(mockedCollector);

            return expectFirstRollbackRequestTimestamp;
        }

        public static long halfSuccessRollbackAndHalfRollbackTuples(CheckpointCoordinator coordinator, List<Integer> terminalTasks,
                                                                    OutputCollector mockedCollector, String currentTxId, long checkpointId) {
            long expectFirstRollbackRequestTimestamp = -1;
            for (int i = 0 ; i < terminalTasks.size() ; i++) {
                // provide rollback tuples from half of tasks
                // provide rollback request tuples from another half of tasks

                Integer taskId = terminalTasks.get(i);
                if (i % 2 == 0) {
                    coordinator.execute(MockTupleUtils.mockRollbackTuple(taskId, currentTxId, checkpointId));
                    Time.advanceTime(1);
                } else {
                    coordinator.execute(MockTupleUtils.mockRollbackRequestTuple(taskId));
                    if (expectFirstRollbackRequestTimestamp < 0) {
                        expectFirstRollbackRequestTimestamp = Time.currentTimeMillis();
                    }
                    Time.advanceTime(1);
                }
            }

            verifyNoMoreInteractions(mockedCollector);

            return expectFirstRollbackRequestTimestamp;
        }
    }

    public static class CheckpointActionTriggeredVerifier {
        public static void verifyCheckpointRequested(CheckpointCoordinator coordinator, OutputCollector mockedCollector) {
            long checkpointStartedTimestamp = Time.currentTimeMillis();
            CheckpointState currentState = coordinator.getCurrentState();
            Assert.assertTrue(currentState instanceof Checkpointing);
            Assert.assertEquals(checkpointStartedTimestamp, currentState.getCurrentTxStartTimestamp());

            String currentTxId = currentState.getCurrentTxId();
            long newCheckpointId = currentState.getLastSuccessCheckpointId() + 1;

            verifyCheckpointTupleEmitted(mockedCollector, currentTxId, newCheckpointId);
        }

        public static void verifyRollbackRequested(CheckpointCoordinator coordinator, OutputCollector mockedCollector,
                                                   KeyValueState<String, String> keyValueState) {
            CheckpointState currentState = coordinator.getCurrentState();

            long rollbackStartedTimestamp = Time.currentTimeMillis();
            // check that checkpoint information is being loaded from state storage
            String lastSuccessTransactionIdFromStateStore = keyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_ID);
            String lastSuccessTransactionTimestampFromStateStore = keyValueState.get(CheckpointConstants.STATE_KEY_LAST_SUCCESS_CHECKPOINT_TIMESTAMP);

            Assert.assertEquals(Long.valueOf(lastSuccessTransactionIdFromStateStore), Long.valueOf(currentState.getLastSuccessCheckpointId()));
            Assert.assertEquals(Long.valueOf(lastSuccessTransactionTimestampFromStateStore), Long.valueOf(currentState.getLastSuccessCheckpointTimestamp()));

            String currentTxId = currentState.getCurrentTxId();
            long checkpointId = currentState.getLastSuccessCheckpointId();

            Assert.assertTrue(currentState instanceof RollingBack);
            Assert.assertEquals(rollbackStartedTimestamp, currentState.getCurrentTxStartTimestamp());

            verifyRollbackTupleEmitted(mockedCollector, currentTxId, checkpointId);
        }

        public static void verifyRollbackRequestWithTriggeringRollback(CheckpointCoordinator coordinator, OutputCollector mockedCollector,
                                                                       int checkpointIntervalMs, long newCheckpointId, long expectFirstRollbackRequestTimestamp) {
            // rollback request is in queue
            CheckpointState currentState = coordinator.getCurrentState();
            Assert.assertTrue(currentState instanceof RollbackRequestQueued);
            Assert.assertNotEquals(newCheckpointId, currentState.getLastSuccessCheckpointId());
            Assert.assertEquals(expectFirstRollbackRequestTimestamp, currentState.getCurrentTxStartTimestamp());

            reset(mockedCollector);

            // finish waiting rollback requests and initiate rollback
            nextTickWithForwardingTime(coordinator, checkpointIntervalMs);

            long rollbackExecutedTime = Time.currentTimeMillis();

            // handle rollback request
            currentState = coordinator.getCurrentState();
            Assert.assertTrue(currentState instanceof RollingBack);
            Assert.assertEquals(rollbackExecutedTime, currentState.getCurrentTxStartTimestamp());
            CheckpointActionTriggeredVerifier.verifyRollbackTupleEmitted(mockedCollector, currentState.getCurrentTxId(), currentState.getLastSuccessCheckpointId());
        }

        private static void nextTickWithForwardingTime(CheckpointCoordinator coordinator, int forwardingTimeMs) {
            Time.advanceTime(forwardingTimeMs + 1);
            coordinator.execute(MockTupleUtils.mockTickTuple());
        }

        private static void verifyCheckpointTupleEmitted(OutputCollector mockedCollector, String currentTxId, long newCheckpointId) {
            ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
            verify(mockedCollector).emit(ArgumentMatchers.eq(CheckpointConstants.CHECKPOINT_STREAM_ID), valuesCaptor.capture());
            Values emittedValue = valuesCaptor.getValue();
            Assert.assertEquals(currentTxId, emittedValue.get(0));
            Assert.assertEquals(CheckpointAction.CHECKPOINT, emittedValue.get(1));
            Assert.assertEquals(newCheckpointId, emittedValue.get(2));
        }

        private static void verifyRollbackTupleEmitted(OutputCollector mockedCollector, String currentTxId, long checkpointId) {
            ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
            verify(mockedCollector).emit(ArgumentMatchers.eq(CheckpointConstants.CHECKPOINT_STREAM_ID), valuesCaptor.capture());
            Values emittedValue = valuesCaptor.getValue();
            Assert.assertEquals(currentTxId, emittedValue.get(0));
            Assert.assertEquals(CheckpointAction.ROLLBACK, emittedValue.get(1));
            Assert.assertEquals(checkpointId, emittedValue.get(2));
        }
    }

    public static class ComponentToTasksMapGenerator {
        public static Map<String, List<Integer>> createTerminalTasks(String componentNamePrefix, int componentCount, int countPerComponent) {
            Map<String, List<Integer>> ret = new HashMap<>();
            int curTaskIdx = START_TERMINAL_TASK_INDEX;
            for (int i = 0 ; i < componentCount ; i++) {
                String componentName = componentNamePrefix + "-" + i;
                List<Integer> tasks = new ArrayList<>(countPerComponent);
                for (int j = 0 ; j < countPerComponent ; j++) {
                    tasks.add(curTaskIdx++);
                }
                ret.put(componentName, tasks);
            }
            return ret;
        }

        public static List<Integer> flattenTasks(Map<String, List<Integer>> terminalTasksMap) {
            return terminalTasksMap.entrySet().stream()
                    .flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
        }
    }

    public static class MockTupleUtils {
        public static Tuple mockTickTuple() {
            Tuple mockTuple = mock(Tuple.class);
            when(mockTuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
            when(mockTuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
            return mockTuple;
        }

        public static Tuple mockCheckpointTuple(int taskId, String txId, long checkpointId) {
            Tuple mockTuple = mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn(CheckpointConstants.CHECKPOINT_STREAM_ID);
            when(mockTuple.getValueByField(CheckpointConstants.CHECKPOINT_FIELD_ACTION)).thenReturn(CheckpointAction.CHECKPOINT);
            when(mockTuple.getStringByField(CheckpointConstants.CHECKPOINT_FIELD_TXID)).thenReturn(txId);
            when(mockTuple.getLongByField(CheckpointConstants.CHECKPOINT_FIELD_CHECKPOINT_ID)).thenReturn(checkpointId);
            when(mockTuple.getSourceTask()).thenReturn(taskId);
            return mockTuple;
        }

        public static Tuple mockRollbackTuple(int taskId, String txId, long checkpointId) {
            Tuple mockTuple = mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn(CheckpointConstants.CHECKPOINT_STREAM_ID);
            when(mockTuple.getValueByField(CheckpointConstants.CHECKPOINT_FIELD_ACTION)).thenReturn(CheckpointAction.ROLLBACK);
            when(mockTuple.getStringByField(CheckpointConstants.CHECKPOINT_FIELD_TXID)).thenReturn(txId);
            when(mockTuple.getLongByField(CheckpointConstants.CHECKPOINT_FIELD_CHECKPOINT_ID)).thenReturn(checkpointId);
            when(mockTuple.getSourceTask()).thenReturn(taskId);
            return mockTuple;
        }

        public static Tuple mockRollbackRequestTuple(int taskId) {
            Tuple mockTuple = mock(Tuple.class);
            when(mockTuple.getSourceStreamId()).thenReturn(CheckpointConstants.CHECKPOINT_STREAM_ID);
            when(mockTuple.getValueByField(CheckpointConstants.CHECKPOINT_FIELD_ACTION)).thenReturn(CheckpointAction.ROLLBACK_REQUEST);
            when(mockTuple.getStringByField(CheckpointConstants.CHECKPOINT_FIELD_TXID)).thenReturn(CheckpointConstants.ROLLBACK_REQUEST_DUMMY_TXID);
            when(mockTuple.getLongByField(CheckpointConstants.CHECKPOINT_FIELD_CHECKPOINT_ID)).thenReturn(CheckpointConstants.ROLLBACK_REQUEST_DUMMY_CHECKPOINT_ID);
            when(mockTuple.getSourceTask()).thenReturn(taskId);
            return mockTuple;
        }
    }

}