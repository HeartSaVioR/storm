/**
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
 */
package org.apache.storm.metrics2;

import com.codahale.metrics.Counter;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskMetrics {
    private static final String METRIC_NAME_ACKED = "acked";
    private static final String METRIC_NAME_FAILED = "failed";
    private static final String METRIC_NAME_EMITTED = "emitted";
    private static final String METRIC_NAME_TRANSFERRED = "transferred";

    private Map<String, Counter> ackedByStream;
    private Map<String, Counter> failedByStream;
    private Map<String, Counter> emittedByStream;
    private Map<String, Counter> transferredByStream;

    private String componentId;

    public TaskMetrics(WorkerTopologyContext context, String componentId, Integer taskId) {
        this.componentId = componentId;

        String topologyId = context.getStormId();
        Integer workerPort = context.getThisWorkerPort();

        preInitializeMetricMap(context, topologyId, componentId, taskId, workerPort);
    }

    public Counter getAcked(String streamId) {
        return getCounter(streamId, this.ackedByStream);
    }

    public Counter getFailed(String streamId) {
        return getCounter(streamId, this.failedByStream);
    }

    public Counter getEmitted(String streamId) {
        return getCounter(streamId, this.emittedByStream);
    }

    public Counter getTransferred(String streamId) {
        return getCounter(streamId, this.transferredByStream);
    }

    private Counter getCounter(String streamId, Map<String, Counter> counterByStream) {
        Counter c = counterByStream.get(streamId);
        if (c == null) {
            throw new RuntimeException("Unknown stream ID provided: " + streamId + " for component " + componentId);
        }
        return c;
    }

    private void preInitializeMetricMap(WorkerTopologyContext context, String topologyId, String componentId,
                                        Integer taskId, Integer workerPort) {
        Set<String> inputAndOutputStreams = new HashSet<>();
        inputAndOutputStreams.add(Utils.DEFAULT_STREAM_ID);

        // input streams
        Map<GlobalStreamId, Grouping> sources = context.getSources(componentId);
        for (GlobalStreamId globalStreamId : sources.keySet()) {
            inputAndOutputStreams.add(globalStreamId.get_streamId());
        }

        System.out.println("DEBUG: component " + componentId + " -> inputStreams " + inputAndOutputStreams);

        // output streams
        Set<String> outputStreams = context.getComponentStreams(componentId);

        System.out.println("DEBUG: component " + componentId + " -> outputStreams " + outputStreams);

        inputAndOutputStreams.addAll(outputStreams);

        this.ackedByStream = preInitializeStreamToCounter(METRIC_NAME_ACKED, topologyId, componentId, taskId, workerPort,
                inputAndOutputStreams);
        this.failedByStream = preInitializeStreamToCounter(METRIC_NAME_FAILED, topologyId, componentId, taskId, workerPort,
                inputAndOutputStreams);
        this.emittedByStream = preInitializeStreamToCounter(METRIC_NAME_EMITTED, topologyId, componentId, taskId, workerPort,
                inputAndOutputStreams);
        this.transferredByStream = preInitializeStreamToCounter(METRIC_NAME_TRANSFERRED, topologyId, componentId, taskId, workerPort,
                inputAndOutputStreams);
    }

    private Map<String, Counter> preInitializeStreamToCounter(String metricName, String topologyId, String componentId, Integer taskId,
                                                              Integer workerPort, Set<String> streams) {
        Map<String, Counter> streamToCounter = new HashMap<>();
        for (String stream : streams) {
            streamToCounter.put(stream, StormMetricRegistry.counter(metricName, topologyId, componentId, taskId, workerPort, stream));
        }
        return streamToCounter;
    }

}
