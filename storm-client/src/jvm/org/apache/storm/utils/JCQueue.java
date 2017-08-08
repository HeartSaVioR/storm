/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.storm.utils;

// TODO: Remove max.spout.pending, fix netty issue
// TODO: Flush tuple timer needs to check if topology.transfer.batch.size>1
// DOCS: Document topology.producer.batch.size, topology.flush.tuple.freq.millis & deprecations, topology.spout.recvq.skips, topo.wait.strategy.*
// DOCS: In release notes, mention that users may want to retweak their topology.executor.receive.buffer.size & topology.producer.batch.size
// DOCS: Add perf tweaking notes: sampling rate, batch size (executor&transfer), spout skip count, sleep strategy, load aware, wait.strategy


import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.metric.api.IStatefulObject;
import org.apache.storm.metric.internal.RateTracker;
import org.jctools.queues.ConcurrentCircularArrayQueue;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;
import org.jctools.queues.SpscArrayQueue;

import java.util.ArrayList;
import java.util.HashMap;


public final class JCQueue implements IStatefulObject {

    public enum ProducerKind {SINGLE, MULTI}

    public static final Object INTERRUPT = new Object();

    private ThroughputMeter emptyMeter = new ThroughputMeter("EmptyBatch");

    private interface Inserter {
        // blocking call that can be interrupted with Thread.interrupt()
        void add(Object obj) throws InterruptedException;

        void flush() throws InterruptedException;
    }

    /* Thread safe. Same instance can be used across multiple threads */
    private static class DirectInserter implements Inserter {
        private JCQueue q;

        public DirectInserter(JCQueue q) {
            this.q = q;
        }

        /**
         * Blocking call, that can be interrupted via Thread.interrupt
         */
        @Override
        public void add(Object obj) throws InterruptedException {
            boolean inserted = q.tryPublishInternal(obj);
            int idleCount = 0;
            while (!inserted) {
                q.metrics.notifyInsertFailure();
                idleCount = q.backPressureWaitStrategy.idle(idleCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                inserted = q.tryPublishInternal(obj);
            }

        }

        @Override
        public void flush() throws InterruptedException {
            return;
        }
    } // class DirectInserter

    private static class BatchInserter implements Inserter {
        private JCQueue q;
        private final int batchSz;
        private ArrayList<Object> currentBatch;

        public BatchInserter(JCQueue q, int batchSz) {
            this.q = q;
            this.batchSz = batchSz;
            this.currentBatch = new ArrayList<>(batchSz + 1);
        }

        @Override
        public void add(Object obj) throws InterruptedException {
            currentBatch.add(obj);
            if (currentBatch.size() >= batchSz) {
                flush();
            }
        }

        @Override
        /** Blocking call - Does not return until at least 1 element is drained or Thread.interrupt() is received */
        public void flush() throws InterruptedException {
            if (currentBatch.isEmpty()) {
                return;
            }
            int publishCount = q.tryPublishInternal(currentBatch);
            int retryCount = 0;
            while (publishCount == 0) { // retry till at least 1 element is drained
                q.metrics.notifyInsertFailure();
                retryCount = q.backPressureWaitStrategy.idle(retryCount);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                publishCount = q.tryPublishInternal(currentBatch);
            }
            currentBatch.subList(0, publishCount).clear();
        }
    } // class BatchInserter

    /**
     * This inner class provides methods to access the metrics of the disruptor queue.
     */
    public class QueueMetrics {
        private final RateTracker arrivalsTracker = new RateTracker(10000, 10);
        private final RateTracker insertFailuresTracker = new RateTracker(10000, 10);

        public long population() {
            return queue.size();
        }

        public long capacity() {
            return queue.capacity();
        }

        public Object getState() {
            HashMap state = new HashMap<String, Object>();

            final double arrivalRateInSecs = arrivalsTracker.reportRate();

            long tuplePop = population();

            // Assume the queue is stable, in which the arrival rate is equal to the consumption rate.
            // If this assumption does not hold, the calculation of sojourn time should also consider
            // departure rate according to Queuing Theory.
            final double sojournTime = tuplePop / Math.max(arrivalRateInSecs, 0.00001) * 1000.0;

            long cap = capacity();
            float pctFull = (1.0F * tuplePop / cap);

            state.put("capacity", cap);
            state.put("pct_full", pctFull);
            state.put("population", tuplePop);

            state.put("arrival_rate_secs", arrivalRateInSecs);
            state.put("sojourn_time_ms", sojournTime); //element sojourn time in milliseconds
            state.put("insert_failures", insertFailuresTracker.reportRate());

            return state;
        }

        public void notifyArrivals(long counts) {
            arrivalsTracker.notify(counts); // TODO: PERF: This is a perf bottleneck esp in when batchSz=1
        }

        public void notifyInsertFailure() {
            insertFailuresTracker.notify(1);
        }

        public void close() {
            arrivalsTracker.close();
            insertFailuresTracker.close();
        }

    }

    private final ConcurrentCircularArrayQueue<Object> queue;

    private final int producerBatchSz;
    private final DirectInserter directInserter = new DirectInserter(this);

    private final ThreadLocal<BatchInserter> thdLocalBatcher = new ThreadLocal<BatchInserter>();

    private final JCQueue.QueueMetrics metrics;

    private String queueName;
    private final IWaitStrategy backPressureWaitStrategy;

    public JCQueue(String queueName, int size, int inputBatchSize, IWaitStrategy backPressureWaitStrategy) {
        this(queueName, ProducerKind.MULTI, size, inputBatchSize, backPressureWaitStrategy);
    }

    public JCQueue(String queueName, ProducerKind type, int size, int inputBatchSize, IWaitStrategy backPressureWaitStrategy) {
        this.queueName = queueName;

        if (type == ProducerKind.SINGLE) {
            this.queue = new SpscArrayQueue<>(size);
        } else {
            this.queue = new MpscArrayQueue<>(size);
        }

        this.metrics = new JCQueue.QueueMetrics();

        //The batch size can be no larger than half the full queue size, to avoid contention issues.
        this.producerBatchSz = Math.max(1, Math.min(inputBatchSize, size / 2));
        this.backPressureWaitStrategy = backPressureWaitStrategy;
    }

    public String getName() {
        return queueName;
    }


    public void haltWithInterrupt() {
        if (tryPublishInternal(INTERRUPT)) {
            metrics.close();
        } else {
            throw new RuntimeException(new QueueFullException());
        }
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q
     */
    public int consume(JCQueue.Consumer consumer) {
        try {
            return consumerImpl(consumer);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Non blocking. Returns immediately if Q is empty. Returns number of elements consumed from Q
     */
    private int consumerImpl(Consumer consumer) throws InterruptedException {
        int count = queue.drain(consumer);
        if (count > 0) {
            consumer.flush();
        } else {
            emptyMeter.record();
        }
        return count;
    }

    // Non Blocking. returns true/false indicating success/failure
    private boolean tryPublishInternal(Object obj) {
        if (queue.offer(obj)) {
            metrics.notifyArrivals(1);
            return true;
        }
        return false;
    }

    // Non Blocking. returns count of how many inserts succeeded
    private int tryPublishInternal(ArrayList<Object> objs) {
        MessagePassingQueue.Supplier<Object> supplier =
            new MessagePassingQueue.Supplier<Object>() {
                int i = 0;

                @Override
                public Object get() {
                    return objs.get(i++);
                }
            };
        int count = queue.fill(supplier, objs.size());
        metrics.notifyArrivals(count);
        return count;
    }

    /**
     * Blocking call. Retries till it can successfully publish the obj. Can be interrupted via Thread.interrupt().
     */
    public void publish(Object obj) throws InterruptedException {
        Inserter inserter;
        if (producerBatchSz > 1) {
            inserter = thdLocalBatcher.get();
            if (inserter == null) {
                BatchInserter b = new BatchInserter(this, producerBatchSz);
                inserter = b;
                thdLocalBatcher.set(b);
            }
        } else {
            inserter = directInserter;
        }
        inserter.add(obj);
    }

    /**
     * Non-blocking call, returns false if failed
     **/
    public boolean tryPublish(Object obj) {
        return tryPublishInternal(obj);
    }

    /**
     * if(batchSz>1)  : Blocking call. Does not return until at least 1 element is drained or Thread.interrupt() is received
     * if(batchSz==1) : NO-OP. Returns immediately. doesnt throw.
     */
    public void flush() throws InterruptedException {
        Inserter inserter = thdLocalBatcher.get();
        if (inserter != null) {
            inserter.flush();
        }
    }

    @Override
    public Object getState() {
        return metrics.getState();
    }


    //This method enables the metrics to be accessed from outside of the JCQueue class
    public JCQueue.QueueMetrics getMetrics() {
        return metrics;
    }

    private class QueueFullException extends Exception {
        public QueueFullException() {
            super(queueName + " is full");
        }
    }

    public interface Consumer extends org.jctools.queues.MessagePassingQueue.Consumer<Object> {
        void accept(Object event);

        void flush() throws InterruptedException;
    }
}