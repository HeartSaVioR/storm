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

package org.apache.storm.streams.testutil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import org.apache.storm.streams.state.StateStorage;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyStateStorage implements StateStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DummyStateStorage.class);

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
    public void initialize(Map<String, Object> topoConf, TopologyContext context) {
        // no-op
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

    @Override
    public void close() throws Exception {
        // no-op
    }
}
