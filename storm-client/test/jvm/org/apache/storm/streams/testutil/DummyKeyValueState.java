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

import org.apache.storm.state.DefaultStateEncoder;
import org.apache.storm.state.Serializer;
import org.apache.storm.streams.state.KeyValueState;
import org.apache.storm.streams.state.StateStorage;

public class DummyKeyValueState<K, V> implements KeyValueState<K, V> {
    private final StateStorage stateStorage;
    private final byte[] namespace;
    private final DefaultStateEncoder<K, V> encoder;

    public DummyKeyValueState(StateStorage stateStorage, byte[] namespace,
                              Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.stateStorage = stateStorage;
        this.namespace = namespace;
        this.encoder = new DefaultStateEncoder<>(keySerializer, valueSerializer);
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
