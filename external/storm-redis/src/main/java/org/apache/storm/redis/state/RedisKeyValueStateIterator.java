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
package org.apache.storm.redis.state;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.redis.utils.ByteArrayWrapper;
import org.apache.storm.redis.utils.RedisEncoder;
import org.apache.storm.state.Serializer;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An iterator over {@link RedisKeyValueState}
 */
public class RedisKeyValueStateIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    private final byte[] namespace;
    private final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> pendingPrepareIterator;
    private final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> pendingCommitIterator;
    private final RedisEncoder<K, V> decoder;
    private final RedisCommandsInstanceContainer container;
    private final ScanParams scanParams;
    private Iterator<Map.Entry<ByteArrayWrapper, byte[]>> pendingIterator;
    private byte[] cursor;
    private List<Map.Entry<ByteArrayWrapper, byte[]>> cachedResult;
    private int readPosition;

    public RedisKeyValueStateIterator(byte[] namespace, RedisCommandsInstanceContainer container,
                                      Iterator<Map.Entry<ByteArrayWrapper, byte[]>> pendingPrepareIterator,
                                      Iterator<Map.Entry<ByteArrayWrapper, byte[]>> pendingCommitIterator,
                                      int chunkSize, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.namespace = namespace;
        this.pendingPrepareIterator = pendingPrepareIterator;
        this.pendingCommitIterator = pendingCommitIterator;
        this.container = container;
        this.decoder = new RedisEncoder<K, V>(keySerializer, valueSerializer);
        this.scanParams = new ScanParams().count(chunkSize);
        this.cursor = ScanParams.SCAN_POINTER_START_BINARY;
    }

    @Override
    public boolean hasNext() {
        if (pendingPrepareIterator != null && pendingPrepareIterator.hasNext()) {
            pendingIterator = pendingPrepareIterator;
            return true;
        } else if (pendingCommitIterator != null && pendingCommitIterator.hasNext()) {
            pendingIterator = pendingCommitIterator;
            return true;
        } else {
            pendingIterator = null;
            return !Arrays.equals(cursor, ScanParams.SCAN_POINTER_START_BINARY);
        }
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Map.Entry<ByteArrayWrapper, byte[]> redisKeyValue = null;
        if (pendingIterator != null) {
            redisKeyValue = pendingIterator.next();
        } else {
            if (cachedResult == null || readPosition >= cachedResult.size()) {
                RedisCommands commands = null;
                try {
                    commands = container.getInstance();
                    ScanResult<Map.Entry<byte[], byte[]>> scanResult = commands.hscan(namespace, cursor, scanParams);
                    cachedResult = wrapEntries(scanResult.getResult());
                    cursor = scanResult.getCursorAsBytes();
                    readPosition = 0;
                } finally {
                    container.returnInstance(commands);
                }
            }
            redisKeyValue = cachedResult.get(readPosition);
            readPosition += 1;
        }

        K key = decoder.decodeKey(redisKeyValue.getKey().unwrap());
        V value = decoder.decodeValue(redisKeyValue.getValue());
        return new AbstractMap.SimpleEntry(key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private List<Map.Entry<ByteArrayWrapper, byte[]>> wrapEntries(List<Map.Entry<byte[], byte[]>> sourceList) {
        List<Map.Entry<ByteArrayWrapper, byte[]>> entries = new ArrayList<>(sourceList.size());

        for (Map.Entry<byte[], byte[]> entry : sourceList) {
            entries.add(new AbstractMap.SimpleEntry<>(new ByteArrayWrapper(entry.getKey()), entry.getValue()));
        }

        return entries;
    }
}
