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
package org.apache.storm.redis.tools.encoder;

import com.google.common.base.Optional;
import org.apache.commons.codec.binary.Base64;

/**
 * Helper class for encoding/decoding redis key values.
 */
public class NewRedisEncoder<K, V> {

    public static final Serializer<Optional<byte[]>> internalValueSerializer = new DefaultStateSerializer<>();

    public static final byte[] TOMBSTONE = internalValueSerializer.serialize(Optional.<byte[]>absent());

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public NewRedisEncoder(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public Serializer<K> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<V> getValueSerializer() {
        return valueSerializer;
    }

    public byte[] encodeKey(K key) {
        return keySerializer.serialize(key);
    }

    public byte[] encodeValue(V value) {
        return internalValueSerializer.serialize(
                Optional.of(valueSerializer.serialize(value)));
    }

    public K decodeKey(byte[] rowKey) {
        return keySerializer.deserialize(rowKey);
    }

    public V decodeValue(byte[] value) {
        Optional<byte[]> internalValue = internalValueSerializer.deserialize(value);
        if (internalValue.isPresent()) {
            return valueSerializer.deserialize(internalValue.get());
        }
        return null;
    }
}