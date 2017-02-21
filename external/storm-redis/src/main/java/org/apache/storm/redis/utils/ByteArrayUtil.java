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
 */

package org.apache.storm.redis.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ByteArrayUtil {
    private ByteArrayUtil() {
    }

    public static class Maps {
        public static Map<ByteArrayWrapper, byte[]> newHashMapWrappingKey(Map<byte[], byte[]> source) {
            Map<ByteArrayWrapper, byte[]> destination = new HashMap<>(source.size());
            wrapEntities(source, destination);
            return destination;
        }

        public static Map<ByteArrayWrapper, byte[]> newConcurrentHashMapWrappingKey(Map<byte[], byte[]> source) {
            Map<ByteArrayWrapper, byte[]> destination = new ConcurrentHashMap<>(source.size());
            wrapEntities(source, destination);
            return destination;
        }

        public static Map<byte[], byte[]> newHashMapUnwrappingKey(Map<ByteArrayWrapper, byte[]> source) {
            Map<byte[], byte[]> destination = new ConcurrentHashMap<>(source.size());
            unwrapEntities(source, destination);
            return destination;
        }

        public static void wrapEntities(Map<byte[], byte[]> source, Map<ByteArrayWrapper, byte[]> destination) {
            for (Map.Entry<byte[], byte[]> entry : source.entrySet()) {
                destination.put(new ByteArrayWrapper(entry.getKey()), entry.getValue());
            }
        }

        public static void unwrapEntities(Map<ByteArrayWrapper, byte[]> source, Map<byte[], byte[]> destination) {
            for (Map.Entry<ByteArrayWrapper, byte[]> entry : source.entrySet()) {
                destination.put(entry.getKey().unwrap(), entry.getValue());
            }
        }
    }

}
