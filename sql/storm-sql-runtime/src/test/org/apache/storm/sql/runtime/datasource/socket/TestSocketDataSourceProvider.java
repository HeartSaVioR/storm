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

package org.apache.storm.sql.runtime.datasource.socket;

import com.google.common.collect.ImmutableList;
import org.apache.storm.sql.runtime.DataSourcesRegistry;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.datasource.socket.bolt.SocketBolt;
import org.apache.storm.sql.runtime.serde.json.JsonSerializer;
import org.apache.storm.topology.IRichBolt;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public class TestSocketDataSourceProvider {
    private static final List<FieldInfo> FIELDS = ImmutableList.of(
            new FieldInfo("ID", int.class, true),
            new FieldInfo("val", String.class, false));
    private static final List<String> FIELD_NAMES = ImmutableList.of("ID", "val");
    private static final JsonSerializer SERIALIZER = new JsonSerializer(FIELD_NAMES);

    @Test
    public void testSocketSink() throws IOException {
        ISqlStreamsDataSource ds = DataSourcesRegistry.constructStreamsDataSource(
                URI.create("socket://localhost:8888"), null, null, new Properties(), FIELDS);
        Assert.assertNotNull(ds);

        IRichBolt consumer = ds.getConsumer();

        Assert.assertEquals(SocketBolt.class, consumer.getClass());
    }
}
