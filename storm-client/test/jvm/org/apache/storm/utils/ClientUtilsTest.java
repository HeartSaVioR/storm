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

package org.apache.storm.utils;

import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ClientUtilsTest {

    @Test
    public void isZkAuthenticationConfiguredTopologyTest() {
        Assert.assertFalse(
                "Returns null if given null config",
                ClientUtils.isZkAuthenticationConfiguredTopology(null));

        Assert.assertFalse(
                "Returns false if scheme key is missing",
                ClientUtils.isZkAuthenticationConfiguredTopology(emptyMockMap()));

        Assert.assertFalse(
                "Returns false if scheme value is null",
                ClientUtils.isZkAuthenticationConfiguredTopology(topologyMockMap(null)));

        Assert.assertTrue(
                "Returns true if scheme value is string",
                ClientUtils.isZkAuthenticationConfiguredTopology(topologyMockMap("foobar")));
    }


    private Map mockMap(String key, String value) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    private Map topologyMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME, value);
    }

    private Map emptyMockMap() {
        return new HashMap<String, Object>();
    }

    @Test
    public void parseJvmHeapMemByChildOptsTest() {
        Assert.assertEquals(
                "1024K results in 1 MB",
                ClientUtils.parseJvmHeapMemByChildOpts("Xmx1024K", 0.0).doubleValue(), 1.0, 0);

        Assert.assertEquals(
                "100M results in 100 MB",
                ClientUtils.parseJvmHeapMemByChildOpts("Xmx100M", 0.0).doubleValue(), 100.0, 0);

        Assert.assertEquals(
                "1G results in 1024 MB",
                ClientUtils.parseJvmHeapMemByChildOpts("Xmx1G", 0.0).doubleValue(), 1024.0, 0);

        Assert.assertEquals(
                "Unmatched value results in default",
                ClientUtils.parseJvmHeapMemByChildOpts("Xmx1T", 123.0).doubleValue(), 123.0, 0);

        Assert.assertEquals(
                "Null value results in default",
                ClientUtils.parseJvmHeapMemByChildOpts(null, 123.0).doubleValue(), 123.0, 0);
    }
}
