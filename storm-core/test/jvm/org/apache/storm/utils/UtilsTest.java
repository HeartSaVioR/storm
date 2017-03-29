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

package org.apache.storm.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.junit.Test;
import org.junit.Assert;

import org.apache.storm.Config;
import org.apache.thrift.transport.TTransportException;

public class UtilsTest {
    public void getConfiguredClientThrowsRuntimeExceptionOnBadArgsTest () throws TTransportException {
        Map config = ClientConfigUtils.readStormConfig();
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 0);

        try {
            new NimbusClient(config, "", 65535);
            Assert.fail("Expected exception to be thrown");
        } catch (RuntimeException e){
            Assert.assertTrue(
                "Cause is not TTransportException " + e,  
                ClientUtils.exceptionCauseIsInstanceOf(TTransportException.class, e));
        }
    }

    private Map mockMap(String key, String value) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(key, value);
        return map;
    }

    private Map serverMockMap(String value) {
        return mockMap(Config.STORM_ZOOKEEPER_AUTH_SCHEME, value);
    }

    private Map emptyMockMap() {
        return new HashMap<String, Object>();
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerTest() {
        Assert.assertFalse(
            "Returns false if given null config",
            Utils.isZkAuthenticationConfiguredStormServer(null));

        Assert.assertFalse(
            "Returns false if scheme key is missing",
            Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));

        Assert.assertFalse(
            "Returns false if scheme value is null",
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap(null)));

        Assert.assertTrue(
            "Returns true if scheme value is string",
            Utils.isZkAuthenticationConfiguredStormServer(serverMockMap("foobar")));
    }

    @Test
    public void isZkAuthenticationConfiguredStormServerWithPropertyTest() {
        String key = "java.security.auth.login.config";
        String oldValue = System.getProperty(key);
        try {
            System.setProperty("java.security.auth.login.config", "anything");
            Assert.assertTrue(Utils.isZkAuthenticationConfiguredStormServer(emptyMockMap()));
        } finally {
            // reset property
            if (oldValue == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, oldValue);
            }
        }
    }

}
