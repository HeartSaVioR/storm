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

package org.apache.storm.redis.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.redis.tools.encoder.NewRedisEncoder;
import org.apache.storm.redis.tools.encoder.OldRedisEncoder;
import org.apache.storm.redis.tools.encoder.Serializer;
import org.apache.storm.redis.tools.state.CheckPointState;
import org.apache.storm.redis.tools.state.NewCheckPointState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.util.SafeEncoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Base64ToBinaryStateMigrationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(Base64ToBinaryStateMigrationUtil.class);
    private static final String OPTION_REDIS_HOST_SHORT = "h";
    private static final String OPTION_REDIS_HOST_LONG = "host";
    private static final String OPTION_REDIS_PORT_SHORT = "p";
    private static final String OPTION_REDIS_PORT_LONG = "port";
    private static final String OPTION_REDIS_PASSWORD_LONG = "password";
    private static final String OPTION_REDIS_DB_NUM_SHORT = "d";
    private static final String OPTION_REDIS_DB_NUM_LONG = "dbnum";
    private static final String OPTION_NAMESPACE_SHORT = "n";
    private static final String OPTION_NAMESPACE_LONG = "namespace";
    private static final String OPTION_KEY_SERIALIZER_SHORT = "k";
    private static final String OPTION_KEY_SERIALIZER_LONG = "key";
    private static final String OPTION_VALUE_SERIALIZER_SHORT = "v";
    private static final String OPTION_VALUE_SERIALIZER_LONG = "value";
    public static final String DEFAULT_STATE_SERIALIZER = "org.apache.storm.redis.tools.encoder.DefaultStateSerializer";

    private final RedisCommandsInstanceContainer container;

    public Base64ToBinaryStateMigrationUtil(JedisPoolConfig poolConfig) {
        this(RedisCommandsContainerBuilder.build(poolConfig));
    }

    public Base64ToBinaryStateMigrationUtil(RedisCommandsInstanceContainer container) {
        this.container = container;
    }

    private void migrate(String namespace) {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();

            String prepareNamespace = namespace + "$prepare";
            migrateHashIfExists(commands, prepareNamespace);
            migrateHashIfExists(commands, namespace);
        } finally {
            container.returnInstance(commands);
        }
    }

    private void migrateCheckpointSpout(String namespace, OldRedisEncoder<String, CheckPointState> oldEncoder,
                                        NewRedisEncoder<String, NewCheckPointState> newEncoder) {
        RedisCommands commands = null;
        try {
            commands = container.getInstance();
            migrateCheckpointSpoutIfExists(commands, namespace, oldEncoder, newEncoder);
        } finally {
            container.returnInstance(commands);
        }
    }

    private void migrateCheckpointSpoutIfExists(RedisCommands commands, String key,
                                                OldRedisEncoder<String, CheckPointState> oldEncoder,
                                                NewRedisEncoder<String, NewCheckPointState> newEncoder) {
        if (commands.exists(key)) {
            LOG.info("Migrating '{}'...", key);

            LOG.info("Reading current checkpoint state '{}'...", key);
            Map<String, String> currentValueMap = commands.hgetAll(key);

            LOG.info("Converting checkpoint state...");
            Map<byte[], byte[]> convertedValueMap = new HashMap<>();
            for (Map.Entry<String, String> entry : currentValueMap.entrySet()) {
                String stateKey = entry.getKey();
                String stateValue = entry.getValue();

                CheckPointState oldState = oldEncoder.decodeValue(stateValue);
                NewCheckPointState newState = new NewCheckPointState(oldState.getTxid(),
                        NewCheckPointState.State.valueOf(oldState.getState().name()), 1L);

                convertedValueMap.put(newEncoder.encodeKey(stateKey), newEncoder.encodeValue(newState));
            }

            String backupKey = key + "_old";

            LOG.info("Backing up current state '{}' to '{}'...", key, backupKey);
            commands.rename(key, backupKey);

            LOG.info("Pushing converted checkpoint state to '{}'...", key);
            commands.hmset(SafeEncoder.encode(key), convertedValueMap);
        }
    }

    private void migrateHashIfExists(RedisCommands commands, String key) {
        if (commands.exists(key)) {
            LOG.info("Migrating '{}'...", key);

            LOG.info("Reading current state '{}'...", key);
            Map<String, String> currentValueMap = commands.hgetAll(key);

            LOG.info("Converting state...");
            Map<byte[], byte[]> convertedValueMap = convertBase64MapToBinaryMap(currentValueMap);

            String backupKey = key + "_old";

            LOG.info("Backing up current state '{}' to '{}'...", key, backupKey);
            commands.rename(key, backupKey);

            LOG.info("Pushing converted state to '{}'...", key);
            commands.hmset(SafeEncoder.encode(key), convertedValueMap);
        }
    }

    private Map<byte[], byte[]> convertBase64MapToBinaryMap(Map<String, String> base64Map) {
        Map<byte[], byte[]> binaryMap = new HashMap<>();
        for (Map.Entry<String, String> entry : base64Map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            byte[] binaryKey = Base64.decodeBase64(key);
            byte[] binaryValue = Base64.decodeBase64(value);

            binaryMap.put(binaryKey, binaryValue);
        }

        return binaryMap;
    }

    public static void main(String[] args) throws IOException, ParseException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption(OPTION_NAMESPACE_LONG)) {
            printUsageAndExit(options, OPTION_NAMESPACE_LONG + " is required");
        }

        String[] namespaces = commandLine.getOptionValues(OPTION_NAMESPACE_LONG);
        String host = commandLine.getOptionValue(OPTION_REDIS_HOST_LONG, "localhost");
        String portStr = commandLine.getOptionValue(OPTION_REDIS_PORT_LONG, "6379");
        String password = commandLine.getOptionValue(OPTION_REDIS_PASSWORD_LONG);
        String dbNumStr = commandLine.getOptionValue(OPTION_REDIS_DB_NUM_LONG, "0");
        String keySerializerClass = commandLine.getOptionValue(OPTION_KEY_SERIALIZER_LONG, DEFAULT_STATE_SERIALIZER);
        String valueSerializerClass = commandLine.getOptionValue(OPTION_VALUE_SERIALIZER_LONG, DEFAULT_STATE_SERIALIZER);

        Class<?> klass = (Class<?>) Class.forName(keySerializerClass);
        Serializer keySerializer = (Serializer) klass.newInstance();
        klass = (Class<?>) Class.forName(valueSerializerClass);
        Serializer valueSerializer = (Serializer) klass.newInstance();

        OldRedisEncoder<String, CheckPointState> oldEncoder = new OldRedisEncoder<>(keySerializer, valueSerializer);
        NewRedisEncoder<String, NewCheckPointState> newEncoder = new NewRedisEncoder<>(keySerializer, valueSerializer);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder()
                .setHost(host)
                .setPort(Integer.parseInt(portStr))
                .setPassword(password)
                .setDatabase(Integer.parseInt(dbNumStr))
                .setTimeout(2000)
                .build();

        Base64ToBinaryStateMigrationUtil migrationUtil = new Base64ToBinaryStateMigrationUtil(jedisPoolConfig);

        for (String namespace : namespaces) {
            if (namespace.startsWith("$checkpointspout-")) {
                migrationUtil.migrateCheckpointSpout(namespace, oldEncoder, newEncoder);
            } else {
                migrationUtil.migrate(namespace);
            }
        }

        LOG.info("Done...");
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(OPTION_NAMESPACE_SHORT, OPTION_NAMESPACE_LONG, true, "REQUIRED the list of namespace to migrate.");
        options.addOption(OPTION_REDIS_HOST_SHORT, OPTION_REDIS_HOST_LONG, true, "Redis hostname (default: localhost)");
        options.addOption(OPTION_REDIS_PORT_SHORT, OPTION_REDIS_PORT_LONG, true, "Redis port (default: 6379)");
        options.addOption(null, OPTION_REDIS_PASSWORD_LONG, true, "Redis password (default: no password)");
        options.addOption(OPTION_REDIS_DB_NUM_SHORT, OPTION_REDIS_DB_NUM_LONG, true, "Redis DB number (default: 0)");
        options.addOption(OPTION_KEY_SERIALIZER_SHORT, OPTION_KEY_SERIALIZER_LONG, true, "Key serializer class if you used custom");
        options.addOption(OPTION_VALUE_SERIALIZER_SHORT, OPTION_VALUE_SERIALIZER_LONG, true, "Value serializer class if you used custom");
        return options;
    }

    private static void printUsageAndExit(Options options, String message) {
        LOG.error(message);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Base64ToBinaryStateMigrationUtil ", options);
        System.exit(1);
    }

}
