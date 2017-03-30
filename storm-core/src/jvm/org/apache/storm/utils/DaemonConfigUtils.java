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

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.validation.ConfigValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DaemonConfigUtils {
    private final static Logger LOG = LoggerFactory.getLogger(DaemonConfigUtils.class);
    public final static String RESOURCES_SUBDIR = "resources";
    public final static String NIMBUS_DO_NOT_REASSIGN = "NIMBUS-DO-NOT-REASSIGN";
    public static final String FILE_SEPARATOR = File.separator;

    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static DaemonConfigUtils _instance = new DaemonConfigUtils();

    /**
     * Provide an instance of this class for delegates to use.  To mock out
     * delegated methods, provide an instance of a subclass that overrides the
     * implementation of the delegated method.
     * @param u a DaemonConfigUtils instance
     * @return the previously set instance
     */
    public static DaemonConfigUtils setInstance(DaemonConfigUtils u) {
        DaemonConfigUtils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    public static String clojureConfigName(String name) {
        return name.toUpperCase().replace("_", "-");
    }

    // ALL-CONFIGS is only used by executor.clj once, do we want to do it here? TODO
    public static List<Object> All_CONFIGS() {
        List<Object> ret = new ArrayList<Object>();
        Config config = new Config();
        Class<?> ConfigClass = config.getClass();
        Field[] fields = ConfigClass.getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Object obj = fields[i].get(null);
                ret.add(obj);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return ret;
    }

    public static String clusterMode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        return mode;
    }

    public static Map<String, Object> readYamlConfig(String name, boolean mustExist) {
        Map<String, Object> conf = Utils.findAndReadConfigFile(name, mustExist);
        ConfigValidation.validateFields(conf);
        return conf;
    }

    public static Map readYamlConfig(String name) {
        return readYamlConfig(name, true);
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static Map<String, Object> readStormConfig() {
        return _instance.readStormConfigImpl();
    }

    public Map<String, Object> readStormConfigImpl() {
        Map<String, Object> conf = Utils.readStormConfig();
        ConfigValidation.validateFields(conf, DaemonConfig.class);
        return conf;
    }

    public static String absoluteHealthCheckDir(Map conf) {
        String stormHome = System.getProperty("storm.home");
        String healthCheckDir = (String) conf.get(DaemonConfig.STORM_HEALTH_CHECK_DIR);
        if (healthCheckDir == null) {
            return (stormHome + FILE_SEPARATOR + "healthchecks");
        } else {
            if (new File(healthCheckDir).isAbsolute()) {
                return healthCheckDir;
            } else {
                return (stormHome + FILE_SEPARATOR + healthCheckDir);
            }
        }
    }

    public static String masterLocalDir(Map conf) throws IOException {
        String ret = String.valueOf(conf.get(Config.STORM_LOCAL_DIR)) + FILE_SEPARATOR + "nimbus";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormJarKey(String topologyId) {
        return (topologyId + "-stormjar.jar");
    }

    public static String masterStormDistRoot(Map conf) throws IOException {
        String ret = ConfigUtils.stormDistPath(masterLocalDir(conf));
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterStormDistRoot(Map conf, String stormId) throws IOException {
        return (masterStormDistRoot(conf) + FILE_SEPARATOR + stormId);
    }

    public static String masterStormJarPath(String stormRoot) {
        return (stormRoot + FILE_SEPARATOR + "stormjar.jar");
    }

    public static String masterInbox(Map conf) throws IOException {
        String ret = masterLocalDir(conf) + FILE_SEPARATOR + "inbox";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    public static String masterInimbusDir(Map conf) throws IOException {
        return (masterLocalDir(conf) + FILE_SEPARATOR + "inimbus");
    }

    public static String supervisorIsupervisorDir(Map conf) throws IOException {
        return (ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "isupervisor");
    }

    public static String supervisorTmpDir(Map conf) throws IOException {
        String ret = ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "tmp";
        FileUtils.forceMkdir(new File(ret));
        return ret;
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState supervisorState(Map conf) throws IOException {
        return _instance.supervisorStateImpl(conf);
    }

    public LocalState supervisorStateImpl(Map conf) throws IOException {
        return new LocalState((ConfigUtils.supervisorLocalDir(conf) + FILE_SEPARATOR + "localstate"));
    }

    // we use this "weird" wrapper pattern temporarily for mocking in clojure test
    public static LocalState nimbusTopoHistoryState(Map conf) throws IOException {
        return _instance.nimbusTopoHistoryStateImpl(conf);
    }

    public LocalState nimbusTopoHistoryStateImpl(Map conf) throws IOException {
        return new LocalState((masterLocalDir(conf) + FILE_SEPARATOR + "history"));
    }

    public static String workerUserRoot(Map conf) {
        return (ConfigUtils.absoluteStormLocalDir(conf) + FILE_SEPARATOR + "workers-users");
    }

    public static String workerUserFile(Map conf, String workerId) {
        return (workerUserRoot(conf) + FILE_SEPARATOR + workerId);
    }

    public static File getLogMetaDataFile(String fname) {
        String[] subStrings = fname.split(FILE_SEPARATOR); // TODO: does this work well on windows?
        String id = subStrings[0];
        Integer port = Integer.parseInt(subStrings[1]);
        return getLogMetaDataFile(Utils.readStormConfig(), id, port);
    }

    public static File getLogMetaDataFile(Map conf, String id, Integer port) {
        String fname = ConfigUtils.workerArtifactsRoot(conf, id, port) + FILE_SEPARATOR + "worker.yaml";
        return new File(fname);
    }

    public static File getWorkerDirFromRoot(String logRoot, String id, Integer port) {
        return new File((logRoot + FILE_SEPARATOR + id + FILE_SEPARATOR + port));
    }

    public static String workerTmpRoot(Map conf, String id) {
        return (ConfigUtils.workerRoot(conf, id) + FILE_SEPARATOR + "tmp");
    }


    public static String workerPidPath(Map<String, Object> conf, String id, long pid) {
        return ConfigUtils.workerPidPath(conf, id, String.valueOf(pid));
    }

    /* TODO: make sure test these two functions in manual tests */
    public static List<String> getTopoLogsUsers(Map topologyConf) {
        List<String> logsUsers = (List<String>)topologyConf.get(DaemonConfig.LOGS_USERS);
        List<String> topologyUsers = (List<String>)topologyConf.get(Config.TOPOLOGY_USERS);
        Set<String> mergedUsers = new HashSet<String>();
        if (logsUsers != null) {
            for (String user : logsUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        if (topologyUsers != null) {
            for (String user : topologyUsers) {
                if (user != null) {
                    mergedUsers.add(user);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedUsers);
        Collections.sort(ret);
        return ret;
    }

    public static List<String> getTopoLogsGroups(Map topologyConf) {
        List<String> logsGroups = (List<String>)topologyConf.get(DaemonConfig.LOGS_GROUPS);
        List<String> topologyGroups = (List<String>)topologyConf.get(Config.TOPOLOGY_GROUPS);
        Set<String> mergedGroups = new HashSet<String>();
        if (logsGroups != null) {
            for (String group : logsGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        if (topologyGroups != null) {
            for (String group : topologyGroups) {
                if (group != null) {
                    mergedGroups.add(group);
                }
            }
        }
        List<String> ret = new ArrayList<String>(mergedGroups);
        Collections.sort(ret);
        return ret;
    }
}
