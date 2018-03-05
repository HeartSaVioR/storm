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

package org.apache.storm.cluster;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ErrorInfo;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.PrivateWorkerKey;
import org.apache.storm.generated.ProfileRequest;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.generated.WorkerTokenServiceType;
import org.apache.storm.nimbus.NimbusInfo;

public interface IStormClusterState {
    List<String> assignments(Runnable callback);

    Assignment assignmentInfo(String stormId, Runnable callback);

    VersionedData<Assignment> assignmentInfoWithVersion(String stormId, Runnable callback);

    Integer assignmentVersion(String stormId, Runnable callback) throws Exception;

    List<String> blobstoreInfo(String blobKey);

    List<NimbusSummary> nimbuses();

    void addNimbusHost(String nimbusId, NimbusSummary nimbusSummary);

    List<String> activeStorms();

    /**
     * Get a storm base for a topology
     * @param stormId the id of the topology
     * @param callback something to call if the data changes (best effort)
     * @return the StormBase or null if it is not alive.
     */
    StormBase stormBase(String stormId, Runnable callback);

    ClusterWorkerHeartbeat getWorkerHeartbeat(String stormId, String node, Long port);

    List<ProfileRequest> getWorkerProfileRequests(String stormId, NodeInfo nodeInfo);

    List<ProfileRequest> getTopologyProfileRequests(String stormId);

    void setWorkerProfileRequest(String stormId, ProfileRequest profileRequest);

    void deleteTopologyProfileRequests(String stormId, ProfileRequest profileRequest);

    Map<ExecutorInfo, ExecutorBeat> executorBeats(String stormId, Map<List<Long>, NodeInfo> executorNodePort);

    List<String> supervisors(Runnable callback);

    SupervisorInfo supervisorInfo(String supervisorId); // returns nil if doesn't exist

    void setupHeatbeats(String stormId);

    void teardownHeartbeats(String stormId);

    void teardownTopologyErrors(String stormId);

    List<String> heartbeatStorms();

    List<String> errorTopologies();

    /** @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon. */
    @Deprecated
    List<String> backpressureTopologies();

    void setTopologyLogConfig(String stormId, LogConfig logConfig);

    LogConfig topologyLogConfig(String stormId, Runnable cb);

    void workerHeartbeat(String stormId, String node, Long port, ClusterWorkerHeartbeat info);

    void removeWorkerHeartbeat(String stormId, String node, Long port);

    void supervisorHeartbeat(String supervisorId, SupervisorInfo info);

    /** @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon. */
    @Deprecated
    boolean topologyBackpressure(String stormId, long timeoutMs, Runnable callback);

    /** @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon. */
    @Deprecated
    void setupBackpressure(String stormId);

    /** @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon. */
    @Deprecated
    void removeBackpressure(String stormId);

    /** @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon. */
    @Deprecated
    void removeWorkerBackpressure(String stormId, String node, Long port);

    void activateStorm(String stormId, StormBase stormBase);

    void updateStorm(String stormId, StormBase newElems);

    void removeStormBase(String stormId);

    void setAssignment(String stormId, Assignment info);

    void setupBlobstore(String key, NimbusInfo nimbusInfo, Integer versionInfo);

    List<String> activeKeys();

    List<String> blobstore(Runnable callback);

    void removeStorm(String stormId);

    void removeBlobstoreKey(String blobKey);

    void removeKeyVersion(String blobKey);

    void reportError(String stormId, String componentId, String node, Long port, Throwable error);

    List<ErrorInfo> errors(String stormId, String componentId);

    ErrorInfo lastError(String stormId, String componentId);

    void setCredentials(String stormId, Credentials creds, Map<String, Object> topoConf) throws NoSuchAlgorithmException;

    Credentials credentials(String stormId, Runnable callback);

    void disconnect();

    /**
     * Get a private key used to validate a token is correct.
     * This is expected to be called from a privileged daemon, and the ACLs should be set up to only
     * allow nimbus and these privileged daemons access to these private keys.
     * @param type the type of service the key is for.
     * @param topologyId the topology id the key is for.
     * @param keyVersion the version of the key this is for.
     * @return the private key or null if it could not be found.
     */
    PrivateWorkerKey getPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion);

    /**
     * Store a new version of a private key.
     * This is expected to only ever be called from nimbus.  All ACLs however need to be setup to allow
     * the given services access to the stored information.
     * @param type the type of service this key is for.
     * @param topologyId the topology this key is for
     * @param keyVersion the version of the key this is for.
     * @param key the key to store.
     */
    void addPrivateWorkerKey(WorkerTokenServiceType type, String topologyId, long keyVersion, PrivateWorkerKey key);

    /**
     * Get the next key version number that should be used for this topology id.
     * This is expected to only ever be called from nimbus, but it is acceptable if the ACLs are setup
     * so that it can work from a privileged daemon for the given service.
     * @param type the type of service this is for.
     * @param topologyId the topology id this is for.
     * @return the next version number.  It should be 0 for a new topology id/service combination.
     */
    long getNextPrivateWorkerKeyVersion(WorkerTokenServiceType type, String topologyId);

    /**
     * Remove all keys for the given topology that have expired. The number of keys should be small enough
     * that doing an exhaustive scan of them all is acceptable as there is no guarantee that expiration time
     * and version number are related.  This should be for all service types.
     * This is expected to only ever be called from nimbus and some ACLs may be setup so being called from other
     * daemons will cause it to fail.
     * @param topologyId the id of the topology to scan.
     */
    void removeExpiredPrivateWorkerKeys(String topologyId);

    /**
     * Remove all of the worker keys for a given topology.  Used to clean up after a topology finishes.
     * This is expected to only ever be called from nimbus and ideally should only ever work from nimbus.
     * @param topologyId the topology to clean up after.
     */
    void removeAllPrivateWorkerKeys(String topologyId);

    /**
     * Get a list of all topologyIds that currently have private worker keys stored, of any kind.
     * This is expected to only ever be called from nimbus.
     * @return the list of topology ids with any kind of private worker key stored.
     */
    Set<String> idsOfTopologiesWithPrivateWorkerKeys();

    /**
     * Set the coordinator information of state checkpoint. The value of id should be guaranteed to be unique and non-concurrent use.
     *
     * @param id The ID. The value should not be shared across different topologies.
     * @param information The checkpoint information for that ID. The reason for the value to be map is for flexibility.
     */
    void setStateCoordinatorInformation(String id, Map<String, Object> information);

    /**
     * Read the coordinator information of state checkpoint. The value of id should be guaranteed to be unique and non-concurrent use.
     *
     * @param id The ID. The value should not be shared across different topologies.
     * @return The checkpoint information for that ID.
     */
    Map<String, Object> readStateCoordinatorInformation(String id);

    /**
     * Get all of the supervisors with the ID as the key.
     */
    default Map<String, SupervisorInfo> allSupervisorInfo() {
        return allSupervisorInfo(null);
    }

    /**
     * @param callback be alerted if the list of supervisors change
     * @return All of the supervisors with the ID as the key
     */
    default Map<String, SupervisorInfo> allSupervisorInfo(Runnable callback) {
        Map<String, SupervisorInfo> ret = new HashMap<>();
        for (String id: supervisors(callback)) {
            ret.put(id, supervisorInfo(id));
        }
        return ret;
    }
    
    /**
     * Get a topology ID from the name of a topology
     * @param topologyName the name of the topology to look for
     * @return the id of the topology or null if it is not alive.
     */
    default Optional<String> getTopoId(final String topologyName) {
        String ret = null;
        for (String topoId: activeStorms()) {
            StormBase base = stormBase(topoId, null);
            if (base != null && topologyName.equals(base.get_name())) {
                ret = topoId;
                break;
            }
        }
        return Optional.ofNullable(ret);
    }
    
    default Map<String, Assignment> topologyAssignments() {
        Map<String, Assignment> ret = new HashMap<>();
        for (String topoId: assignments(null)) {
            ret.put(topoId, assignmentInfo(topoId, null));
        }
        return ret;
    }
    
    default Map<String, StormBase> topologyBases() {
        Map<String, StormBase> stormBases = new HashMap<>();
        for (String topologyId : activeStorms()) {
            StormBase base = stormBase(topologyId, null);
            if (base != null) { //rece condition with delete
                stormBases.put(topologyId, base);
            }
        }
        return stormBases;
    }
}
