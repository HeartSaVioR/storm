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

package org.apache.storm.redis.tools.state;

import org.apache.storm.spout.CheckpointSpout;

import static org.apache.storm.redis.tools.state.NewCheckPointState.State.COMMITTED;
import static org.apache.storm.redis.tools.state.NewCheckPointState.State.COMMITTING;
import static org.apache.storm.redis.tools.state.NewCheckPointState.State.PREPARING;

/**
 * Captures the current state of the transaction in {@link CheckpointSpout}. The state transitions are as follows.
 * <pre>
 *                  ROLLBACK(tx2)
 *               <-------------                  PREPARE(tx2)                     COMMIT(tx2)
 * COMMITTED(tx1)-------------> PREPARING(tx2) --------------> COMMITTING(tx2) -----------------> COMMITTED (tx2)
 *
 * </pre>
 *
 * During recovery, if a previous transaction is in PREPARING state, it is rolled back since all bolts in the topology
 * might not have prepared (saved) the data for commit. If the previous transaction is in COMMITTING state, it is
 * rolled forward (committed) since some bolts might have already committed the data.
 * <p>
 * During normal flow, the state transitions from PREPARING to COMMITTING to COMMITTED. In case of failures the
 * prepare/commit operation is retried.
 * </p>
 */
public class NewCheckPointState {
    private long txid;
    private State state;
    private long stateVersion;

    public enum State {
        /**
         * The checkpoint spout has committed the transaction.
         */
        COMMITTED,
        /**
         * The checkpoint spout has started committing the transaction
         * and the commit is in progress.
         */
        COMMITTING,
        /**
         * The checkpoint spout has started preparing the transaction for commit
         * and the prepare is in progress.
         */
        PREPARING
    }

    public enum Action {
        /**
         * prepare transaction for commit
         */
        PREPARE,
        /**
         * commit the previously prepared transaction
         */
        COMMIT,
        /**
         * rollback the previously prepared transaction
         */
        ROLLBACK,
        /**
         * initialize the state
         */
        INITSTATE
    }

    public NewCheckPointState(long txid, State state) {
        this(txid, state, 0L);
    }

    public NewCheckPointState(long txid, State state, long stateVersion) {
        this.txid = txid;
        this.state = state;
        this.stateVersion = stateVersion;
    }

    // for kryo
    public NewCheckPointState() {
    }

    public long getTxid() {
        return txid;
    }

    public State getState() {
        return state;
    }

    public long getStateVersion() {
        return stateVersion;
    }

    /**
     * Get the next state based on this checkpoint state.
     *
     * @param recovering if in recovering phase
     * @return the next checkpoint state based on this state.
     */
    public NewCheckPointState nextState(boolean recovering) {
        NewCheckPointState nextState;
        switch (state) {
            case PREPARING:
                nextState = recovering ? new NewCheckPointState(txid - 1, COMMITTED, stateVersion) :
                        new NewCheckPointState(txid, COMMITTING, stateVersion);
                break;
            case COMMITTING:
                nextState = new NewCheckPointState(txid, COMMITTED, stateVersion);
                break;
            case COMMITTED:
                nextState = recovering ? this : new NewCheckPointState(txid + 1, PREPARING, stateVersion);
                break;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
        return nextState;
    }

    /**
     * Get the next action to perform based on this checkpoint state.
     *
     * @param recovering if in recovering phase
     * @return the next action to perform based on this state
     */
    public Action nextAction(boolean recovering) {
        Action action;
        switch (state) {
            case PREPARING:
                action = recovering ? Action.ROLLBACK : Action.PREPARE;
                break;
            case COMMITTING:
                action = Action.COMMIT;
                break;
            case COMMITTED:
                action = recovering ? Action.INITSTATE : Action.PREPARE;
                break;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
        return action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewCheckPointState)) return false;

        NewCheckPointState that = (NewCheckPointState) o;

        if (getTxid() != that.getTxid()) return false;
        if (getStateVersion() != that.getStateVersion()) return false;
        return getState() == that.getState();
    }

    @Override
    public int hashCode() {
        int result = (int) (getTxid() ^ (getTxid() >>> 32));
        result = 31 * result + (getState() != null ? getState().hashCode() : 0);
        result = 31 * result + (int) (getStateVersion() ^ (getStateVersion() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "CheckPointState{" +
                "txid=" + txid +
                ", state=" + state +
                ", stateVersion=" + stateVersion +
                '}';
    }
}
