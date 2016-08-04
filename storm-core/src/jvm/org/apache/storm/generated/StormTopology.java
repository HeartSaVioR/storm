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
/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class StormTopology implements org.apache.thrift.TBase<StormTopology, StormTopology._Fields>, java.io.Serializable, Cloneable, Comparable<StormTopology> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("StormTopology");

  private static final org.apache.thrift.protocol.TField SPOUTS_FIELD_DESC = new org.apache.thrift.protocol.TField("spouts", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField BOLTS_FIELD_DESC = new org.apache.thrift.protocol.TField("bolts", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField STATE_SPOUTS_FIELD_DESC = new org.apache.thrift.protocol.TField("state_spouts", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField WORKER_HOOKS_FIELD_DESC = new org.apache.thrift.protocol.TField("worker_hooks", org.apache.thrift.protocol.TType.LIST, (short)4);
  private static final org.apache.thrift.protocol.TField DEPENDENCIES_FIELD_DESC = new org.apache.thrift.protocol.TField("dependencies", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StormTopologyStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StormTopologyTupleSchemeFactory());
  }

  private Map<String,SpoutSpec> spouts; // required
  private Map<String,Bolt> bolts; // required
  private Map<String,StateSpoutSpec> state_spouts; // required
  private List<ByteBuffer> worker_hooks; // optional
  private List<String> dependencies; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SPOUTS((short)1, "spouts"),
    BOLTS((short)2, "bolts"),
    STATE_SPOUTS((short)3, "state_spouts"),
    WORKER_HOOKS((short)4, "worker_hooks"),
    DEPENDENCIES((short)5, "dependencies");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SPOUTS
          return SPOUTS;
        case 2: // BOLTS
          return BOLTS;
        case 3: // STATE_SPOUTS
          return STATE_SPOUTS;
        case 4: // WORKER_HOOKS
          return WORKER_HOOKS;
        case 5: // DEPENDENCIES
          return DEPENDENCIES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.WORKER_HOOKS,_Fields.DEPENDENCIES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SPOUTS, new org.apache.thrift.meta_data.FieldMetaData("spouts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SpoutSpec.class))));
    tmpMap.put(_Fields.BOLTS, new org.apache.thrift.meta_data.FieldMetaData("bolts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Bolt.class))));
    tmpMap.put(_Fields.STATE_SPOUTS, new org.apache.thrift.meta_data.FieldMetaData("state_spouts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, StateSpoutSpec.class))));
    tmpMap.put(_Fields.WORKER_HOOKS, new org.apache.thrift.meta_data.FieldMetaData("worker_hooks", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    tmpMap.put(_Fields.DEPENDENCIES, new org.apache.thrift.meta_data.FieldMetaData("dependencies", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(StormTopology.class, metaDataMap);
  }

  public StormTopology() {
  }

  public StormTopology(
    Map<String,SpoutSpec> spouts,
    Map<String,Bolt> bolts,
    Map<String,StateSpoutSpec> state_spouts)
  {
    this();
    this.spouts = spouts;
    this.bolts = bolts;
    this.state_spouts = state_spouts;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public StormTopology(StormTopology other) {
    if (other.is_set_spouts()) {
      Map<String,SpoutSpec> __this__spouts = new HashMap<String,SpoutSpec>(other.spouts.size());
      for (Map.Entry<String, SpoutSpec> other_element : other.spouts.entrySet()) {

        String other_element_key = other_element.getKey();
        SpoutSpec other_element_value = other_element.getValue();

        String __this__spouts_copy_key = other_element_key;

        SpoutSpec __this__spouts_copy_value = new SpoutSpec(other_element_value);

        __this__spouts.put(__this__spouts_copy_key, __this__spouts_copy_value);
      }
      this.spouts = __this__spouts;
    }
    if (other.is_set_bolts()) {
      Map<String,Bolt> __this__bolts = new HashMap<String,Bolt>(other.bolts.size());
      for (Map.Entry<String, Bolt> other_element : other.bolts.entrySet()) {

        String other_element_key = other_element.getKey();
        Bolt other_element_value = other_element.getValue();

        String __this__bolts_copy_key = other_element_key;

        Bolt __this__bolts_copy_value = new Bolt(other_element_value);

        __this__bolts.put(__this__bolts_copy_key, __this__bolts_copy_value);
      }
      this.bolts = __this__bolts;
    }
    if (other.is_set_state_spouts()) {
      Map<String,StateSpoutSpec> __this__state_spouts = new HashMap<String,StateSpoutSpec>(other.state_spouts.size());
      for (Map.Entry<String, StateSpoutSpec> other_element : other.state_spouts.entrySet()) {

        String other_element_key = other_element.getKey();
        StateSpoutSpec other_element_value = other_element.getValue();

        String __this__state_spouts_copy_key = other_element_key;

        StateSpoutSpec __this__state_spouts_copy_value = new StateSpoutSpec(other_element_value);

        __this__state_spouts.put(__this__state_spouts_copy_key, __this__state_spouts_copy_value);
      }
      this.state_spouts = __this__state_spouts;
    }
    if (other.is_set_worker_hooks()) {
      List<ByteBuffer> __this__worker_hooks = new ArrayList<ByteBuffer>(other.worker_hooks);
      this.worker_hooks = __this__worker_hooks;
    }
    if (other.is_set_dependencies()) {
      List<String> __this__dependencies = new ArrayList<String>(other.dependencies);
      this.dependencies = __this__dependencies;
    }
  }

  public StormTopology deepCopy() {
    return new StormTopology(this);
  }

  @Override
  public void clear() {
    this.spouts = null;
    this.bolts = null;
    this.state_spouts = null;
    this.worker_hooks = null;
    this.dependencies = null;
  }

  public int get_spouts_size() {
    return (this.spouts == null) ? 0 : this.spouts.size();
  }

  public void put_to_spouts(String key, SpoutSpec val) {
    if (this.spouts == null) {
      this.spouts = new HashMap<String,SpoutSpec>();
    }
    this.spouts.put(key, val);
  }

  public Map<String,SpoutSpec> get_spouts() {
    return this.spouts;
  }

  public void set_spouts(Map<String,SpoutSpec> spouts) {
    this.spouts = spouts;
  }

  public void unset_spouts() {
    this.spouts = null;
  }

  /** Returns true if field spouts is set (has been assigned a value) and false otherwise */
  public boolean is_set_spouts() {
    return this.spouts != null;
  }

  public void set_spouts_isSet(boolean value) {
    if (!value) {
      this.spouts = null;
    }
  }

  public int get_bolts_size() {
    return (this.bolts == null) ? 0 : this.bolts.size();
  }

  public void put_to_bolts(String key, Bolt val) {
    if (this.bolts == null) {
      this.bolts = new HashMap<String,Bolt>();
    }
    this.bolts.put(key, val);
  }

  public Map<String,Bolt> get_bolts() {
    return this.bolts;
  }

  public void set_bolts(Map<String,Bolt> bolts) {
    this.bolts = bolts;
  }

  public void unset_bolts() {
    this.bolts = null;
  }

  /** Returns true if field bolts is set (has been assigned a value) and false otherwise */
  public boolean is_set_bolts() {
    return this.bolts != null;
  }

  public void set_bolts_isSet(boolean value) {
    if (!value) {
      this.bolts = null;
    }
  }

  public int get_state_spouts_size() {
    return (this.state_spouts == null) ? 0 : this.state_spouts.size();
  }

  public void put_to_state_spouts(String key, StateSpoutSpec val) {
    if (this.state_spouts == null) {
      this.state_spouts = new HashMap<String,StateSpoutSpec>();
    }
    this.state_spouts.put(key, val);
  }

  public Map<String,StateSpoutSpec> get_state_spouts() {
    return this.state_spouts;
  }

  public void set_state_spouts(Map<String,StateSpoutSpec> state_spouts) {
    this.state_spouts = state_spouts;
  }

  public void unset_state_spouts() {
    this.state_spouts = null;
  }

  /** Returns true if field state_spouts is set (has been assigned a value) and false otherwise */
  public boolean is_set_state_spouts() {
    return this.state_spouts != null;
  }

  public void set_state_spouts_isSet(boolean value) {
    if (!value) {
      this.state_spouts = null;
    }
  }

  public int get_worker_hooks_size() {
    return (this.worker_hooks == null) ? 0 : this.worker_hooks.size();
  }

  public java.util.Iterator<ByteBuffer> get_worker_hooks_iterator() {
    return (this.worker_hooks == null) ? null : this.worker_hooks.iterator();
  }

  public void add_to_worker_hooks(ByteBuffer elem) {
    if (this.worker_hooks == null) {
      this.worker_hooks = new ArrayList<ByteBuffer>();
    }
    this.worker_hooks.add(elem);
  }

  public List<ByteBuffer> get_worker_hooks() {
    return this.worker_hooks;
  }

  public void set_worker_hooks(List<ByteBuffer> worker_hooks) {
    this.worker_hooks = worker_hooks;
  }

  public void unset_worker_hooks() {
    this.worker_hooks = null;
  }

  /** Returns true if field worker_hooks is set (has been assigned a value) and false otherwise */
  public boolean is_set_worker_hooks() {
    return this.worker_hooks != null;
  }

  public void set_worker_hooks_isSet(boolean value) {
    if (!value) {
      this.worker_hooks = null;
    }
  }

  public int get_dependencies_size() {
    return (this.dependencies == null) ? 0 : this.dependencies.size();
  }

  public java.util.Iterator<String> get_dependencies_iterator() {
    return (this.dependencies == null) ? null : this.dependencies.iterator();
  }

  public void add_to_dependencies(String elem) {
    if (this.dependencies == null) {
      this.dependencies = new ArrayList<String>();
    }
    this.dependencies.add(elem);
  }

  public List<String> get_dependencies() {
    return this.dependencies;
  }

  public void set_dependencies(List<String> dependencies) {
    this.dependencies = dependencies;
  }

  public void unset_dependencies() {
    this.dependencies = null;
  }

  /** Returns true if field dependencies is set (has been assigned a value) and false otherwise */
  public boolean is_set_dependencies() {
    return this.dependencies != null;
  }

  public void set_dependencies_isSet(boolean value) {
    if (!value) {
      this.dependencies = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SPOUTS:
      if (value == null) {
        unset_spouts();
      } else {
        set_spouts((Map<String,SpoutSpec>)value);
      }
      break;

    case BOLTS:
      if (value == null) {
        unset_bolts();
      } else {
        set_bolts((Map<String,Bolt>)value);
      }
      break;

    case STATE_SPOUTS:
      if (value == null) {
        unset_state_spouts();
      } else {
        set_state_spouts((Map<String,StateSpoutSpec>)value);
      }
      break;

    case WORKER_HOOKS:
      if (value == null) {
        unset_worker_hooks();
      } else {
        set_worker_hooks((List<ByteBuffer>)value);
      }
      break;

    case DEPENDENCIES:
      if (value == null) {
        unset_dependencies();
      } else {
        set_dependencies((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SPOUTS:
      return get_spouts();

    case BOLTS:
      return get_bolts();

    case STATE_SPOUTS:
      return get_state_spouts();

    case WORKER_HOOKS:
      return get_worker_hooks();

    case DEPENDENCIES:
      return get_dependencies();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SPOUTS:
      return is_set_spouts();
    case BOLTS:
      return is_set_bolts();
    case STATE_SPOUTS:
      return is_set_state_spouts();
    case WORKER_HOOKS:
      return is_set_worker_hooks();
    case DEPENDENCIES:
      return is_set_dependencies();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StormTopology)
      return this.equals((StormTopology)that);
    return false;
  }

  public boolean equals(StormTopology that) {
    if (that == null)
      return false;

    boolean this_present_spouts = true && this.is_set_spouts();
    boolean that_present_spouts = true && that.is_set_spouts();
    if (this_present_spouts || that_present_spouts) {
      if (!(this_present_spouts && that_present_spouts))
        return false;
      if (!this.spouts.equals(that.spouts))
        return false;
    }

    boolean this_present_bolts = true && this.is_set_bolts();
    boolean that_present_bolts = true && that.is_set_bolts();
    if (this_present_bolts || that_present_bolts) {
      if (!(this_present_bolts && that_present_bolts))
        return false;
      if (!this.bolts.equals(that.bolts))
        return false;
    }

    boolean this_present_state_spouts = true && this.is_set_state_spouts();
    boolean that_present_state_spouts = true && that.is_set_state_spouts();
    if (this_present_state_spouts || that_present_state_spouts) {
      if (!(this_present_state_spouts && that_present_state_spouts))
        return false;
      if (!this.state_spouts.equals(that.state_spouts))
        return false;
    }

    boolean this_present_worker_hooks = true && this.is_set_worker_hooks();
    boolean that_present_worker_hooks = true && that.is_set_worker_hooks();
    if (this_present_worker_hooks || that_present_worker_hooks) {
      if (!(this_present_worker_hooks && that_present_worker_hooks))
        return false;
      if (!this.worker_hooks.equals(that.worker_hooks))
        return false;
    }

    boolean this_present_dependencies = true && this.is_set_dependencies();
    boolean that_present_dependencies = true && that.is_set_dependencies();
    if (this_present_dependencies || that_present_dependencies) {
      if (!(this_present_dependencies && that_present_dependencies))
        return false;
      if (!this.dependencies.equals(that.dependencies))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_spouts = true && (is_set_spouts());
    list.add(present_spouts);
    if (present_spouts)
      list.add(spouts);

    boolean present_bolts = true && (is_set_bolts());
    list.add(present_bolts);
    if (present_bolts)
      list.add(bolts);

    boolean present_state_spouts = true && (is_set_state_spouts());
    list.add(present_state_spouts);
    if (present_state_spouts)
      list.add(state_spouts);

    boolean present_worker_hooks = true && (is_set_worker_hooks());
    list.add(present_worker_hooks);
    if (present_worker_hooks)
      list.add(worker_hooks);

    boolean present_dependencies = true && (is_set_dependencies());
    list.add(present_dependencies);
    if (present_dependencies)
      list.add(dependencies);

    return list.hashCode();
  }

  @Override
  public int compareTo(StormTopology other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_spouts()).compareTo(other.is_set_spouts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_spouts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.spouts, other.spouts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_bolts()).compareTo(other.is_set_bolts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_bolts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.bolts, other.bolts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_state_spouts()).compareTo(other.is_set_state_spouts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_state_spouts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.state_spouts, other.state_spouts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_worker_hooks()).compareTo(other.is_set_worker_hooks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_worker_hooks()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.worker_hooks, other.worker_hooks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_dependencies()).compareTo(other.is_set_dependencies());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_dependencies()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dependencies, other.dependencies);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StormTopology(");
    boolean first = true;

    sb.append("spouts:");
    if (this.spouts == null) {
      sb.append("null");
    } else {
      sb.append(this.spouts);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("bolts:");
    if (this.bolts == null) {
      sb.append("null");
    } else {
      sb.append(this.bolts);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("state_spouts:");
    if (this.state_spouts == null) {
      sb.append("null");
    } else {
      sb.append(this.state_spouts);
    }
    first = false;
    if (is_set_worker_hooks()) {
      if (!first) sb.append(", ");
      sb.append("worker_hooks:");
      if (this.worker_hooks == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.worker_hooks, sb);
      }
      first = false;
    }
    if (is_set_dependencies()) {
      if (!first) sb.append(", ");
      sb.append("dependencies:");
      if (this.dependencies == null) {
        sb.append("null");
      } else {
        sb.append(this.dependencies);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_spouts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'spouts' is unset! Struct:" + toString());
    }

    if (!is_set_bolts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'bolts' is unset! Struct:" + toString());
    }

    if (!is_set_state_spouts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'state_spouts' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class StormTopologyStandardSchemeFactory implements SchemeFactory {
    public StormTopologyStandardScheme getScheme() {
      return new StormTopologyStandardScheme();
    }
  }

  private static class StormTopologyStandardScheme extends StandardScheme<StormTopology> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, StormTopology struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SPOUTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map44 = iprot.readMapBegin();
                struct.spouts = new HashMap<String,SpoutSpec>(2*_map44.size);
                String _key45;
                SpoutSpec _val46;
                for (int _i47 = 0; _i47 < _map44.size; ++_i47)
                {
                  _key45 = iprot.readString();
                  _val46 = new SpoutSpec();
                  _val46.read(iprot);
                  struct.spouts.put(_key45, _val46);
                }
                iprot.readMapEnd();
              }
              struct.set_spouts_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BOLTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map48 = iprot.readMapBegin();
                struct.bolts = new HashMap<String,Bolt>(2*_map48.size);
                String _key49;
                Bolt _val50;
                for (int _i51 = 0; _i51 < _map48.size; ++_i51)
                {
                  _key49 = iprot.readString();
                  _val50 = new Bolt();
                  _val50.read(iprot);
                  struct.bolts.put(_key49, _val50);
                }
                iprot.readMapEnd();
              }
              struct.set_bolts_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // STATE_SPOUTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map52 = iprot.readMapBegin();
                struct.state_spouts = new HashMap<String,StateSpoutSpec>(2*_map52.size);
                String _key53;
                StateSpoutSpec _val54;
                for (int _i55 = 0; _i55 < _map52.size; ++_i55)
                {
                  _key53 = iprot.readString();
                  _val54 = new StateSpoutSpec();
                  _val54.read(iprot);
                  struct.state_spouts.put(_key53, _val54);
                }
                iprot.readMapEnd();
              }
              struct.set_state_spouts_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // WORKER_HOOKS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list56 = iprot.readListBegin();
                struct.worker_hooks = new ArrayList<ByteBuffer>(_list56.size);
                ByteBuffer _elem57;
                for (int _i58 = 0; _i58 < _list56.size; ++_i58)
                {
                  _elem57 = iprot.readBinary();
                  struct.worker_hooks.add(_elem57);
                }
                iprot.readListEnd();
              }
              struct.set_worker_hooks_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // DEPENDENCIES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list59 = iprot.readListBegin();
                struct.dependencies = new ArrayList<String>(_list59.size);
                String _elem60;
                for (int _i61 = 0; _i61 < _list59.size; ++_i61)
                {
                  _elem60 = iprot.readString();
                  struct.dependencies.add(_elem60);
                }
                iprot.readListEnd();
              }
              struct.set_dependencies_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, StormTopology struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.spouts != null) {
        oprot.writeFieldBegin(SPOUTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.spouts.size()));
          for (Map.Entry<String, SpoutSpec> _iter62 : struct.spouts.entrySet())
          {
            oprot.writeString(_iter62.getKey());
            _iter62.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.bolts != null) {
        oprot.writeFieldBegin(BOLTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.bolts.size()));
          for (Map.Entry<String, Bolt> _iter63 : struct.bolts.entrySet())
          {
            oprot.writeString(_iter63.getKey());
            _iter63.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.state_spouts != null) {
        oprot.writeFieldBegin(STATE_SPOUTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.state_spouts.size()));
          for (Map.Entry<String, StateSpoutSpec> _iter64 : struct.state_spouts.entrySet())
          {
            oprot.writeString(_iter64.getKey());
            _iter64.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.worker_hooks != null) {
        if (struct.is_set_worker_hooks()) {
          oprot.writeFieldBegin(WORKER_HOOKS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.worker_hooks.size()));
            for (ByteBuffer _iter65 : struct.worker_hooks)
            {
              oprot.writeBinary(_iter65);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.dependencies != null) {
        if (struct.is_set_dependencies()) {
          oprot.writeFieldBegin(DEPENDENCIES_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.dependencies.size()));
            for (String _iter66 : struct.dependencies)
            {
              oprot.writeString(_iter66);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StormTopologyTupleSchemeFactory implements SchemeFactory {
    public StormTopologyTupleScheme getScheme() {
      return new StormTopologyTupleScheme();
    }
  }

  private static class StormTopologyTupleScheme extends TupleScheme<StormTopology> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, StormTopology struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.spouts.size());
        for (Map.Entry<String, SpoutSpec> _iter67 : struct.spouts.entrySet())
        {
          oprot.writeString(_iter67.getKey());
          _iter67.getValue().write(oprot);
        }
      }
      {
        oprot.writeI32(struct.bolts.size());
        for (Map.Entry<String, Bolt> _iter68 : struct.bolts.entrySet())
        {
          oprot.writeString(_iter68.getKey());
          _iter68.getValue().write(oprot);
        }
      }
      {
        oprot.writeI32(struct.state_spouts.size());
        for (Map.Entry<String, StateSpoutSpec> _iter69 : struct.state_spouts.entrySet())
        {
          oprot.writeString(_iter69.getKey());
          _iter69.getValue().write(oprot);
        }
      }
      BitSet optionals = new BitSet();
      if (struct.is_set_worker_hooks()) {
        optionals.set(0);
      }
      if (struct.is_set_dependencies()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.is_set_worker_hooks()) {
        {
          oprot.writeI32(struct.worker_hooks.size());
          for (ByteBuffer _iter70 : struct.worker_hooks)
          {
            oprot.writeBinary(_iter70);
          }
        }
      }
      if (struct.is_set_dependencies()) {
        {
          oprot.writeI32(struct.dependencies.size());
          for (String _iter71 : struct.dependencies)
          {
            oprot.writeString(_iter71);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, StormTopology struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map72 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.spouts = new HashMap<String,SpoutSpec>(2*_map72.size);
        String _key73;
        SpoutSpec _val74;
        for (int _i75 = 0; _i75 < _map72.size; ++_i75)
        {
          _key73 = iprot.readString();
          _val74 = new SpoutSpec();
          _val74.read(iprot);
          struct.spouts.put(_key73, _val74);
        }
      }
      struct.set_spouts_isSet(true);
      {
        org.apache.thrift.protocol.TMap _map76 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.bolts = new HashMap<String,Bolt>(2*_map76.size);
        String _key77;
        Bolt _val78;
        for (int _i79 = 0; _i79 < _map76.size; ++_i79)
        {
          _key77 = iprot.readString();
          _val78 = new Bolt();
          _val78.read(iprot);
          struct.bolts.put(_key77, _val78);
        }
      }
      struct.set_bolts_isSet(true);
      {
        org.apache.thrift.protocol.TMap _map80 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.state_spouts = new HashMap<String,StateSpoutSpec>(2*_map80.size);
        String _key81;
        StateSpoutSpec _val82;
        for (int _i83 = 0; _i83 < _map80.size; ++_i83)
        {
          _key81 = iprot.readString();
          _val82 = new StateSpoutSpec();
          _val82.read(iprot);
          struct.state_spouts.put(_key81, _val82);
        }
      }
      struct.set_state_spouts_isSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list84 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.worker_hooks = new ArrayList<ByteBuffer>(_list84.size);
          ByteBuffer _elem85;
          for (int _i86 = 0; _i86 < _list84.size; ++_i86)
          {
            _elem85 = iprot.readBinary();
            struct.worker_hooks.add(_elem85);
          }
        }
        struct.set_worker_hooks_isSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list87 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.dependencies = new ArrayList<String>(_list87.size);
          String _elem88;
          for (int _i89 = 0; _i89 < _list87.size; ++_i89)
          {
            _elem88 = iprot.readString();
            struct.dependencies.add(_elem88);
          }
        }
        struct.set_dependencies_isSet(true);
      }
    }
  }

}

