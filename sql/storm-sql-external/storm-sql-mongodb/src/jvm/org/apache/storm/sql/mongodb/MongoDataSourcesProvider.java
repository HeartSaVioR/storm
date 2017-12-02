/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.mongodb;

import com.google.common.base.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.sql.runtime.DataSource;
import org.apache.storm.sql.runtime.DataSourcesProvider;
import org.apache.storm.sql.runtime.FieldInfo;
import org.apache.storm.sql.runtime.IOutputSerializer;
import org.apache.storm.sql.runtime.ISqlStreamsDataSource;
import org.apache.storm.sql.runtime.utils.FieldInfoUtils;
import org.apache.storm.sql.runtime.utils.SerdeUtils;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

/**
 * Create a MongoDB sink based on the URI and properties. The URI has the format of
 * mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]].
 * The properties are in JSON format which specifies the name of the MongoDB collection and etc.
 */
public class MongoDataSourcesProvider implements DataSourcesProvider {

    private static class MongoStreamsDataSource implements ISqlStreamsDataSource {
        private final String url;
        private final Properties props;
        private final IOutputSerializer serializer;

        private MongoStreamsDataSource(String url, Properties props, IOutputSerializer serializer) {
            this.url = url;
            this.props = props;
            this.serializer = serializer;
        }

        @Override
        public IRichSpout getProducer() {
            throw new UnsupportedOperationException(this.getClass().getName() + " doesn't provide Producer");
        }

        @Override
        public IRichBolt getConsumer() {
            Preconditions.checkArgument(!props.isEmpty(), "Writable MongoDB must contain collection config");

            String serField;
            if (props.contains("ser.field")) {
                serField = props.getProperty("ser.field");
            } else if (props.contains("trident.ser.field")) {
                // backward compatibility
                serField = props.getProperty("trident.ser.field");
            } else {
                serField = "tridentSerField";
            }

            MongoMapper mapper = new SqlMongoMapper(serField, serializer);

            return new MongoInsertBolt(url, props.getProperty("collection.name"), mapper);
        }
    }

    private static class SqlMongoMapper implements MongoMapper {
        private final String serField;
        private final IOutputSerializer serializer;

        private SqlMongoMapper(String serField, IOutputSerializer serializer) {
            this.serField = serField;
            this.serializer = serializer;
        }

        @Override
        public Document toDocument(ITuple tuple) {
            Document document = new Document();
            Values values = (Values) tuple.getValue(1);
            byte[] array = serializer.write(values, null).array();
            document.append(serField, array);
            return document;
        }

        @Override
        public Document toDocumentByKeys(List<Object> keys) {
            return null;
        }
    }

    @Override
    public String scheme() {
        return "mongodb";
    }

    @Override
    public DataSource construct(URI uri, String inputFormatClass, String outputFormatClass,
                                List<FieldInfo> fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ISqlStreamsDataSource constructStreams(URI uri, String inputFormatClass, String outputFormatClass,
                                                  Properties properties, List<FieldInfo> fields) {
        List<String> fieldNames = FieldInfoUtils.getFieldNames(fields);
        IOutputSerializer serializer = SerdeUtils.getSerializer(outputFormatClass, properties, fieldNames);
        return new MongoStreamsDataSource(uri.toString(), properties, serializer);
    }

}
