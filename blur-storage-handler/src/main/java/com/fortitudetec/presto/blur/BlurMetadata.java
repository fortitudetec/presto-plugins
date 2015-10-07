/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fortitudetec.presto.blur;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.ColumnDefinition;
import org.apache.blur.thrift.generated.Schema;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.VarcharType;
import com.fortitudetec.presto.BaseColumnHandle;
import com.fortitudetec.presto.BaseReadOnlyConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class BlurMetadata extends BaseReadOnlyConnectorMetadata {

  private final Iface _client;
  private final String _connectorId;

  public BlurMetadata(String connectorId, Iface client) {
    _connectorId = connectorId;
    _client = client;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    try {
      return ImmutableList.copyOf(_client.tableList());
    } catch (TException e) {
      throw new PrestoException(StandardErrorCode.EXTERNAL, e.getMessage(), e);
    }
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return new BlurTableHandle(_connectorId, tableName);
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    if (schemaNameOrNull == null) {
      return ImmutableList.of();
    }
    try {
      Schema schema = _client.schema(schemaNameOrNull);
      Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
      Builder<SchemaTableName> builder = ImmutableList.builder();
      for (String fam : families.keySet()) {
        builder.add(new SchemaTableName(schemaNameOrNull, fam));
      }
      return builder.build();
    } catch (TException e) {
      throw new PrestoException(StandardErrorCode.EXTERNAL, e.getMessage(), e);
    }
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    BlurTableHandle blurTableHandle = (BlurTableHandle) tableHandle;
    try {
      Schema schema = _client.schema(blurTableHandle.getTableName().getSchemaName());
      Map<String, Map<String, ColumnDefinition>> families = schema.getFamilies();
      Map<String, ColumnDefinition> map = families.get(blurTableHandle.getTableName().getTableName());
      ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
      for (Entry<String, ColumnDefinition> e : map.entrySet()) {
        String columnName = e.getKey();
        ColumnDefinition cd = e.getValue();
        String subColumnName = cd.getSubColumnName();
        if (subColumnName == null) {
          BaseColumnHandle baseColumnHandle = new BaseColumnHandle(_connectorId, columnName, VarcharType.VARCHAR);
          builder.put(columnName, baseColumnHandle);
        }
      }
      return builder.build();
    } catch (TException e) {
      throw new PrestoException(StandardErrorCode.EXTERNAL, e.getMessage(), e);
    }
  }
}
