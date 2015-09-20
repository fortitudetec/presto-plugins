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
package com.fortitudetec.presto;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableList.Builder;

public abstract class BaseReadOnlyConnectorMetadata implements ConnectorMetadata {

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle columnHandle) {
    BaseColumnHandle baseColumnHandle = (BaseColumnHandle) columnHandle;
    return new ColumnMetadata(baseColumnHandle.getColumnName(), baseColumnHandle.getType(), false);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
    BaseTableHandle baseTableHandle = (BaseTableHandle) table;
    Builder<ColumnMetadata> builder = ImmutableList.builder();
    Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
    for (Entry<String, ColumnHandle> e : columnHandles.entrySet()) {
      ColumnMetadata columnMetadata = getColumnMetadata(session, table, e.getValue());
      builder.add(columnMetadata);
    }
    return new ConnectorTableMetadata(baseTableHandle.getTableName(), builder.build());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> mapBuilder = ImmutableMap.builder();
    List<String> listSchemaNames = listSchemaNames(session);
    for (String schema : listSchemaNames) {
      List<SchemaTableName> listTables = listTables(session, schema);
      for (SchemaTableName schemaTableName : listTables) {
        if (prefix.matches(schemaTableName)) {
          Builder<ColumnMetadata> builder = ImmutableList.builder();
          ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName);
          Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
          for (Entry<String, ColumnHandle> e : columnHandles.entrySet()) {
            builder.add(getColumnMetadata(session, tableHandle, e.getValue()));
          }
          mapBuilder.put(schemaTableName, builder.build());
        }
      }
    }
    return mapBuilder.build();
  }

  @Override
  public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
    return null;
  }

  @Override
  public boolean canCreateSampledTables(ConnectorSession session) {
    return false;
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support creates");
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support drops");
  }

  @Override
  public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support renames");
  }

  @Override
  public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support creates");
  }

  @Override
  public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle,
      Collection<Slice> fragments) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support creates");
  }

  @Override
  public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
  }

  @Override
  public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle,
      Collection<Slice> fragments) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support inserts");
  }

  @Override
  public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support views");
  }

  @Override
  public void dropView(ConnectorSession session, SchemaTableName viewName) {
    throw new PrestoException(NOT_SUPPORTED, "This connector does not support views");
  }

  @Override
  public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull) {
    return ImmutableList.of();
  }

  @Override
  public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
    return ImmutableMap.of();
  }

}
