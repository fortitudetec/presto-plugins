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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public abstract class BaseReadOnlyConnectorMetadata implements ConnectorMetadata {

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return new BaseTableHandle(tableName);
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
      Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
    BaseTableHandle tableHandle = (BaseTableHandle) table;
    BaseTableLayoutHandle baseTableLayoutHandle = createTableLayoutHandle(tableHandle);
    ConnectorTableLayout layout = new ConnectorTableLayout(baseTableLayoutHandle);
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  protected BaseTableLayoutHandle createTableLayoutHandle(BaseTableHandle tableHandle) {
    BaseTableLayoutHandle baseTableLayoutHandle = new BaseTableLayoutHandle(tableHandle);
    return baseTableLayoutHandle;
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
    BaseTableLayoutHandle layout = (BaseTableLayoutHandle) handle;
    List<ConnectorTableLayoutResult> tableLayouts = getTableLayouts(session, layout.getTable(),
        Constraint.<ColumnHandle> alwaysTrue(), Optional.empty());
    ConnectorTableLayoutResult connectorTableLayoutResult = tableLayouts.get(0);
    return connectorTableLayoutResult.getTableLayout();
  }

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

}
