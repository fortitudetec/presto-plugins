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
package com.fortitudetec.pronto;

import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;

public class ProntoMetaData implements ConnectorMetadata {

  public ProntoMetaData(String connectorId, Map<String, String> config, Configuration configuration) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public boolean canCreateSampledTables(ConnectorSession session) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle columnHandle) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void commitCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle,
      Collection<Slice> fragments) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void commitInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle,
      Collection<Slice> fragments) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void dropView(ConnectorSession session, SchemaTableName viewName) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
    throw new RuntimeException("Not implemented.");
  }

}
