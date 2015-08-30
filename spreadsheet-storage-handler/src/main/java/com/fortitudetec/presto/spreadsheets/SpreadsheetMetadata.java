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
package com.fortitudetec.presto.spreadsheets;

import io.airlift.slice.Slice;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;

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
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fortitudetec.presto.spreadsheets.util.SpreadsheetReader;
import com.fortitudetec.presto.spreadsheets.util.Table;
import com.fortitudetec.presto.spreadsheets.util.TableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.spi.StandardErrorCode.*;

public class SpreadsheetMetadata implements ConnectorMetadata {

  private final Path _basePath;
  private final Configuration _configuration;
  private final String _spreadsheetSubDir;
  private final String _connectorId;

  public SpreadsheetMetadata(String connectorId, Configuration configuration, Path basePath, String spreadsheetSubDir) {
    _connectorId = connectorId;
    _basePath = basePath;
    _configuration = configuration;
    _spreadsheetSubDir = spreadsheetSubDir;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(session.getUser());
    Map<String, Path> schemaMap = ugi.doAs(new MapSchemaNames(session.getUser()));
    return ImmutableList.copyOf(schemaMap.keySet());
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    Path spreadsheetPath = getSpreadsheetBasePath(session.getUser());
    Path filePath = getSpreadsheetFilePath(session.getUser(), spreadsheetPath, tableName.getSchemaName());
    return new SpreadsheetTableHandle(_connectorId, session.getUser(), tableName, filePath.toString());
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
    SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) table;
    Builder<ColumnMetadata> builder = ImmutableList.builder();
    Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
    for (Entry<String, ColumnHandle> e : columnHandles.entrySet()) {
      ColumnMetadata columnMetadata = getColumnMetadata(session, table, e.getValue());
      builder.add(columnMetadata);
    }
    return new ConnectorTableMetadata(spreadsheetTableHandle.getTableName(), builder.build());
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    if (schemaNameOrNull == null) {
      return ImmutableList.of();
    }
    Path spreadsheetBasePath = getSpreadsheetBasePath(session.getUser());
    Path filePath = getSpreadsheetFilePath(session.getUser(), spreadsheetBasePath, schemaNameOrNull);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(session.getUser());
    SpreadsheetReader spreadSheetHelper = ugi.doAs(new GetSpreadsheetHelper(filePath, _configuration));
    List<String> tableNames = spreadSheetHelper.getTableNames();
    Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String table : tableNames) {
      builder.add(new SchemaTableName(schemaNameOrNull, table));
    }
    return builder.build();
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) tableHandle;
    SpreadsheetReader spreadSheetHelper = getSpreadSheetHelper(session, spreadsheetTableHandle, _configuration);
    SchemaTableName schemaTableName = spreadsheetTableHandle.getTableName();
    String tableName = schemaTableName.getTableName();
    Table table = spreadSheetHelper.getTable(tableName);
    List<String> columnNames = table.getColumnNames();
    ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
    for (String columnName : columnNames) {
      TableType columnType = table.getType(columnName);
      Type type = getType(columnType);
      builder.put(columnName, new SpreadsheetColumnHandle(_connectorId, columnName, type));
    }
    return builder.build();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle columnHandle) {
    SpreadsheetColumnHandle spreadsheetColumnHandle = (SpreadsheetColumnHandle) columnHandle;
    return new ColumnMetadata(spreadsheetColumnHandle.getColumnName(), spreadsheetColumnHandle.getType(), false);
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

  // //////////

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

  class ListTables implements PrivilegedAction<List<SchemaTableName>> {

    private final Path _file;
    private final String _schemaName;

    public ListTables(String schemaName, Path file) {
      _file = file;
      _schemaName = schemaName;
    }

    @Override
    public List<SchemaTableName> run() {
      try {
        FileSystem fileSystem = _file.getFileSystem(_configuration);
        FSDataInputStream inputStream = fileSystem.open(_file);
        FileStatus fileStatus = fileSystem.getFileStatus(_file);
        SpreadsheetReader spreadSheetHelper = new SpreadsheetReader(inputStream, fileStatus.getLen());
        inputStream.close();
        List<String> tableNames = spreadSheetHelper.getTableNames();
        Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : tableNames) {
          builder.add(new SchemaTableName(_schemaName, table));
        }
        return builder.build();
      } catch (IOException e) {
        throw new PrestoException(INTERNAL_ERROR, "Unknown error", e);
      }
    }

  }

  static class GetSpreadsheetHelper implements PrivilegedAction<SpreadsheetReader> {

    private final Path _file;
    private final Configuration _configuration;

    public GetSpreadsheetHelper(Path file, Configuration configuration) {
      _file = file;
      _configuration = configuration;
    }

    @Override
    public SpreadsheetReader run() {
      try {
        FileSystem fileSystem = _file.getFileSystem(_configuration);
        FileStatus fileStatus = fileSystem.getFileStatus(_file);
        FSDataInputStream inputStream = fileSystem.open(_file);
        SpreadsheetReader spreadSheetHelper = new SpreadsheetReader(inputStream, fileStatus.getLen());
        inputStream.close();
        return spreadSheetHelper;
      } catch (IOException e) {
        throw new PrestoException(INTERNAL_ERROR, "Unknown error", e);
      }
    }
  }

  class MapSchemaNames implements PrivilegedAction<Map<String, Path>> {
    private final String _user;

    public MapSchemaNames(String user) {
      _user = user;
    }

    @Override
    public Map<String, Path> run() {
      try {
        FileSystem fileSystem = _basePath.getFileSystem(_configuration);
        Path path = getSpreadsheetBasePath(_user);
        FileStatus[] listStatus = fileSystem.listStatus(path, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            return path.getName().endsWith(".xlsx");
          }
        });
        Map<String, Path> map = new HashMap<>();
        for (FileStatus fileStatus : listStatus) {
          String prestoSchemaName = toPrestoSchemaName(fileStatus.getPath().getName());
          INNER: while (true) {
            if (map.containsKey(prestoSchemaName)) {
              prestoSchemaName = prestoSchemaName + "_dup";
            } else {
              break INNER;
            }
          }
          map.put(prestoSchemaName, fileStatus.getPath());
        }
        return ImmutableMap.copyOf(map);
      } catch (IOException e) {
        throw new PrestoException(INTERNAL_ERROR, "Unknown error", e);
      }
    }
  }

  public static String toPrestoSchemaName(String name) {
    int index = name.lastIndexOf('.');
    return name.substring(0, index).replace(' ', '_').replace('.', '_').toLowerCase();
  }

  private Path getSpreadsheetFilePath(String user, Path spreadsheetPath, String schema) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    Map<String, Path> schemaMap = ugi.doAs(new MapSchemaNames(user));
    Path path = schemaMap.get(schema);
    if (path == null) {
      throw new PrestoException(INTERNAL_ERROR, "File [" + schema + "] not found.");
    }
    return path;
  }

  private Path getSpreadsheetBasePath(String user) {
    return new Path(new Path(_basePath, user), _spreadsheetSubDir);
  }

  private Type getType(TableType columnType) {
    switch (columnType) {
    case BOOLEAN:
      return BooleanType.BOOLEAN;
    case NUMBER:
      return DoubleType.DOUBLE;
    case STRING:
      return new VarcharType();
    default:
      throw new PrestoException(INTERNAL_ERROR, "Not Supported [" + columnType + "]");
    }
  }

  public static SpreadsheetReader getSpreadSheetHelper(ConnectorSession session,
      SpreadsheetTableHandle spreadsheetTableHandle, Configuration configuration) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(session.getUser());
    return ugi.doAs(new GetSpreadsheetHelper(new Path(spreadsheetTableHandle.getSpreadsheetPath()), configuration));
  }
}
