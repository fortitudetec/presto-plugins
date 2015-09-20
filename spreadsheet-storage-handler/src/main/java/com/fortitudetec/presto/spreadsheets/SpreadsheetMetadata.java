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

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fortitudetec.presto.BaseReadOnlyConnectorMetadata;
import com.fortitudetec.presto.spreadsheets.util.NormalizeName;
import com.fortitudetec.presto.spreadsheets.util.SpreadsheetReader;
import com.fortitudetec.presto.spreadsheets.util.Table;
import com.fortitudetec.presto.spreadsheets.util.TableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class SpreadsheetMetadata extends BaseReadOnlyConnectorMetadata {

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
          String prestoSchemaName = NormalizeName.normalizeName(fileStatus.getPath().getName(), map.keySet());
          map.put(prestoSchemaName, fileStatus.getPath());
        }
        return ImmutableMap.copyOf(map);
      } catch (IOException e) {
        throw new PrestoException(INTERNAL_ERROR, "Unknown error", e);
      }
    }
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
