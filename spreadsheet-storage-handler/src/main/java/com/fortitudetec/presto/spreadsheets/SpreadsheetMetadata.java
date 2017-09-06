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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
import com.fortitudetec.presto.BaseColumnHandle;
import com.fortitudetec.presto.BaseReadOnlyConnectorMetadata;
import com.fortitudetec.presto.BaseTableHandle;
import com.fortitudetec.presto.BaseTableLayoutHandle;
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
  private final boolean _useFileCache;
  private final UserGroupInformation _ugi;

  public SpreadsheetMetadata(UserGroupInformation ugi, Configuration configuration, Path basePath,
      String spreadsheetSubDir, boolean useFileCache) throws IOException {
    _basePath = basePath;
    _configuration = configuration;
    _spreadsheetSubDir = spreadsheetSubDir;
    _useFileCache = useFileCache;
    _ugi = ugi;
  }

  @Override
  protected BaseTableLayoutHandle createTableLayoutHandle(BaseTableHandle tableHandle) {
    return new SpreadsheetTableLayoutHandle((SpreadsheetTableHandle) tableHandle);
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    Map<String, Path> schemaMap = getUgi(session).doAs(new MapSchemaNames(session.getUser()));
    return ImmutableList.copyOf(schemaMap.keySet());
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    Path spreadsheetPath = getSpreadsheetBasePath(session.getUser());
    Path filePath = getSpreadsheetFilePath(session, spreadsheetPath, tableName.getSchemaName());
    return new SpreadsheetTableHandle(session.getUser(), tableName, filePath.toString());
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    if (schemaNameOrNull == null) {
      return ImmutableList.of();
    }
    Path spreadsheetBasePath = getSpreadsheetBasePath(session.getUser());
    Path filePath = getSpreadsheetFilePath(session, spreadsheetBasePath, schemaNameOrNull);
    SpreadsheetReader spreadSheetHelper = getUgi(session).doAs(
        new GetSpreadsheetHelper(_useFileCache, filePath, _configuration));
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
    SpreadsheetReader spreadSheetHelper = getSpreadSheetHelper(getUgi(session), session, spreadsheetTableHandle,
        _configuration, _useFileCache);
    SchemaTableName schemaTableName = spreadsheetTableHandle.getTableName();
    String tableName = schemaTableName.getTableName();
    Table table = spreadSheetHelper.getTable(tableName);
    List<String> columnNames = table.getColumnNames();
    ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
    for (String columnName : columnNames) {
      TableType columnType = table.getType(columnName);
      Type type = getType(columnType);
      builder.put(columnName, new BaseColumnHandle(columnName, type));
    }
    return builder.build();
  }

  class ListTables implements PrivilegedAction<List<SchemaTableName>> {

    private final Path _file;
    private final String _schemaName;
    private final boolean _useFileCache;

    public ListTables(String schemaName, Path file, boolean useFileCache) {
      _file = file;
      _schemaName = schemaName;
      _useFileCache = useFileCache;
    }

    @Override
    public List<SchemaTableName> run() {
      try {
        SpreadsheetReader spreadSheetHelper = new SpreadsheetReader(_useFileCache, _configuration, _file);
        List<String> tableNames = spreadSheetHelper.getTableNames();
        Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : tableNames) {
          builder.add(new SchemaTableName(_schemaName, table));
        }
        return builder.build();
      } catch (IOException e) {
        throw new PrestoException(SpreadsheetErrorCode.INTERNAL_ERROR, "Unknown error", e);
      }
    }

  }

  static class GetSpreadsheetHelper implements PrivilegedAction<SpreadsheetReader> {

    private final Path _file;
    private final Configuration _configuration;
    private final boolean _useFileCache;

    public GetSpreadsheetHelper(boolean useFileCache, Path file, Configuration configuration) {
      _useFileCache = useFileCache;
      _file = file;
      _configuration = configuration;
    }

    @Override
    public SpreadsheetReader run() {
      try {
        return new SpreadsheetReader(_useFileCache, _configuration, _file);
      } catch (IOException e) {
        throw new PrestoException(SpreadsheetErrorCode.INTERNAL_ERROR, "Unknown error", e);
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
            String name = path.getName();
            if (name.startsWith(".")) {
              return false;
            }
            return name.endsWith(".xlsx");
          }
        });
        Map<String, Path> map = new HashMap<>();
        for (FileStatus fileStatus : listStatus) {
          String prestoSchemaName = NormalizeName.normalizeName(fileStatus.getPath()
                                                                          .getName(),
              map.keySet());
          map.put(prestoSchemaName, fileStatus.getPath());
        }
        return ImmutableMap.copyOf(map);
      } catch (IOException e) {
        throw new PrestoException(SpreadsheetErrorCode.INTERNAL_ERROR, "Unknown error", e);
      }
    }
  }

  private Path getSpreadsheetFilePath(ConnectorSession session, Path spreadsheetPath, String schema) {
    Map<String, Path> schemaMap = getUgi(session).doAs(new MapSchemaNames(session.getUser()));
    Path path = schemaMap.get(schema);
    if (path == null) {
      throw new PrestoException(SpreadsheetErrorCode.INTERNAL_ERROR, "File [" + schema + "] not found.");
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
      return VarcharType.VARCHAR;
    default:
      throw new PrestoException(SpreadsheetErrorCode.INTERNAL_ERROR, "Not Supported [" + columnType + "]");
    }
  }

  public static SpreadsheetReader getSpreadSheetHelper(UserGroupInformation ugi, ConnectorSession session,
      SpreadsheetTableHandle spreadsheetTableHandle, Configuration configuration, boolean useFileCache) {
    return ugi.doAs(
        new GetSpreadsheetHelper(useFileCache, new Path(spreadsheetTableHandle.getSpreadsheetPath()), configuration));
  }

  private UserGroupInformation getUgi(ConnectorSession session) {
    return getProxyUserGroupInformation(session, _ugi);
  }

  public static UserGroupInformation getProxyUserGroupInformation(ConnectorSession session, UserGroupInformation ugi) {
    return UserGroupInformation.createProxyUser(session.getUser(), ugi);
  }
}
