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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.security.UserGroupInformation;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fortitudetec.presto.spreadsheets.util.NormalizeName;
import com.fortitudetec.presto.spreadsheets.util.SpreadsheetReader;
import com.fortitudetec.presto.spreadsheets.util.Table;
import com.fortitudetec.presto.spreadsheets.util.TableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class SpreadsheetMetadata implements ConnectorMetadata {

  private final Path _basePath;
  private final Configuration _configuration;
  private final String _spreadsheetSubDir;
  private final boolean _useFileCache;
  private final UserGroupInformation _ugi;
  private final boolean _proxyUser;

  private static final String DEFAULT_COMMENT = "";

  public SpreadsheetMetadata(UserGroupInformation ugi, Configuration configuration, Path basePath,
      String spreadsheetSubDir, boolean useFileCache, boolean proxyUser) throws IOException {
    _proxyUser = proxyUser;
    _basePath = basePath;
    _configuration = configuration;
    _spreadsheetSubDir = spreadsheetSubDir;
    _useFileCache = useFileCache;
    _ugi = ugi;
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
      Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
    SpreadsheetTableHandle tableHandle = (SpreadsheetTableHandle) table;
    SpreadsheetTableLayoutHandle baseTableLayoutHandle = createTableLayoutHandle(tableHandle);
    ConnectorTableLayout layout = new ConnectorTableLayout(baseTableLayoutHandle);
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
    SpreadsheetTableLayoutHandle layout = (SpreadsheetTableLayoutHandle) handle;
    List<ConnectorTableLayoutResult> tableLayouts = getTableLayouts(session, layout.getTable(),
        Constraint.<ColumnHandle>alwaysTrue(), Optional.empty());
    ConnectorTableLayoutResult connectorTableLayoutResult = tableLayouts.get(0);
    return connectorTableLayoutResult.getTableLayout();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
      ColumnHandle columnHandle) {
    SpreadsheetColumnHandle baseColumnHandle = (SpreadsheetColumnHandle) columnHandle;
    return new ColumnMetadata(baseColumnHandle.getColumnName(), baseColumnHandle.getType(), DEFAULT_COMMENT, false);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
    SpreadsheetTableHandle baseTableHandle = (SpreadsheetTableHandle) table;
    Builder<ColumnMetadata> builder = ImmutableList.builder();
    Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
    for (Entry<String, ColumnHandle> e : columnHandles.entrySet()) {
      ColumnMetadata columnMetadata = getColumnMetadata(session, table, e.getValue());
      builder.add(columnMetadata);
    }
    return new ConnectorTableMetadata(baseTableHandle.getTableName(), builder.build());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
      SchemaTablePrefix prefix) {
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

  private SpreadsheetTableLayoutHandle createTableLayoutHandle(SpreadsheetTableHandle tableHandle) {
    return new SpreadsheetTableLayoutHandle(tableHandle);
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
      Map<String, Path> schemaMap = getUgi(session).doAs(new MapSchemaNames(session.getUser()));
      Set<String> schemaSet = new TreeSet<>(schemaMap.keySet());
      Builder<SchemaTableName> builder = ImmutableList.builder();
      for (String schemaName : schemaSet) {
        builder.addAll(listTables(session, schemaName));
      }
      return builder.build();
    } else {
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
      builder.put(columnName, new SpreadsheetColumnHandle(columnName, type));
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
        FileStatus[] listStatus = fileSystem.listStatus(path, (PathFilter) p -> {
          String name = p.getName();
          if (name.startsWith(".")) {
            return false;
          }
          return name.endsWith(".xlsx");
        });
        Arrays.asList(listStatus);
        Map<String, Path> map = new HashMap<>();
        for (FileStatus fileStatus : listStatus) {
          String name = fileStatus.getPath()
                                  .getName();
          map.put(NormalizeName.normalizeName(name, map.keySet()), fileStatus.getPath());
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
    return getUgi(session, _proxyUser, _ugi);
  }

  public static UserGroupInformation getUgi(ConnectorSession session, boolean proxyUser, UserGroupInformation ugi) {
    if (proxyUser) {
      return getProxyUserGroupInformation(session, ugi);
    }
    return ugi;
  }

  public static UserGroupInformation getProxyUserGroupInformation(ConnectorSession session, UserGroupInformation ugi) {
    return UserGroupInformation.createProxyUser(session.getUser(), ugi);
  }
}
