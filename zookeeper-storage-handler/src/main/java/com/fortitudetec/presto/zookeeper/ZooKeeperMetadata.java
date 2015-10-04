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
package com.fortitudetec.presto.zookeeper;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.AVERSION;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CTIME;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CVERSION;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATA;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATALENGTH;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATA_AS_STRING;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.EPHEMERALOWNER;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.MTIME;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.MZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.NUMCHILDREN;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.PATH;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.PZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.VERSION;

import java.util.List;
import java.util.Map;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.fortitudetec.presto.BaseColumnHandle;
import com.fortitudetec.presto.BaseReadOnlyConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class ZooKeeperMetadata extends BaseReadOnlyConnectorMetadata {

  private final String _connectorId;

  public ZooKeeperMetadata(String connectorId) {
    _connectorId = connectorId;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return ImmutableList.of("zk");
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    return new ZooKeeperTableHandle(_connectorId, tableName);
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    if (schemaNameOrNull == null) {
      return ImmutableList.of();
    } else if (schemaNameOrNull.equals("zk")) {
      return ImmutableList.of(new SchemaTableName(schemaNameOrNull, "zk"));
    } else {
      throw new PrestoException(NOT_FOUND, "Schema " + schemaNameOrNull + " not found");
    }
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    Builder<String, ColumnHandle> builder = ImmutableMap.builder();
    add(builder, new BaseColumnHandle(_connectorId, PATH, VarcharType.VARCHAR));
    add(builder, new BaseColumnHandle(_connectorId, DATA, VarbinaryType.VARBINARY));
    add(builder, new BaseColumnHandle(_connectorId, DATA_AS_STRING, VarcharType.VARCHAR));
    add(builder, new BaseColumnHandle(_connectorId, AVERSION, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, CTIME, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, CVERSION, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, CZXID, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, DATALENGTH, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, EPHEMERALOWNER, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, MTIME, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, MZXID, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, NUMCHILDREN, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, PZXID, BigintType.BIGINT));
    add(builder, new BaseColumnHandle(_connectorId, VERSION, BigintType.BIGINT));
    return builder.build();
  }

  private void add(Builder<String, ColumnHandle> builder, BaseColumnHandle columnHandle) {
    builder.put(columnHandle.getColumnName(), columnHandle);
  }

}
