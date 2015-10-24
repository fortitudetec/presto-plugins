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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

public class BaseHandleResolver implements ConnectorHandleResolver {

  protected final String _connectorId;

  public BaseHandleResolver(String connectorId) {
    _connectorId = connectorId;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return BaseSplit.class;
  }

  @Override
  public boolean canHandle(ConnectorTableLayoutHandle handle) {
    if (handle.getClass().equals(getTableLayoutHandleClass())) {
      ConnectorId connectorId = (ConnectorId) handle;
      return connectorId.getConnectorId().equals(connectorId);
    }
    return false;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return BaseTableLayoutHandle.class;
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return BaseTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return BaseColumnHandle.class;
  }

  @Override
  public boolean canHandle(ConnectorTableHandle tableHandle) {
    if (tableHandle.getClass().equals(getTableHandleClass())) {
      ConnectorId connectorId = (ConnectorId) tableHandle;
      return connectorId.getConnectorId().equals(connectorId);
    }
    return false;
  }

  @Override
  public boolean canHandle(ColumnHandle columnHandle) {
    if (columnHandle.getClass().equals(getColumnHandleClass())) {
      ConnectorId connectorId = (ConnectorId) columnHandle;
      return connectorId.getConnectorId().equals(connectorId);
    }
    return false;
  }

  @Override
  public boolean canHandle(ConnectorSplit split) {
    if (split.getClass().equals(getSplitClass())) {
      ConnectorId connectorId = (ConnectorId) split;
      return connectorId.getConnectorId().equals(connectorId);
    }
    return false;
  }

}
