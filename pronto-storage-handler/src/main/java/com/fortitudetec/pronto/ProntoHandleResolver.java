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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

public class ProntoHandleResolver implements ConnectorHandleResolver {

  private final String _connectorId;

  public ProntoHandleResolver(String connectorId) {
    _connectorId = connectorId;
  }

  @Override
  public boolean canHandle(ConnectorTableHandle tableHandle) {
    if (tableHandle instanceof ProntoTableHandle) {
      ProntoTableHandle prontoTableHandle = (ProntoTableHandle) tableHandle;
      return _connectorId.equals(prontoTableHandle.getConnectorId());
    }
    return false;
  }

  @Override
  public boolean canHandle(ColumnHandle columnHandle) {
    if (columnHandle instanceof ProntoColumnHandle) {
      ProntoColumnHandle prontoColumnHandle = (ProntoColumnHandle) columnHandle;
      return _connectorId.equals(prontoColumnHandle.getConnectorId());
    }
    return false;
  }

  @Override
  public boolean canHandle(ConnectorSplit split) {
    if (split instanceof ProntoSplit) {
      ProntoSplit prontoSplit = (ProntoSplit) split;
      return _connectorId.equals(prontoSplit.getConnectorId());
    }
    return false;
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return ProntoTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return ProntoColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return ProntoSplit.class;
  }

}
