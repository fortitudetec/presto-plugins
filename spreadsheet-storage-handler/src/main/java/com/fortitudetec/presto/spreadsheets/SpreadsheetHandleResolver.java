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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

public class SpreadsheetHandleResolver implements ConnectorHandleResolver {

  private final String _connectorId;

  public SpreadsheetHandleResolver(String connectorId) {
    _connectorId = connectorId;
  }

  @Override
  public boolean canHandle(ConnectorTableHandle tableHandle) {
    if (tableHandle instanceof SpreadsheetTableHandle) {
      SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) tableHandle;
      return spreadsheetTableHandle.getConnectorId().equals(_connectorId);
    }
    return false;
  }

  @Override
  public boolean canHandle(ColumnHandle columnHandle) {
    if (columnHandle instanceof SpreadsheetColumnHandle) {
      SpreadsheetColumnHandle spreadsheetColumnHandle = (SpreadsheetColumnHandle) columnHandle;
      return spreadsheetColumnHandle.getConnectorId().equals(_connectorId);
    }
    return false;
  }

  @Override
  public boolean canHandle(ConnectorSplit split) {
    if (split instanceof SpreadsheetSplit) {
      SpreadsheetSplit spreadsheetSplit = (SpreadsheetSplit) split;
      return spreadsheetSplit.getConnectorId().equals(_connectorId);
    }
    return false;
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return SpreadsheetTableHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return SpreadsheetColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return SpreadsheetSplit.class;
  }

}
