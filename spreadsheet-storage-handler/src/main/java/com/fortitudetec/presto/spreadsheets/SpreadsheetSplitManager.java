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

import java.util.List;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;

@SuppressWarnings("deprecation")
public class SpreadsheetSplitManager implements ConnectorSplitManager {

  private final String _connectorId;

  public SpreadsheetSplitManager(String connectorId) {
    _connectorId = connectorId;
  }

  @Override
  public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table,
      TupleDomain<ColumnHandle> tupleDomain) {
    SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) table;
    return new ConnectorPartitionResult(ImmutableList.<ConnectorPartition> of(new SpreadsheetPartition(
        spreadsheetTableHandle, tupleDomain)), tupleDomain);
  }

  @Override
  public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table,
      List<ConnectorPartition> partitions) {
    SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) table;
    SpreadsheetSplit spreadsheetSplit = new SpreadsheetSplit(_connectorId, spreadsheetTableHandle);
    return new FixedSplitSource(_connectorId, ImmutableList.of(spreadsheetSplit));
  }

}
