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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

public class SpreadsheetConnector implements Connector {

  private final ConnectorMetadata _metadata;
  private final ConnectorSplitManager _splitManager;
  private final ConnectorRecordSetProvider _recordSetProvider;

  public SpreadsheetConnector(UserGroupInformation ugi, Configuration configuration, Path basePath,
      String spreadsheetSubDir, boolean useFileCache) throws IOException {
    _metadata = new SpreadsheetMetadata(ugi, configuration, basePath, spreadsheetSubDir, useFileCache);
    _splitManager = new SpreadsheetSplitManager();
    _recordSetProvider = new SpreadsheetRecordSetProvider(ugi, configuration, useFileCache);
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
    return _metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return _splitManager;
  }

  @Override
  public ConnectorRecordSetProvider getRecordSetProvider() {
    return _recordSetProvider;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
    return SpreadsheetTransactionHandle.INSTANCE;
  }

}
