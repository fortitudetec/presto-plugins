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
package com.fortitudetec.presto.blur;

import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

public class BlurConnector implements Connector {

  private final ConnectorHandleResolver _handleResolver;
  private final ConnectorMetadata _metadata;
  private final ConnectorSplitManager _splitManager;
  private final ConnectorRecordSetProvider _recordSetProvider;

  public BlurConnector(String connectorId, String connection, int sessionTimeout) {
    Iface client = BlurClient.getClientFromZooKeeperConnectionStr(connection);
    _handleResolver = new BlurHandleResolver(connectorId);
    _splitManager = new BlurSplitManager(connectorId);
    _recordSetProvider = new BlurRecordSetProvider(connectorId, client);
    _metadata = new BlurMetadata(connectorId, client);
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return _handleResolver;
  }

  @Override
  public ConnectorMetadata getMetadata() {
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
  public void shutdown() {

  }

}
