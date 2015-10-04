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

import java.util.Map;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.PrestoException;
import static com.facebook.presto.spi.StandardErrorCode.EXTERNAL;

public class ZooKeeperConnectorFactory implements ConnectorFactory {

  public ZooKeeperConnectorFactory(ClassLoader classLoader) {

  }

  @Override
  public String getName() {
    return "zookeeper";
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config) {
    String connection = getOrThrowExceptionIfNull(config, "connection");
    String sessionTimoutStr = getOrThrowExceptionIfNull(config, "sessionTimeout");
    int sessionTimeout = Integer.parseInt(sessionTimoutStr);
    return new ZooKeeperConnector(connectorId, connection, sessionTimeout);
  }

  private String getOrThrowExceptionIfNull(Map<String, String> config, String name) {
    String s = config.get(name);
    if (s == null) {
      throw new PrestoException(EXTERNAL, name + " missing in configuration");
    }
    return s;
  }
}