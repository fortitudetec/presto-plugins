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

import java.util.HashMap;
import java.util.Map;

import com.facebook.presto.Session;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.tests.DistributedQueryRunner;

public class ZooKeeperQueryRunner extends DistributedQueryRunner {

  public ZooKeeperQueryRunner() throws Exception {
    super(createDefaultSession(), 2, getExtraProps());
  }

  private static Session createDefaultSession() {
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("connector.name", "zk");
    properties.put("connection", "node-000");
    properties.put("sessionTimeout", "30000");
    return TestingSession.testSessionBuilder().setCatalog("zk").setCatalogProperties("zk", properties).build();
  }

  private static Map<String, String> getExtraProps() {
    return new HashMap<String, String>();
  }

}
