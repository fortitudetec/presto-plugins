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

import java.util.List;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.fortitudetec.presto.BaseColumnHandle;
import com.fortitudetec.presto.BaseRecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class ZooKeeperRecordSet extends BaseRecordSet {

  private final String _zkConnection;
  private final List<BaseColumnHandle> _columnHandles;
  private final int _sessionTimeout;

  public ZooKeeperRecordSet(String zkConnection, int sessionTimeout, List<? extends ColumnHandle> columnHandles) {
    super(columnHandles);
    _zkConnection = zkConnection;
    _sessionTimeout = sessionTimeout;
    Builder<BaseColumnHandle> builder = ImmutableList.builder();
    for (ColumnHandle columnHandle : columnHandles) {
      builder.add((BaseColumnHandle) columnHandle);
    }
    _columnHandles = builder.build();
  }

  @Override
  public RecordCursor cursor() {
    return new ZooKeeperRecordCursor(_columnHandles, _zkConnection, _sessionTimeout);
  }

}
