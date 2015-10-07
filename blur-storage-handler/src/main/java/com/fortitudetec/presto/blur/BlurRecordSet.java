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

import java.util.List;

import org.apache.blur.thrift.generated.Blur.Iface;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.fortitudetec.presto.BaseColumnHandle;
import com.fortitudetec.presto.BaseRecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class BlurRecordSet extends BaseRecordSet {

  private final Iface _client;
  private final List<BaseColumnHandle> _columnHandles;

  public BlurRecordSet(List<? extends ColumnHandle> columnHandles, Iface client) {
    super(columnHandles);
    _client = client;
    Builder<BaseColumnHandle> builder = ImmutableList.builder();
    for (ColumnHandle columnHandle : columnHandles) {
      builder.add((BaseColumnHandle) columnHandle);
    }
    _columnHandles = builder.build();
  }

  @Override
  public RecordCursor cursor() {
    return new BlurRecordCursor(_columnHandles, _client);
  }

}
