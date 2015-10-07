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

import io.airlift.slice.Slice;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fortitudetec.presto.BaseColumnHandle;

public class BlurRecordCursor implements RecordCursor {

  private final List<BaseColumnHandle> _columnHandles;
  private final Iface _client;

  public BlurRecordCursor(List<BaseColumnHandle> columnHandles, Iface client) {
    _columnHandles=columnHandles;
    _client=client;
  }

  @Override
  public long getTotalBytes() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public long getCompletedBytes() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public long getReadTimeNanos() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Type getType(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public boolean advanceNextPosition() {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public boolean getBoolean(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public long getLong(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public double getDouble(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Slice getSlice(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public Object getObject(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public boolean isNull(int field) {
    throw new RuntimeException("Not implemented.");
  }

  @Override
  public void close() {
    throw new RuntimeException("Not implemented.");
  }

}
