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

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

import java.util.List;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.fortitudetec.presto.spreadsheets.util.SpreadsheetReader;
import com.fortitudetec.presto.spreadsheets.util.Table;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class SpreadsheetRecordCursor implements RecordCursor {

  private final List<SpreadsheetColumnHandle> _columns;
  private final List<Type> _types;
  private final long _totalBytes;
  private final long _loadTime;
  private final int _numberOfRows;
  private final Table _table;

  private int _currentRow = -1;

  public SpreadsheetRecordCursor(String tableName, SpreadsheetReader spreadSheetHelper, List<SpreadsheetColumnHandle> columns,
      List<Type> types) {
    _columns = columns;
    _types = types;
    _totalBytes = spreadSheetHelper.getLength();
    _loadTime = spreadSheetHelper.getLoadTime();
    _table = spreadSheetHelper.getTable(tableName);
    _numberOfRows = _table.getNumberOfRows();
  }

  @Override
  public Type getType(int field) {
    return _types.get(field);
  }

  @Override
  public boolean advanceNextPosition() {
    _currentRow++;
    return _currentRow < _numberOfRows;
  }

  @Override
  public boolean isNull(int field) {
    SpreadsheetColumnHandle columnHandle = _columns.get(field);
    String columnName = columnHandle.getColumnName();
    return _table.isNull(_currentRow, columnName);
  }

  @Override
  public boolean getBoolean(int field) {
    SpreadsheetColumnHandle columnHandle = _columns.get(field);
    String columnName = columnHandle.getColumnName();
    return _table.getBoolean(_currentRow, columnName);
  }

  @Override
  public long getLong(int field) {
    throw new PrestoException(NOT_SUPPORTED, "Object not supported.");
  }

  @Override
  public double getDouble(int field) {
    SpreadsheetColumnHandle columnHandle = _columns.get(field);
    String columnName = columnHandle.getColumnName();
    return _table.getDouble(_currentRow, columnName);
  }

  @Override
  public Slice getSlice(int field) {
    SpreadsheetColumnHandle columnHandle = _columns.get(field);
    String columnName = columnHandle.getColumnName();
    String s = _table.getString(_currentRow, columnName);
    return Slices.wrappedBuffer(s.getBytes());
  }

  @Override
  public Object getObject(int field) {
    throw new PrestoException(NOT_SUPPORTED, "Object not supported.");
  }

  @Override
  public void close() {

  }

  public long getTotalBytes() {
    return _totalBytes;
  }

  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return _loadTime;
  }
}
