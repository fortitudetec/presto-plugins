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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.fortitudetec.presto.spreadsheets.util.SpreadsheetReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class SpreadsheetRecordSet implements RecordSet {

  private final List<Type> _types;
  private final List<SpreadsheetColumnHandle> _columns;
  private final SpreadsheetReader _spreadSheetHelper;
  private final String _tableName;

  public SpreadsheetRecordSet(String tableName, SpreadsheetReader spreadSheetHelper,
      List<? extends ColumnHandle> columns) {
    Builder<Type> builder1 = ImmutableList.builder();
    Builder<SpreadsheetColumnHandle> builder2 = ImmutableList.builder();
    for (ColumnHandle columnHandle : columns) {
      SpreadsheetColumnHandle baseColumnHandle = (SpreadsheetColumnHandle) columnHandle;
      builder1.add(baseColumnHandle.getType());
      builder2.add(baseColumnHandle);
    }
    _types = builder1.build();
    _columns = builder2.build();
    _spreadSheetHelper = spreadSheetHelper;
    _tableName = tableName;
  }

  @Override
  public List<Type> getColumnTypes() {
    return _types;
  }

  @Override
  public RecordCursor cursor() {
    return new SpreadsheetRecordCursor(_tableName, _spreadSheetHelper, _columns, _types);
  }

}
