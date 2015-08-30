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
package com.fortitudetec.presto.spreadsheets.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.common.collect.ImmutableList;

public class SpreadsheetReader {

  private final XSSFWorkbook _workbook;
  private final Map<String, Table> _tables = new ConcurrentHashMap<String, Table>();
  private final long _length;
  private final long _loadTime;

  public SpreadsheetReader(InputStream inputStream, long length) throws IOException {
    long s = System.nanoTime();
    _workbook = new XSSFWorkbook(inputStream);
    for (int i = 0; i < _workbook.getNumberOfSheets(); i++) {
      XSSFSheet sheet = _workbook.getSheetAt(i);
      _tables.put(sheet.getSheetName().toLowerCase(), new Table(sheet));
    }
    _loadTime = System.nanoTime() - s;
    _length = length;
    inputStream.close();
  }

  public long getLength() {
    return _length;
  }

  public long getLoadTime() {
    return _loadTime;
  }

  public List<String> getTableNames() {
    return ImmutableList.copyOf(_tables.keySet());
  }

  public Table getTable(String tableName) {
    return _tables.get(tableName);
  }

}
