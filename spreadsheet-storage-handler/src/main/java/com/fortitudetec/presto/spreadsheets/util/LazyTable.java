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
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LazyTable implements Table {

  private Table _table;
  private boolean _dataLoaded;
  private Path _path;
  private FileSystem _fileSystem;

  public LazyTable(FileSystem fileSystem, Path path) throws IOException {
    _dataLoaded = false;
    _fileSystem = fileSystem;
    _path = path;
    _table = BaseTable.load(fileSystem, path, _dataLoaded);
  }

  private void loadDataIfNeeded() {
    if (!_dataLoaded) {
      _dataLoaded = true;
      try {
        _table = BaseTable.load(_fileSystem, _path, _dataLoaded);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static Table create(FileSystem fileSystem, Path path) throws IOException {
    return new LazyTable(fileSystem, path);
  }

  @Override
  public String getName() {
    return _table.getName();
  }

  @Override
  public int getNumberOfRows() {
    return _table.getNumberOfRows();
  }

  @Override
  public TableType getType(String columnName) {
    return _table.getType(columnName);
  }

  @Override
  public List<String> getColumnNames() {
    return _table.getColumnNames();
  }

  @Override
  public boolean isNull(int row, String columnName) {
    loadDataIfNeeded();
    return _table.isNull(row, columnName);
  }

  @Override
  public boolean getBoolean(int row, String columnName) {
    loadDataIfNeeded();
    return _table.getBoolean(row, columnName);
  }

  @Override
  public String getString(int row, String columnName) {
    loadDataIfNeeded();
    return _table.getString(row, columnName);
  }

  @Override
  public double getDouble(int row, String columnName) {
    loadDataIfNeeded();
    return _table.getDouble(row, columnName);
  }

}
