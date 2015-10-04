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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

public class BaseTable implements Table, Writable {

  protected int _numberOfRows;
  protected String _name;
  protected boolean _loadData;
  protected final Map<Coordinate, ColumnValue> _values = new HashMap<>();
  protected final Map<String, Integer> _columnNames = new TreeMap<>();
  protected final Map<Integer, TableType> _columnTypes = new TreeMap<>();

  public static Table load(FileSystem fileSystem, Path path, boolean loadData) throws IOException {
    BaseTable table = new BaseTable(loadData);
    try (FSDataInputStream inputStream = fileSystem.open(path)) {
      table.readFields(inputStream);
    }
    return table;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(_numberOfRows);
    Util.writeString(out, _name);

    out.writeInt(_columnNames.size());
    for (Entry<String, Integer> e : _columnNames.entrySet()) {
      String key = e.getKey();
      Util.writeString(out, key);
      Integer value = e.getValue();
      out.writeInt(value);
    }

    out.writeInt(_columnTypes.size());
    for (Entry<Integer, TableType> e : _columnTypes.entrySet()) {
      Integer key = e.getKey();
      out.writeInt(key);
      TableType value = e.getValue();
      out.writeInt(value.type);
    }

    out.writeInt(_values.size());
    for (Entry<Coordinate, ColumnValue> e : _values.entrySet()) {
      Coordinate key = e.getKey();
      key.write(out);
      ColumnValue value = e.getValue();
      value.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _numberOfRows = in.readInt();
    _name = Util.readString(in);

    int len = in.readInt();
    _columnNames.clear();
    for (int i = 0; i < len; i++) {
      String k = Util.readString(in);
      int v = in.readInt();
      _columnNames.put(k, v);
    }

    len = in.readInt();
    _columnTypes.clear();
    for (int i = 0; i < len; i++) {
      int k = in.readInt();
      int v = in.readInt();
      _columnTypes.put(k, TableType.lookup(v));
    }

    if (_loadData) {
      len = in.readInt();
      _values.clear();
      for (int i = 0; i < len; i++) {
        Coordinate k = new Coordinate();
        k.readFields(in);
        ColumnValue v = new ColumnValue();
        v.readFields(in);
        _values.put(k, v);
      }
    }
  }

  protected BaseTable(boolean loadData) {
    _loadData = loadData;
  }

  public BaseTable(int numberOfRows, String name) {
    _numberOfRows = numberOfRows;
    _name = name;
  }

  public int getNumberOfRows() {
    return _numberOfRows;
  }

  public static String getHumanReadableName(int num) {
    String result = "";
    while (num > 0) {
      num--;
      int remainder = num % 26;
      result = ((char) (remainder + 'A')) + result;
      num = (num - remainder) / 26;
    }
    return result;
  }

  public TableType getType(String columnName) {
    Integer index = _columnNames.get(columnName);
    return _columnTypes.get(index);
  }

  public List<String> getColumnNames() {
    return new ArrayList<String>(_columnNames.keySet());
  }

  public boolean isNull(int row, String columnName) {
    Integer col = _columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return columnValue == null;
  }

  public boolean getBoolean(int row, String columnName) {
    Integer col = _columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (boolean) columnValue.value;
  }

  public String getString(int row, String columnName) {
    Integer col = _columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (String) columnValue.value;
  }

  public double getDouble(int row, String columnName) {
    Integer col = _columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (double) columnValue.value;
  }

  protected ColumnValue get(int col, int row) {
    return _values.get(new Coordinate(col, row));
  }

  protected static class Coordinate implements Writable {

    private int _x;
    private int _y;

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(_x);
      out.writeInt(_y);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      _x = in.readInt();
      _y = in.readInt();
    }

    public Coordinate() {

    }

    public Coordinate(int x, int y) {
      _x = x;
      _y = y;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + _x;
      result = prime * result + _y;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Coordinate other = (Coordinate) obj;
      if (_x != other._x)
        return false;
      if (_y != other._y)
        return false;
      return true;
    }

  }

  @Override
  public String getName() {
    return _name;
  }

}
