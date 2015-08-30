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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;

public class Table {

  private Map<Coordinate, ColumnValue> values = new HashMap<>();
  private Map<String, Integer> columnNames = new TreeMap<>();
  private Map<Integer, TableType> columnTypes = new TreeMap<>();
  private final int _numnberOfRows;

  public Table(XSSFSheet sheet) {
    int numnberOfRows = 0;
    for (Row r : sheet) {
      for (Cell c : r) {
        int rowIndex = c.getRowIndex();
        int columnIndex = c.getColumnIndex();
        String humanReadableName = getHumanReadableName(columnIndex + 1);
        int baseColumnOffset = columnIndex * TableType.values().length;
        TableType type = getType(c);
        if (type == TableType.BLANK) {
          // move on, blank cells are treated like nulls
          continue;
        }
        int col = baseColumnOffset + type.offset;
        set(col, rowIndex, new ColumnValue(type, getValue(c)));
        String columnName = humanReadableName + "_" + type.name();
        columnNames.put(columnName.toLowerCase(), col);
        columnTypes.put(col, type);
      }
      numnberOfRows++;
    }
    reduceColumnNames();
    _numnberOfRows = numnberOfRows;
  }

  public int getNumberOfRows() {
    return _numnberOfRows;
  }

  private void reduceColumnNames() {
    Map<String, Set<String>> counts = new HashMap<>();
    for (String name : columnNames.keySet()) {
      int indexOf = name.indexOf('_');
      String basename = name.substring(0, indexOf);
      Set<String> names = counts.get(basename);
      if (names == null) {
        counts.put(basename, names = new HashSet<>());
      }
      names.add(name);
    }

    for (Entry<String, Set<String>> e : counts.entrySet()) {
      Set<String> value = e.getValue();
      if (value.size() == 1) {
        String newName = e.getKey();
        String currentName = value.iterator().next();
        columnNames.put(newName, columnNames.remove(currentName));
      }
    }
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
    Integer index = columnNames.get(columnName);
    return columnTypes.get(index);
  }

  public List<String> getColumnNames() {
    return new ArrayList<String>(columnNames.keySet());
  }

  public boolean isNull(int row, String columnName) {
    Integer col = columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return columnValue == null;
  }

  public boolean getBoolean(int row, String columnName) {
    Integer col = columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (boolean) columnValue.value;
  }

  public String getString(int row, String columnName) {
    Integer col = columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (String) columnValue.value;
  }

  public double getDouble(int row, String columnName) {
    Integer col = columnNames.get(columnName);
    ColumnValue columnValue = get(col, row);
    return (double) columnValue.value;
  }

  private Object getValue(Cell c) {
    return getValue(c, c.getCellType());
  }

  private Object getValue(Cell c, int type) {
    switch (type) {
    case Cell.CELL_TYPE_BLANK:
      return null;
    case Cell.CELL_TYPE_BOOLEAN:
      return c.getBooleanCellValue();
    case Cell.CELL_TYPE_ERROR:
      return c.getErrorCellValue();
    case Cell.CELL_TYPE_FORMULA:
      return getValue(c, c.getCachedFormulaResultType());
    case Cell.CELL_TYPE_NUMERIC:
      return c.getNumericCellValue();
    case Cell.CELL_TYPE_STRING:
      return c.getStringCellValue();
    default:
      throw new RuntimeException("Unknown type [" + type + "]");
    }
  }

  private TableType getType(Cell c) {
    return getType(c, c.getCellType());
  }

  private TableType getType(Cell c, int type) {
    switch (type) {
    case Cell.CELL_TYPE_BLANK:
      return TableType.BLANK;
    case Cell.CELL_TYPE_BOOLEAN:
      return TableType.BOOLEAN;
    case Cell.CELL_TYPE_ERROR:
      return TableType.ERROR;
    case Cell.CELL_TYPE_FORMULA:
      return getType(c, c.getCachedFormulaResultType());
    case Cell.CELL_TYPE_NUMERIC:
      return TableType.NUMBER;
    case Cell.CELL_TYPE_STRING:
      return TableType.STRING;
    default:
      throw new RuntimeException("Unknown type [" + type + "]");
    }
  }

  private ColumnValue get(int col, int row) {
    return values.get(new Coordinate(col, row));
  }

  private void set(int col, int row, ColumnValue value) {
    values.put(new Coordinate(col, row), value);
  }

  private static class Coordinate {

    private int _x;
    private int _y;

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

}
