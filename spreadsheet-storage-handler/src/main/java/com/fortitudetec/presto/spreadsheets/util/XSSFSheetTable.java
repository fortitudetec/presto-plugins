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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;

public class XSSFSheetTable extends BaseTable {

  public XSSFSheetTable(String name, XSSFSheet sheet) {
    super(getNumberOfRows(sheet), name);
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
        int col = baseColumnOffset + type.type;
        set(col, rowIndex, new ColumnValue(type, getValue(c)));
        String columnName = humanReadableName + "_" + type.name();
        _columnNames.put(columnName.toLowerCase(), col);
        _columnTypes.put(col, type);
      }
    }
    reduceColumnNames();
  }

  private static int getNumberOfRows(XSSFSheet sheet) {
    int numnberOfRows = 0;
    Iterator<Row> iterator = sheet.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      numnberOfRows++;
    }
    return numnberOfRows;
  }

  private void reduceColumnNames() {
    Map<String, Set<String>> counts = new HashMap<>();
    for (String name : _columnNames.keySet()) {
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
        _columnNames.put(newName, _columnNames.remove(currentName));
      }
    }
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

  private void set(int col, int row, ColumnValue value) {
    _values.put(new Coordinate(col, row), value);
  }

}
