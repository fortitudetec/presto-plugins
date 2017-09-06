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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.fortitudetec.presto.spreadsheets.util.xss.XSSFSheetTable;
import com.google.common.collect.ImmutableList;

public class SpreadsheetReader {

  private static final Logger LOGGER = Logger.getLogger(SpreadsheetReader.class.getName());

  private final Map<String, Table> _tables = new ConcurrentHashMap<String, Table>();
  private final long _length;
  private final long _loadTime;
  private final boolean _fileCache;

  public SpreadsheetReader(boolean useFileCache, Configuration configuration, Path file) throws IOException {
    _fileCache = useFileCache;
    FileSystem fileSystem = file.getFileSystem(configuration);
    FileStatus fileStatus = fileSystem.getFileStatus(file);
    long s = System.nanoTime();
    _length = fileStatus.getLen();
    if (_fileCache) {
      if (!isCacheValid(fileStatus, fileSystem, file)) {
        buildCache(fileSystem, file);
      }
      loadSheets(fileSystem, file);
    } else {
      loadSheetsDirectly(fileSystem, file);
    }
    _loadTime = System.nanoTime() - s;
  }

  private void loadSheets(FileSystem fileSystem, Path file) throws IOException {
    Path cachePath = getCachePath(file);
    FileStatus[] listStatus = fileSystem.listStatus(cachePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        if (path.getName()
                .startsWith("sheet.")) {
          return true;
        }
        return false;
      }
    });
    for (FileStatus fileStatus : listStatus) {
      registerTable(LazyTable.create(fileSystem, fileStatus.getPath()));
    }
  }

  private void registerTable(Table table) {
    _tables.put(table.getName(), table);
  }

  private void buildCache(FileSystem fileSystem, Path file) throws IOException {
    Path cachePath = getCachePath(file);
    fileSystem.delete(cachePath, true);
    try (FSDataInputStream inputStream = fileSystem.open(file)) {
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        XSSFSheet sheet = workbook.getSheetAt(i);
        XSSFSheetTable tableXSSFSheet = getXSSFSheetTable(sheet);
        writeTable(fileSystem, cachePath, tableXSSFSheet);
      }
      workbook.close();
    }
    FileStatus fileStatus = fileSystem.getFileStatus(file);
    try (FSDataOutputStream outputStream = fileSystem.create(getSheetFsPath(cachePath))) {
      fileStatus.write(outputStream);
    }
  }

  private XSSFSheetTable getXSSFSheetTable(XSSFSheet sheet) {
    String normalizeName = NormalizeName.normalizeName(sheet.getSheetName(), _tables.keySet());
    return new XSSFSheetTable(normalizeName, sheet);
  }

  private Path getSheetFsPath(Path cachePath) {
    return new Path(cachePath, "index.fs");
  }

  private Path getCachePath(Path file) {
    return new Path(file.getParent(), "." + file.getName());
  }

  private void writeTable(FileSystem fileSystem, Path cacheDir, BaseTable table) throws IOException {
    Path file = new Path(cacheDir, "sheet." + table.getName());
    LOGGER.info("Writing cache table [" + file + "].");
    try (FSDataOutputStream outputStream = fileSystem.create(file)) {
      table.write(outputStream);
    }
  }

  private void loadSheetsDirectly(FileSystem fileSystem, Path file) throws IOException {
    try (FSDataInputStream inputStream = fileSystem.open(file)) {
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
        XSSFSheet sheet = workbook.getSheetAt(i);
        XSSFSheetTable tableXSSFSheet = getXSSFSheetTable(sheet);
        registerTable(tableXSSFSheet);
      }
      workbook.close();
    }
  }

  private boolean isCacheValid(FileStatus fileStatus, FileSystem fileSystem, Path file) throws IOException {
    Path cachePath = getCachePath(file);
    Path sheetFsPath = getSheetFsPath(cachePath);
    if (!fileSystem.exists(sheetFsPath)) {
      return false;
    }
    FileStatus onDiskFileStatus = new FileStatus();
    try (FSDataInputStream inputStream = fileSystem.open(sheetFsPath)) {
      onDiskFileStatus.readFields(inputStream);
    }
    return onDiskFileStatus.getModificationTime() == fileStatus.getModificationTime();
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
