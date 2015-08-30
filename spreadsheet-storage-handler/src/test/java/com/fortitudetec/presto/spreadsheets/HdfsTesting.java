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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTesting {

  public static void main(String[] args) throws IOException {
    Path path = new Path("hdfs://192.168.1.120/user/amccurry/spreadsheets/");
    Configuration configuration = new Configuration();
    FileSystem fileSystem = path.getFileSystem(configuration);
    fileSystem.mkdirs(path);
//    fileSystem.copyFromLocalFile(new Path("/Users/amccurry/Downloads/Nic Hard Drive Lookup.xlsx"), path);
    
    fileSystem.copyFromLocalFile(new Path("/Users/amccurry/Downloads/test.csv.xlsx"), path);

    // Path path = new
    // Path("hdfs://192.168.1.120/user/amccurry/spreadsheets/Digital Ocean Pricing.xlsx");
    // Configuration configuration = new Configuration();
    // FileSystem fileSystem = path.getFileSystem(configuration);
    // FSDataInputStream inputStream = fileSystem.open(path);
    // SpreadsheetHelper spreadSheetHelper = new SpreadsheetHelper(inputStream,
    // 0);
    //
    // for (String table : spreadSheetHelper.getTableNames()) {
    // List<String> columnNames = spreadSheetHelper.getColumnNames(table);
    // for (String columnName : columnNames) {
    // System.out.println(columnName + "=>" +
    // spreadSheetHelper.getColumnType(table, columnName));
    // }
    // }

  }

}
