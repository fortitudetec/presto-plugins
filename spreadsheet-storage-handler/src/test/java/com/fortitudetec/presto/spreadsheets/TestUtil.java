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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestUtil {

  public static final String SPREADSHEETS = "spreadsheets";
  public static final String PRESTO_EXAMPLE_XLSX = "Presto Example.xlsx";

  public static Path setupTest(Configuration conf, String user, Class<?> clazz) throws IOException {
    return setupTest(conf, user, clazz, SPREADSHEETS);
  }

  public static Path setupTest(Configuration conf, String user, Class<?> clazz, String spreadsheetSubDir)
      throws IOException {
    Path projectPath = new Path("./target/tmp/" + clazz.getName());
    FileSystem fileSystem = projectPath.getFileSystem(conf);
    projectPath = projectPath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    InputStream inputStream = clazz.getResourceAsStream("/" + PRESTO_EXAMPLE_XLSX);
    Path userPath = new Path(projectPath, user);
    Path spreadsheetPath = new Path(userPath, spreadsheetSubDir);
    Path file = new Path(spreadsheetPath, PRESTO_EXAMPLE_XLSX);
    FSDataOutputStream outputStream = fileSystem.create(file);
    IOUtils.copy(inputStream, outputStream);
    inputStream.close();
    outputStream.close();
    return projectPath;
  }
}
