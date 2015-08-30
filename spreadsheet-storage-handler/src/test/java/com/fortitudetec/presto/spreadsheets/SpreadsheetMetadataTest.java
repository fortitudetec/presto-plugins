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

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SpreadsheetMetadataTest {

  private static final String SCHEMA_NAME = "presto_example_xlsx";
  private static final String SPREADSHEETS = "spreadsheets";
  private static final String PRESTO_EXAMPLE_XLSX = "Presto Example.xlsx";
  public static final ConnectorSession SESSION = new TestingConnectorSession("user1", UTC_KEY, ENGLISH,
      System.currentTimeMillis(), ImmutableList.of(), ImmutableMap.of());
  public static final String CONNECTOR_ID = "test";

  private Configuration conf = new Configuration();

  public static Path setupTest(Configuration conf, Class<SpreadsheetMetadataTest> clazz) throws IOException {
    return setupTest(conf, clazz, SPREADSHEETS);
  }

  public static Path setupTest(Configuration conf, Class<SpreadsheetMetadataTest> clazz, String spreadsheetSubDir)
      throws IOException {
    Path projectPath = new Path("./target/tmp/" + clazz.getName());
    FileSystem fileSystem = projectPath.getFileSystem(conf);
    projectPath = projectPath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    InputStream inputStream = clazz.getResourceAsStream("/" + PRESTO_EXAMPLE_XLSX);
    Path userPath = new Path(projectPath, SESSION.getUser());
    Path spreadsheetPath = new Path(userPath, spreadsheetSubDir);
    Path file = new Path(spreadsheetPath, PRESTO_EXAMPLE_XLSX);
    FSDataOutputStream outputStream = fileSystem.create(file);
    IOUtils.copy(inputStream, outputStream);
    inputStream.close();
    outputStream.close();
    return projectPath;
  }

  @Test
  public void testListSchemaNames() throws IOException {
    Path basePath = setupTest(conf, SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, conf, basePath, SPREADSHEETS);
    List<String> listSchemaNames = spreadsheetMetadata.listSchemaNames(SESSION);
    assertEquals(1, listSchemaNames.size());
    assertEquals(SCHEMA_NAME, listSchemaNames.get(0));
  }

  @Test
  public void testListTables() throws IOException {
    Path basePath = setupTest(conf, SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, conf, basePath, SPREADSHEETS);
    List<SchemaTableName> listTables = spreadsheetMetadata.listTables(SESSION, SCHEMA_NAME);
    assertEquals(2, listTables.size());
    List<String> tables = new ArrayList<String>();
    for (SchemaTableName schemaTableName : listTables) {
      assertEquals(SCHEMA_NAME, schemaTableName.getSchemaName());
      tables.add(schemaTableName.getTableName());
    }
    Collections.sort(tables);
    assertEquals("multiple_types_per_column", tables.get(0));
    assertEquals("simple_sheet", tables.get(1));
  }

  @Test
  public void testGetTableHandle() throws IOException {
    Path basePath = setupTest(conf, SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, conf, basePath, SPREADSHEETS);
    List<SchemaTableName> listTables = spreadsheetMetadata.listTables(SESSION, SCHEMA_NAME);
    for (SchemaTableName name : listTables) {
      ConnectorTableHandle tableHandle = spreadsheetMetadata.getTableHandle(SESSION, name);
      assertTrue(tableHandle instanceof SpreadsheetTableHandle);
      SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) tableHandle;
      String connectorId = spreadsheetTableHandle.getConnectorId();
      assertEquals(CONNECTOR_ID, connectorId);
      String filePath = new Path(new Path(new Path(basePath, SESSION.getUser()), SPREADSHEETS), PRESTO_EXAMPLE_XLSX)
          .toString();
      assertEquals(filePath, spreadsheetTableHandle.getSpreadsheetPath());
      SchemaTableName tableName = spreadsheetTableHandle.getTableName();
      assertEquals(name, tableName);
      assertEquals(SESSION.getUser(), spreadsheetTableHandle.getUser());
    }
  }
}
