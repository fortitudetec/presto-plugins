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
import static com.fortitudetec.presto.spreadsheets.TestUtil.PRESTO_EXAMPLE_XLSX;
import static com.fortitudetec.presto.spreadsheets.TestUtil.SPREADSHEETS;
import static com.fortitudetec.presto.spreadsheets.TestUtil.setupTest;
import static java.util.Locale.ENGLISH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SpreadsheetMetadataTest {

  private static UserGroupInformation ugi;

  static {
    try {
      ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final String SCHEMA_NAME = "presto_example_xlsx";

  public static final ConnectorSession SESSION = new TestingConnectorSession("user1", Optional.of("test"), UTC_KEY,
      ENGLISH, System.currentTimeMillis(), ImmutableList.of(), ImmutableMap.of());

  private boolean useFileCache = true;
  private Configuration conf = new Configuration();

  @Test
  public void testListSchemaNames() throws IOException {
    Path basePath = setupTest(conf, SESSION.getUser(), SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(ugi, conf, basePath, SPREADSHEETS, useFileCache);
    List<String> listSchemaNames = spreadsheetMetadata.listSchemaNames(SESSION);
    assertEquals(1, listSchemaNames.size());
    assertEquals(SCHEMA_NAME, listSchemaNames.get(0));
  }

  @Test
  public void testListTables() throws IOException {
    Path basePath = setupTest(conf, SESSION.getUser(), SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(ugi, conf, basePath, SPREADSHEETS, useFileCache);
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
    Path basePath = setupTest(conf, SESSION.getUser(), SpreadsheetMetadataTest.class);
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(ugi, conf, basePath, SPREADSHEETS, useFileCache);
    List<SchemaTableName> listTables = spreadsheetMetadata.listTables(SESSION, SCHEMA_NAME);
    for (SchemaTableName name : listTables) {
      ConnectorTableHandle tableHandle = spreadsheetMetadata.getTableHandle(SESSION, name);
      assertTrue(tableHandle instanceof SpreadsheetTableHandle);
      SpreadsheetTableHandle spreadsheetTableHandle = (SpreadsheetTableHandle) tableHandle;
      String filePath = new Path(new Path(new Path(basePath, SESSION.getUser()), SPREADSHEETS),
          PRESTO_EXAMPLE_XLSX).toString();
      assertEquals(filePath, spreadsheetTableHandle.getSpreadsheetPath());
      SchemaTableName tableName = spreadsheetTableHandle.getTableName();
      assertEquals(name, tableName);
      assertEquals(SESSION.getUser(), spreadsheetTableHandle.getUser());
    }
  }
}
