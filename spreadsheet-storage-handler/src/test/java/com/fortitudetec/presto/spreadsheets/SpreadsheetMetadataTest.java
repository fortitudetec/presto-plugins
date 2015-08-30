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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SpreadsheetMetadataTest {

  public static final ConnectorSession SESSION = new TestingConnectorSession("amccurry", UTC_KEY, ENGLISH,
      System.currentTimeMillis(), ImmutableList.of(), ImmutableMap.of());
  public static final String CONNECTOR_ID = "test";

  @Test
  public void testListSchemaNames() {
    Configuration configuration = new Configuration();
    Path basePath = new Path("hdfs://192.168.1.120/user");
    String spreadsheetSubDir = "spreadsheets";
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, configuration, basePath,
        spreadsheetSubDir);
    List<String> listSchemaNames = spreadsheetMetadata.listSchemaNames(SESSION);
    System.out.println(listSchemaNames);
  }

  @Test
  public void testListTables() {
    Configuration configuration = new Configuration();
    Path basePath = new Path("hdfs://192.168.1.120/user");
    String spreadsheetSubDir = "spreadsheets";
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, configuration, basePath,
        spreadsheetSubDir);
    List<SchemaTableName> listTables = spreadsheetMetadata.listTables(SESSION, "Digital Ocean Pricing.xlsx");
    System.out.println(listTables);
  }

  @Test
  public void testGetTableHandle() {
    Configuration configuration = new Configuration();
    Path basePath = new Path("hdfs://192.168.1.120/user");
    String spreadsheetSubDir = "spreadsheets";
    SpreadsheetMetadata spreadsheetMetadata = new SpreadsheetMetadata(CONNECTOR_ID, configuration, basePath,
        spreadsheetSubDir);
    List<SchemaTableName> listTables = spreadsheetMetadata.listTables(SESSION, "Digital Ocean Pricing.xlsx");
    for (SchemaTableName name : listTables) {
      ConnectorTableHandle tableHandle = spreadsheetMetadata.getTableHandle(SESSION, name);
      System.out.println(tableHandle);
    }
  }

}
