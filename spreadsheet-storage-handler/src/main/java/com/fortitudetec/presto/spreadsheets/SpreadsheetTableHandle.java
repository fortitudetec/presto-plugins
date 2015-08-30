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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SpreadsheetTableHandle implements ConnectorTableHandle {

  private final String _user;
  private final SchemaTableName _tableName;
  private final String _spreadsheetPath;
  private final String _connectorId;

  @JsonCreator
  public SpreadsheetTableHandle(@JsonProperty("connectorId") String connectorId, @JsonProperty("user") String user,
      @JsonProperty("tableName") SchemaTableName tableName, @JsonProperty("spreadsheetPath") String spreadsheetPath) {
    _connectorId = connectorId;
    _user = user;
    _tableName = tableName;
    _spreadsheetPath = spreadsheetPath;
  }

  @JsonProperty
  public String getUser() {
    return _user;
  }

  @JsonProperty
  public SchemaTableName getTableName() {
    return _tableName;
  }

  @JsonProperty
  public String getSpreadsheetPath() {
    return _spreadsheetPath;
  }

  @JsonProperty
  public String getConnectorId() {
    return _connectorId;
  }

  @Override
  public String toString() {
    return "SpreadsheetTableHandle [_user=" + _user + ", _tableName=" + _tableName + ", _spreadsheetPath="
        + _spreadsheetPath + ", _connectorId=" + _connectorId + "]";
  }

}
