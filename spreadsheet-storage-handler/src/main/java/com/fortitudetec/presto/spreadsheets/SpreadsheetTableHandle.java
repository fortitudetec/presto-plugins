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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fortitudetec.presto.BaseTableHandle;

public class SpreadsheetTableHandle extends BaseTableHandle {

  private final String _user;
  private final String _spreadsheetPath;

  @JsonCreator
  public SpreadsheetTableHandle(@JsonProperty("connectorId") String connectorId, @JsonProperty("user") String user,
      @JsonProperty("tableName") SchemaTableName tableName, @JsonProperty("spreadsheetPath") String spreadsheetPath) {
    super(connectorId, tableName);
    _user = user;
    _spreadsheetPath = spreadsheetPath;
  }

  @JsonProperty
  public String getUser() {
    return _user;
  }


  @JsonProperty
  public String getSpreadsheetPath() {
    return _spreadsheetPath;
  }


  @Override
  public String toString() {
    return "SpreadsheetTableHandle [_user=" + _user + ", _tableName=" + _tableName + ", _spreadsheetPath="
        + _spreadsheetPath + ", _connectorId=" + _connectorId + "]";
  }

}
