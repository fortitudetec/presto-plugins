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
package com.fortitudetec.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BaseColumnHandle implements ColumnHandle, ConnectorId {

  protected final String _columnName;
  protected final Type _type;
  protected final String _connectorId;

  @JsonCreator
  public BaseColumnHandle(@JsonProperty("connectorId") String connectorId,
      @JsonProperty("columnName") String columnName, @JsonProperty("type") Type type) {
    _connectorId = connectorId;
    _columnName = columnName;
    _type = type;
  }

  @JsonProperty
  public String getColumnName() {
    return _columnName;
  }

  @JsonProperty
  public Type getType() {
    return _type;
  }

  @JsonProperty
  public String getConnectorId() {
    return _connectorId;
  }

  @Override
  public String toString() {
    return "BaseColumnHandle [_columnName=" + _columnName + ", _type=" + _type + ", _connectorId=" + _connectorId + "]";
  }

}
