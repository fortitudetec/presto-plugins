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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.fortitudetec.presto.BaseHandleResolver;

public class SpreadsheetHandleResolver extends BaseHandleResolver {

  public SpreadsheetHandleResolver(String connectorId) {
    super(connectorId);
  }

  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return SpreadsheetTableHandle.class;
  }


  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return SpreadsheetSplit.class;
  }

}
