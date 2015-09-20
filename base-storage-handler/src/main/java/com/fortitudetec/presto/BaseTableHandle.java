package com.fortitudetec.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;

public interface BaseTableHandle extends ConnectorTableHandle {
  
  SchemaTableName getTableName();

  String getConnectorId();

}
