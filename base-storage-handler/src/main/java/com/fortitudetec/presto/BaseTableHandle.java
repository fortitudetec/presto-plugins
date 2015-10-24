package com.fortitudetec.presto;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BaseTableHandle implements ConnectorTableHandle, ConnectorId {

  protected final SchemaTableName _tableName;
  protected final String _connectorId;

  @JsonCreator
  public BaseTableHandle(@JsonProperty("connectorId") String connectorId,
      @JsonProperty("tableName") SchemaTableName tableName) {
    _connectorId = connectorId;
    _tableName = tableName;
  }

  @JsonProperty
  public SchemaTableName getTableName() {
    return _tableName;
  }

  @JsonProperty
  public String getConnectorId() {
    return _connectorId;
  }

  @Override
  public String toString() {
    return "BaseTableHandle [_tableName=" + _tableName + ", _connectorId=" + _connectorId + "]";
  }

}
