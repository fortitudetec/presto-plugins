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
package com.fortitudetec.presto.spreadsheets.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ColumnValue implements Writable {

  public TableType type;
  public Object value;

  public ColumnValue() {

  }

  public ColumnValue(TableType type, Object value) {
    this.type = type;
    this.value = value;
  }

  @Override
  public String toString() {
    return "ColumnValue [type=" + type + ", value=" + value + "]";
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(type.type);
    switch (type) {
    case BLANK:
      return;
    case BOOLEAN:
      out.writeBoolean((boolean) value);
      return;
    case ERROR:
      out.writeByte((byte) value);
      return;
    case NUMBER:
      out.writeDouble((double) value);
      return;
    case STRING:
      Util.writeString(out, (String) value);
      return;
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = TableType.lookup(in.readInt());
    switch (type) {
    case BLANK:
      return;
    case BOOLEAN:
      value = in.readBoolean();
      return;
    case ERROR:
      value = in.readByte();
      return;
    case NUMBER:
      value = in.readDouble();
      return;
    case STRING:
      value = Util.readString(in);
      return;
    default:
      throw new IOException("Type [" + type + "] not supported.");
    }
  }
}
