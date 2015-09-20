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
package com.fortitudetec.presto.zookeeper;

import static com.facebook.presto.spi.StandardErrorCode.EXTERNAL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.AVERSION;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CTIME;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CVERSION;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.CZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATA;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATALENGTH;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.DATA_AS_STRING;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.EPHEMERALOWNER;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.MTIME;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.MZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.NUMCHILDREN;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.PATH;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.PZXID;
import static com.fortitudetec.presto.zookeeper.ZooKeeperColumnNames.VERSION;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class ZooKeeperRecordCursor implements RecordCursor {

  private static final String UTF_8 = "UTF-8";

  private static final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperRecordCursor.class);

  private static final Watcher NOTHING = new Watcher() {
    @Override
    public void process(WatchedEvent event) {

    }
  };

  private final ZooKeeper _zooKeeper;
  private final List<ZooKeeperColumnHandle> _columnHandles;
  private final List<String> _pathList;

  private int _index = -1;
  private Stat _stat;
  private byte[] _data;
  private boolean _fetched;

  public ZooKeeperRecordCursor(List<ZooKeeperColumnHandle> columnHandles, String zkConnection, int sessionTimeout) {
    _columnHandles = columnHandles;
    try {
      _zooKeeper = new ZooKeeper(zkConnection, sessionTimeout, NOTHING);
    } catch (IOException e) {
      throw new PrestoException(EXTERNAL, e.getMessage(), e);
    }
    Builder<String> builder = ImmutableList.builder();
    try {
      buildPathList(builder, "/");
    } catch (KeeperException | InterruptedException e) {
      throw new PrestoException(EXTERNAL, "Unknown error while trying to build path list.", e);
    }
    _pathList = builder.build();
  }

  private void buildPathList(Builder<String> builder, String path) throws KeeperException, InterruptedException {
    builder.add(path);
    List<String> children = new ArrayList<String>(_zooKeeper.getChildren(path, false));
    Collections.sort(children);
    for (String c : children) {
      buildPathList(builder, trim(path + "/" + c));
    }
  }

  private String trim(String s) {
    return s.replace("//", "/");
  }

  @Override
  public void close() {
    try {
      _zooKeeper.close();
    } catch (InterruptedException e) {
      LOGGER.error("Unknown error during closing of zk client", e);
    }
  }

  @Override
  public boolean advanceNextPosition() {
    _fetched = false;
    _data = null;
    _stat = null;
    _index++;
    while (_index < _pathList.size()) {
      String path = _pathList.get(_index);
      try {
        _stat = _zooKeeper.exists(path, false);
        if (_stat != null) {
          return true;
        } else {
          _index++;
        }
      } catch (KeeperException | InterruptedException e) {
        throw new PrestoException(EXTERNAL, "Unknown error while trying to stat.", e);
      }
    }
    return false;
  }

  @Override
  public long getLong(int field) {
    ZooKeeperColumnHandle zooKeeperColumnHandle = _columnHandles.get(field);
    String columnName = zooKeeperColumnHandle.getColumnName();
    return getValue(columnName);
  }

  private long getValue(String columnName) {
    switch (columnName) {
    case VERSION: {
      return _stat.getVersion();
    }
    case PZXID: {
      return _stat.getPzxid();
    }
    case MZXID: {
      return _stat.getMzxid();
    }
    case NUMCHILDREN: {
      return _stat.getNumChildren();
    }
    case MTIME: {
      return _stat.getMtime();
    }
    case EPHEMERALOWNER: {
      return _stat.getEphemeralOwner();
    }
    case DATALENGTH: {
      return _stat.getDataLength();
    }
    case CZXID: {
      return _stat.getCzxid();
    }
    case CVERSION: {
      return _stat.getCversion();
    }
    case CTIME: {
      return _stat.getCtime();
    }
    case AVERSION: {
      return _stat.getAversion();
    }
    default:
      throw new PrestoException(EXTERNAL, "Column [" + columnName + "] cannot be found.");
    }
  }

  @Override
  public boolean isNull(int field) {
    ZooKeeperColumnHandle zooKeeperColumnHandle = _columnHandles.get(field);
    String columnName = zooKeeperColumnHandle.getColumnName();
    if (columnName.equals(PATH)) {
      return false;
    } else if (columnName.equals(DATA) || columnName.equals(DATA_AS_STRING)) {
      return fetchData();
    } else {
      return false;
    }
  }

  private boolean fetchData() {
    if (_fetched) {
      return _data == null;
    }
    String path = _pathList.get(_index);
    try {
      _data = _zooKeeper.getData(path, false, _stat);
      _fetched = true;
      return _data == null;
    } catch (KeeperException | InterruptedException e) {
      throw new PrestoException(EXTERNAL, "Unknown error while trying to fetch data.", e);
    }
  }

  @Override
  public Slice getSlice(int field) {
    ZooKeeperColumnHandle zooKeeperColumnHandle = _columnHandles.get(field);
    String columnName = zooKeeperColumnHandle.getColumnName();
    if (columnName.equals(PATH)) {
      return Slices.wrappedBuffer(toBytes(_pathList.get(_index)));
    } else if (columnName.equals(DATA)) {
      return Slices.wrappedBuffer(_data);
    } else if (columnName.equals(DATA_AS_STRING)) {
      return Slices.wrappedBuffer(toBytes(toStringBinary(_data, 0, _data.length)));
    } else {
      throw new PrestoException(EXTERNAL, "Column [" + columnName + "] is not slice.");
    }

  }

  private byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new PrestoException(EXTERNAL, "UTF-8 not supported?", e);
    }
  }

  @Override
  public Type getType(int field) {
    return _columnHandles.get(field).getType();
  }

  @Override
  public long getTotalBytes() {
    return 0;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public long getCompletedBytes() {
    return 0;
  }

  @Override
  public Object getObject(int field) {
    throw new PrestoException(NOT_SUPPORTED, "This record set does not support objects");
  }

  @Override
  public double getDouble(int field) {
    throw new PrestoException(NOT_SUPPORTED, "This record set does not support doubles");
  }

  @Override
  public boolean getBoolean(int field) {
    throw new PrestoException(NOT_SUPPORTED, "This record set does not support booleans");
  }

  public static String toStringBinary(final byte[] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    try {
      String first = new String(b, off, len, "ISO-8859-1");
      for (int i = 0; i < first.length(); ++i) {
        int ch = first.charAt(i) & 0xFF;
        if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
            || " `~!@#$%^&*()-_=+[]{}\\|;:'\",.<>/?".indexOf(ch) >= 0) {
          result.append(first.charAt(i));
        } else {
          result.append(String.format("\\x%02X", ch));
        }
      }
    } catch (UnsupportedEncodingException e) {
      LOGGER.error("ISO-8859-1 not supported?", e);
    }
    return result.toString();
  }

}
