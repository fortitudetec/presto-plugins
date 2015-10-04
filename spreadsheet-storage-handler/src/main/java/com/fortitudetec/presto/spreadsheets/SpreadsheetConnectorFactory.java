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

import java.util.Map;
import java.util.ServiceLoader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;

public class SpreadsheetConnectorFactory implements ConnectorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpreadsheetConnectorFactory.class);

  private final Configuration _configuration;

  public SpreadsheetConnectorFactory(ClassLoader classLoader) {
    _configuration = new HdfsConfiguration();
    // This is kind of stupid but because FileSystem only loads built in types
    // from the system classloader the DistributedFileSystem won't load if it's
    // in a non system class loader.
    ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
    for (FileSystem fs : serviceLoader) {
      LOGGER.info("Loading filesystem type {} class {}", fs.getScheme(), fs.getClass());
      _configuration.setClass("fs." + fs.getScheme() + ".impl", fs.getClass(), FileSystem.class);
    }
  }

  @Override
  public String getName() {
    return "spreadsheet";
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config) {
    Path basePath = new Path(config.get("basepath"));
    String spreadsheetSubDir = config.get("subdir");
    String useFileCacheStr = config.get("useFileCache");
    boolean useFileCache = true;
    if (useFileCacheStr != null) {
      useFileCache = Boolean.parseBoolean(useFileCacheStr);
    }
    return new SpreadsheetConnector(connectorId, _configuration, basePath, spreadsheetSubDir, useFileCache);
  }

}
