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

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

public class SpreadsheetConnectorFactory implements ConnectorFactory {

  private static final String PROXY_USER = "proxyUser";

  private static final Logger LOGGER = Logger.getLogger(SpreadsheetConnectorFactory.class.getName());

  private static final String SPREADSHEET = "spreadsheet";
  private static final String USE_FILE_CACHE = "useFileCache";
  private static final String SUBDIR = "subdir";
  private static final String BASEPATH = "basepath";

  private final Configuration _configuration = new Configuration();
  private final SpreadsheetHandleResolver _handleResolver;

  public SpreadsheetConnectorFactory(ClassLoader classLoader) {
    setupConfiguration();
    _handleResolver = new SpreadsheetHandleResolver();
  }

  @Override
  public String getName() {
    return SPREADSHEET;
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
    Path basePath = new Path(config.get(BASEPATH));
    String spreadsheetSubDir = config.get(SUBDIR);
    String useFileCacheStr = config.get(USE_FILE_CACHE);
    String proxyUserStr = config.get(PROXY_USER);
    boolean proxyUser = false;
    if (proxyUserStr != null) {
      proxyUser = Boolean.parseBoolean(proxyUserStr);
    }
    boolean useFileCache = true;
    if (useFileCacheStr != null) {
      useFileCache = Boolean.parseBoolean(useFileCacheStr);
    }
    try {
      return new SpreadsheetConnector(UserGroupInformation.getCurrentUser(), _configuration, basePath,
          spreadsheetSubDir, useFileCache, proxyUser);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setupConfiguration() {
    // This is kind of stupid but because FileSystem only loads built in
    // types from the system classloader the DistributedFileSystem won't
    // load if it's in a non system class loader.
    ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
    for (FileSystem fs : serviceLoader) {
      LOGGER.info("Loading filesystem type " + fs.getScheme() + " class " + fs.getClass());
      _configuration.setClass("fs." + fs.getScheme() + ".impl", fs.getClass(), FileSystem.class);
    }
    // testConfiguration(_configuration);
  }

  // private void testConfiguration(Configuration configuration) {
  // try {
  // FileSystem fileSystem = FileSystem.get(configuration);
  // fileSystem.listStatus(new Path("/"));
  // LOGGER.info("Root path lookup successful.");
  // } catch (IOException e) {
  // LOGGER.log(Level.SEVERE, "Could not lookup from root path.");
  // throw new RuntimeException(e);
  // }
  // }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return _handleResolver;
  }

}
