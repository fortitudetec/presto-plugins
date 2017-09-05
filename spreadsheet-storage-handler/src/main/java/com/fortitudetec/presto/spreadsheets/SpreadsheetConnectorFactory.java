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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.fortitudetec.presto.BaseErrorCode;

public class SpreadsheetConnectorFactory implements ConnectorFactory {

  private static final String HDFS_SITE_XML = "hdfs-site.xml";

  private static final String CORE_SITE_XML = "core-site.xml";

  private static final Logger LOGGER = LoggerFactory.getLogger(SpreadsheetConnectorFactory.class);

  private static final String USE_FILE_CACHE = "useFileCache";
  private static final String SUBDIR = "subdir";
  private static final String BASEPATH = "basepath";

  private final AtomicReference<Configuration> _configuration = new AtomicReference<Configuration>(null);
  private final SpreadsheetHandleResolver _handleResolver;

  public SpreadsheetConnectorFactory(ClassLoader classLoader) {

    _handleResolver = new SpreadsheetHandleResolver();
  }

  @Override
  public String getName() {
    return "spreadsheet";
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
    Configuration configuration = getConfiguration(config.get("hdfs.conf"));
    Path basePath = new Path(config.get(BASEPATH));
    String spreadsheetSubDir = config.get(SUBDIR);
    String useFileCacheStr = config.get(USE_FILE_CACHE);
    boolean useFileCache = true;
    if (useFileCacheStr != null) {
      useFileCache = Boolean.parseBoolean(useFileCacheStr);
    }
    return new SpreadsheetConnector(configuration, basePath, spreadsheetSubDir, useFileCache);
  }

  private Configuration getConfiguration(String path) {
    synchronized (_configuration) {
      Configuration configuration = new Configuration(true);
      addFileIfExists(path + "/" + CORE_SITE_XML, configuration);
      addFileIfExists(path + "/" + HDFS_SITE_XML, configuration);
      // This is kind of stupid but because FileSystem only loads built in types
      // from the system classloader the DistributedFileSystem won't load if
      // it's
      // in a non system class loader.
      ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
      for (FileSystem fs : serviceLoader) {
        LOGGER.info("Loading filesystem type {} class {}", fs.getScheme(), fs.getClass());
        configuration.setClass("fs." + fs.getScheme() + ".impl", fs.getClass(), FileSystem.class);
      }
      _configuration.set(configuration);
      return _configuration.get();
    }
  }

  private void addFileIfExists(String path, Configuration configuration) {
    File file = new File(path);
    if (file.exists()) {
      try {
        configuration.addResource(new FileInputStream(file));
      } catch (FileNotFoundException e) {
        throw new PrestoException(BaseErrorCode.CONFIG_ERROR, path + " not found.");
      }
    }
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return _handleResolver;
  }

}
