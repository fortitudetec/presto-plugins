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
package com.fortitudetec.pronto;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.google.common.base.Splitter;

public class ProntoFactory implements ConnectorFactory {
  private static final String HDFS_CONFIG_RESOURCES = "hdfs.config.resources";
  private static final Logger LOGGER = LoggerFactory.getLogger(ProntoFactory.class);
  private static final String PRONTO = "pronto";
  private final ClassLoader _classLoader;
  private Configuration _configuration;

  public ProntoFactory(ClassLoader classLoader) {
    _classLoader = classLoader;
  }

  @Override
  public String getName() {
    return PRONTO;
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config) {
    try {
      if (_configuration == null) {
        _configuration = getConfiguration(config);
      }
      return new ProntoConnector(connectorId, config, _classLoader, _configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Configuration getConfiguration(Map<String, String> config) throws IOException {
    // This is kind of stupid but because FileSystem only loads built in types
    // from the system classloader the DistributedFileSystem won't load if it's
    // in a non system class loader.
    Configuration configuration = new Configuration();
    ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
    for (FileSystem fs : serviceLoader) {
      LOGGER.info("Loading filesystem type {} class {}", fs.getScheme(), fs.getClass());
      configuration.setClass("fs." + fs.getScheme() + ".impl", fs.getClass(), FileSystem.class);
    }
    String resources = config.get(HDFS_CONFIG_RESOURCES);
    if (resources != null) {
      for (String path : Splitter.on(',').split(resources)) {
        configuration.addResource(toInputStream(path));
      }
    }
    return configuration;
  }

  private InputStream toInputStream(String path) throws IOException {
    try (FileInputStream input = new FileInputStream(path)) {
      return new ByteArrayInputStream(IOUtils.toByteArray(input));
    }
  }

}
