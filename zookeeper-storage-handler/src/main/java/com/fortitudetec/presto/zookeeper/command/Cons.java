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
package com.fortitudetec.presto.zookeeper.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jline.internal.InputStreamReader;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class Cons extends BaseCommand {

  public static void main(String[] args) throws IOException {
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    Cons conf = new Cons(host, port);
    List<String[]> execute = conf.execute();
    for (String[] s : execute) {
      System.out.println(Arrays.asList(s));
    }
  }

  private final String _host;
  private final int _port;
  private final String[] name = new String[] { "connection", "metric", "value" };

  public Cons(String host, int port) throws IOException {
    _host = host;
    _port = port;
  }

  @Override
  public String command() {
    return "cons";
  }

  @Override
  public String[] names() {
    return name;
  }

  @Override
  public List<String[]> execute() throws IOException {
    Socket socket = new Socket(_host, _port);
    BufferedReader reader = null;
    try {
      OutputStream outstream = socket.getOutputStream();
      outstream.write(command().getBytes());
      outstream.flush();
      // this replicates NC - close the output stream before reading
      socket.shutdownOutput();
      reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      String line;
      List<String[]> results = new ArrayList<String[]>();
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        String connection = getConnection(line);
        Map<String, String> props = getProps(line);
        for (Entry<String, String> e : props.entrySet()) {
          results.add(new String[] { connection, e.getKey(), e.getValue() });
        }
      }
      return results;
    } finally {
      socket.close();
      if (reader != null) {
        reader.close();
      }
    }
  }

  private Map<String, String> getProps(String line) {
    int i1 = line.indexOf('(');
    int i2 = line.indexOf(')', i1);
    return getMap(line.substring(i1 + 1, i2));
  }

  private Map<String, String> getMap(String s) {
    Builder<String, String> builder = ImmutableMap.builder();
    for (String str : Splitter.on(',').splitToList(s)) {
      List<String> list = Splitter.on('=').splitToList(str);
      builder.put(list.get(0), list.get(1));
    }
    return builder.build();
  }

  private String getConnection(String line) {
    int indexOf = line.indexOf('(');
    return line.substring(0, indexOf).trim();
  }

}
