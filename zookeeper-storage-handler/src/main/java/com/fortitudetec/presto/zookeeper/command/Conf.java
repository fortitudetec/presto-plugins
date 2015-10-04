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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Conf extends BaseCommand {

  public static void main(String[] args) throws IOException {
    String host = args[0];
    int port = Integer.parseInt(args[1]);
    Conf conf = new Conf(host, port);
    System.out.println(conf.execute());
  }

  private final String _host;
  private final int _port;

  public Conf(String host, int port) throws IOException {
    _host = host;
    _port = port;
  }

  @Override
  public String command() {
    return "conf";
  }

  @Override
  public String[] names() {
    return new String[] { "name", "value" };
  }

  @Override
  public List<String[]> execute() throws IOException {
    Socket socket = new Socket(_host, _port);
    InputStream inputStream = null;
    try {
      OutputStream outstream = socket.getOutputStream();
      outstream.write(command().getBytes());
      outstream.flush();
      // this replicates NC - close the output stream before reading
      socket.shutdownOutput();
      inputStream = socket.getInputStream();
      Properties properties = new Properties();
      properties.load(inputStream);
      List<String[]> results = new ArrayList<String[]>();
      Set<Object> keySet = properties.keySet();
      for (Object o : keySet) {
        Object v = properties.get(o);
        results.add(new String[] { o.toString(), v.toString() });
      }
      return results;
    } finally {
      socket.close();
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

}
