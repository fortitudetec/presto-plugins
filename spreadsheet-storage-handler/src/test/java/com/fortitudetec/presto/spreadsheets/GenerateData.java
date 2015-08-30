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
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;

public class GenerateData {

  public static void main(String[] args) throws FileNotFoundException {
    PrintWriter out = new PrintWriter(new File("test.csv"));
    Random random = new Random(1);
    for (int i = 0; i < 10000; i++) {
      printRow(out, random);
    }
    out.close();
  }

  private static void printRow(PrintWriter out, Random random) {
    for (int i = 0; i < 10; i++) {
      if (i != 0) {
        out.print(',');
      }
      out.print("\"" + Math.abs(random.nextLong()) + "\"");
    }
    out.println();
  }

}
