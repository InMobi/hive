/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.cli;

import jline.TerminalFactory;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapStatusOptionsProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(LlapStatusOptionsProcessor.class);

  private static final String LLAPSTATUS_CONSTANT = "llapstatus";

  enum OptionConstants {

    NAME("name", 'n', "LLAP cluster name"),
    HIVECONF("hiveconf", null, "Use value for given property. Overridden by explicit parameters", "property=value", 2),
    HELP("help", 'H', "Print help information"),;


    private final String longOpt;
    private final Character shortOpt;
    private final String description;
    private final String argName;
    private final int numArgs;

    OptionConstants(String longOpt, char shortOpt, String description) {
      this(longOpt, shortOpt, description, longOpt, 1);

    }

    OptionConstants(String longOpt, Character shortOpt, String description, String argName, int numArgs) {
      this.longOpt = longOpt;
      this.shortOpt = shortOpt;
      this.description = description;
      this.argName = argName;
      this.numArgs = numArgs;
    }

    public String getLongOpt() {
      return longOpt;
    }

    public Character getShortOpt() {
      return shortOpt;
    }

    public String getDescription() {
      return description;
    }

    public String getArgName() {
      return argName;
    }

    public int getNumArgs() {
      return numArgs;
    }
  }


  public static class LlapStatusOptions {
    private final String name;

    LlapStatusOptions(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  public LlapStatusOptionsProcessor() {

    for (OptionConstants optionConstant : OptionConstants.values()) {

      OptionBuilder optionBuilder = OptionBuilder.hasArgs(optionConstant.getNumArgs())
          .withArgName(optionConstant.getArgName()).withLongOpt(optionConstant.getLongOpt())
          .withDescription(optionConstant.getDescription());
      if (optionConstant.getShortOpt() == null) {
        options.addOption(optionBuilder.create());
      } else {
        options.addOption(optionBuilder.create(optionConstant.getShortOpt()));
      }
    }
  }

  public LlapStatusOptions processOptions(String[] args) throws ParseException {
    commandLine = new GnuParser().parse(options, args);
    if (commandLine.hasOption(OptionConstants.HELP.getShortOpt()) ||
        false == commandLine.hasOption(OptionConstants.NAME.getLongOpt())) {
      printUsage();
      return null;
    }

    String name = commandLine.getOptionValue(OptionConstants.NAME.getLongOpt());
    return new LlapStatusOptions(name);
  }


  private void printUsage() {
    HelpFormatter hf = new HelpFormatter();
    try {
      int width = hf.getWidth();
      int jlineWidth = TerminalFactory.get().getWidth();
      width = Math.min(160, Math.max(jlineWidth, width)); // Ignore potentially incorrect values
      hf.setWidth(width);
    } catch (Throwable t) { // Ignore
    }
    hf.printHelp(LLAPSTATUS_CONSTANT, options);
  }

}
