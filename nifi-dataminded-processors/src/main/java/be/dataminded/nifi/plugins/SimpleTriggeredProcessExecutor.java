/*
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
package be.dataminded.nifi.plugins;

import be.dataminded.nifi.plugins.util.ArgumentUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.zeroturnaround.exec.ProcessExecutor;

import java.util.*;
import java.util.concurrent.TimeoutException;

@Tags({"command", "process", "source", "external", "invoke", "script", "restricted", "dataminded"})
@CapabilityDescription("Runs an operating system command specified by the user and writes the output of that command to a FlowFile.")
@DynamicProperty(name = "An environment variable name", value = "An environment variable value", description = "These environment variables are passed to the process spawned by this Processor")
@Restricted("Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
public class SimpleTriggeredProcessExecutor extends AbstractProcessor {

    private static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
            .name("Command")
            .description(
                    "Specifies the command to be executed; if just the name of an executable is provided, it must be in the user's environment PATH.")
            .required(true)
            .expressionLanguageSupported(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor COMMAND_ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Command Arguments")
            .description(
                    "The arguments to supply to the executable delimited by white space. White space can be escaped by enclosing it in double-quotes.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successful FlowFiles are routed to this relationship")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed FlowFiles are routed to this relationship")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(SUCCESS, FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(COMMAND, COMMAND_ARGUMENTS);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Sets the environment variable '" + propertyDescriptorName + "' for the process' environment")
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        // get or create flow file
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        StringBuffer exitcode = new StringBuffer();
        final List<String> commandStrings = createCommandStrings(context, flowFile.getAttributes());
        flowFile = session.write(flowFile, out -> {
            try {
                exitcode.append(new ProcessExecutor(commandStrings).redirectOutput(out).redirectError(out).destroyOnExit().execute().getExitValue());
            } catch (InterruptedException e) {
                logger.warn("The external process was interrupted", e);
            } catch (TimeoutException e) {
                logger.warn("The external process  timed out", e);
            }
        });

        if(exitcode.toString().equals("0")) {
            session.transfer(flowFile, SUCCESS);
        } else {
            session.transfer(flowFile, FAILURE);
        }
    }

    private List<String> createCommandStrings(final ProcessContext context, final Map<String, String> attributes) {
        final String command = context.getProperty(COMMAND).getValue();
        final String arguments = context.getProperty(COMMAND_ARGUMENTS).isSet()
                ? context.getProperty(COMMAND_ARGUMENTS).evaluateAttributeExpressions(attributes).getValue()
                : null;
        final List<String> args = ArgumentUtils.splitArgs(arguments, ' ');

        final List<String> commandStrings = new ArrayList<>(args.size() + 1);
        commandStrings.add(command);
        commandStrings.addAll(args);
        return commandStrings;
    }
}