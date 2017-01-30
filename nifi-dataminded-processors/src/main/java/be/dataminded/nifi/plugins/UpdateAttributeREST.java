/*
 * Copyright 2017 Data Minded
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONObject;

import java.util.*;

@EventDriven
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"attributes", "modification", "update", "REST", "JSON", "dataminded"})
@CapabilityDescription("Updates the Attributes for a FlowFile based on an HTTP REST call")
public class UpdateAttributeREST extends AbstractProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> supportedDescriptors;

    private static final String ATT_ACCOUNT_NAME = "Account name";
    private static final String ATT_ACCOUNT_USERNAME = "Account username";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All flowfiles that are succesfully updated are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All flowfiles were an error occured are routed to this relationship")
            .build();

    public static final PropertyDescriptor REST_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Rest Endpoint")
            .required(true)
            .description("The HTTP REST endpoint")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        procRels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(procRels);

        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(REST_ENDPOINT);
        supportedDescriptors = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedDescriptors;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        // Get flowfile
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {return;}

        try {

            // Get filename of flowFile
            String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());

            // Invoke REST service with filename as parameter (For now parameter is just '1')
            String restEndpoint = processContext.getProperty(REST_ENDPOINT).getValue();
            JSONObject jsonResult = Unirest.get(restEndpoint)
                .header("accept", "application/json")
                .asJson()
                .getBody()
                .getObject();

            // Add attributes to flowfile based on REST call
            Map<String,String> newAttributes = new HashMap<>();
            newAttributes.put(ATT_ACCOUNT_NAME, jsonResult.getString("name"));
            newAttributes.put(ATT_ACCOUNT_USERNAME, jsonResult.getString("username"));
            FlowFile updatedFlowFile = processSession.putAllAttributes(flowFile,newAttributes);

            // Transfer flowfile to success state
            processSession.transfer(updatedFlowFile, REL_SUCCESS);

        } catch (UnirestException e) {
            processSession.transfer(flowFile, REL_FAILURE);
            throw new ProcessException(e);
        }
    }
}