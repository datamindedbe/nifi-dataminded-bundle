package be.dataminded.nifi.plugins;
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


import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.cloudwatch.model.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import org.json.JSONArray;
import org.json.JSONObject;


@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "cloudwatch", "metrics", "put", "publish", "alarm", "dataminded", "counts", "JSON"})
@CapabilityDescription("Publishes count metrics and alarms to Amazon CloudWatch, the names and dimensions are chosen by the content of the JSON body in the flow file.")
public class PutCloudWatchCountMetricAndAlarm extends AbstractAWSCredentialsProviderProcessor<AmazonCloudWatchClient> {

    // the default names of the JSON parameters which we use to define the names and dimensions
    private static final String TABLE_NAME = "table.name";
    private static final String SCHEMA_NAME = "schema.name";
    private static final String SOURCE_NAME = "source.name";
    private static final String TENANT_NAME = "tenant.name";

    private final ComponentLog logger = getLogger();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final PropertyDescriptor NAME_ELEMENT_TOTAL_COUNT = new PropertyDescriptor.Builder()
            .name("TotalCountElementName")
            .displayName("TotalCountElementName")
            .description("The name of the JSON element where we have to look for the total count and publish as an alarm")
            .required(true)
            .defaultValue("generateoracletablefetch.total.row.count")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor NAME_ELEMENT_TO_SUM = new PropertyDescriptor.Builder()
            .name("SumElementName")
            .displayName("SumElementName")
            .description("The name of the JSON element which we have to sum and publish as metric")
            .required(true)
            .defaultValue("executesql.row.count")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor ENVIRONMENT = new PropertyDescriptor.Builder()
            .name("Environment")
            .displayName("Environment")
            .description("The environment of this Nifi instance, this will be added to the dimension of the metric and the name of the alarm")
            .required(true)
            .defaultValue("ACC")
            .allowableValues("ACC", "PRD")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor NAME_PREFIX_ALARM = new PropertyDescriptor.Builder()
            .name("AlarmPrefixName")
            .displayName("AlarmPrefixName")
            .description("The prefix that will be used for the alarm name")
            .required(true)
            .defaultValue("INGRESS")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor ALARM_STATISTIC = new PropertyDescriptor.Builder()
            .name("AlarmStatistic")
            .displayName("AlarmStatistic")
            .description("The statistic that will be used by the alarm")
            .required(true)
            .defaultValue("Sum")
            .allowableValues("Sum", "Minimum", "Maximum", "Average", "SampleCount")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor ALARM_PERIOD = new PropertyDescriptor.Builder()
            .name("AlarmPeriod")
            .displayName("AlarmPeriod")
            .description("The period over which the alarm will look to validate, in seconds")
            .required(true)
            .defaultValue("43200")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALARM_EVALUATE_PERIODS = new PropertyDescriptor.Builder()
            .name("EvaluationPeriods")
            .displayName("EvaluationPeriods")
            .description("The number of periods over which data is compared to the specified threshold.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ALARM_COMPARISON_OPERATOR = new PropertyDescriptor.Builder()
            .name("AlarmComparisonOperator")
            .displayName("AlarmComparisonOperator")
            .description("The arithmetic operation to use when comparing the specified statistic and threshold. ")
            .required(true)
            .defaultValue("LessThanThreshold")
            .allowableValues("GreaterThanOrEqualToThreshold", "GreaterThanThreshold", "LessThanThreshold", "LessThanOrEqualToThreshold")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final PropertyDescriptor ALARM_ACTION = new PropertyDescriptor.Builder()
            .name("AlarmAction")
            .displayName("AlarmAction")
            .description("The action to execute when this alarm transitions to the ALARM state from any other state.")
            .required(true)
            .defaultValue("arn:aws:sns:eu-west-1:561010060099:NIFI-ACC-METRIC-ALARM")
            .addValidator(new StandardValidators.StringLengthValidator(1, 255))
            .build();

    public static final List<PropertyDescriptor> properties =
            Collections.unmodifiableList(
                    Arrays.asList(NAME_ELEMENT_TOTAL_COUNT, NAME_ELEMENT_TO_SUM, ENVIRONMENT, NAME_PREFIX_ALARM,
                            ALARM_STATISTIC, ALARM_PERIOD, ALARM_EVALUATE_PERIODS, ALARM_COMPARISON_OPERATOR,
                            ALARM_ACTION, REGION, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE,
                            ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT)
            );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Create client using aws credentials provider. This is the preferred way for creating clients
     */

    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        getLogger().info("Creating client using aws credentials provider");
        return new AmazonCloudWatchClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        getLogger().debug("Creating client with aws credentials");
        return new AmazonCloudWatchClient(awsCredentials, clientConfiguration);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        long totalTableCount = 0;
        long sumCount = 0;
        String tableName = "";
        String schemaName = "";
        String source = "";
        String tenantName = "";

        try {

            InputStream inputStream = session.read(flowFile);
            StringWriter writer = new StringWriter();
            IOUtils.copy(inputStream, writer, "UTF-8");
            String flowFileContent = writer.toString();

            // The MergeContent controller will be configured to append the JSON content with commas
            // We have to surround this list with square brackets to become a valid JSON Array
            String jsonContent = "[" + flowFileContent + "]";

            JSONArray jsonArray = new JSONArray(jsonContent);

            Iterator iterator = jsonArray.iterator();

            ArrayList<Long> counts = new ArrayList<>();

            while (iterator.hasNext()) {
                JSONObject o = (JSONObject) iterator.next();
                counts.add(o.getLong(context.getProperty(NAME_ELEMENT_TO_SUM).getValue()));
            }
            sumCount = counts.stream().mapToLong(Long::longValue).sum();

            JSONObject firstElement = (JSONObject) jsonArray.get(0);
            totalTableCount = firstElement.getLong(context.getProperty(NAME_ELEMENT_TOTAL_COUNT).getValue());
            tableName = firstElement.getString(TABLE_NAME);
            schemaName = firstElement.getString(SCHEMA_NAME);
            source = firstElement.getString(SOURCE_NAME);
            tenantName = firstElement.getString(TENANT_NAME);


        } catch (IOException e) {
            logger.error("Something went wrong when trying to read the flowFile body: " + e.getMessage());
        } catch (org.json.JSONException e) {
            logger.error("Something when trying to parse the JSON body of the flowFile: " + e.getMessage());
        } catch (Exception e) {
            logger.error("something else went wrong in body processing of this FlowFile: " + e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

        try {

            String environment = context.getProperty(ENVIRONMENT).getValue();
            String alarmPrefix = context.getProperty(NAME_PREFIX_ALARM).getValue();

            Map<String, Long> metrics = new HashMap<>();
            // first metric: this is the total count of the records that were exported
            metrics.put("COUNT_", sumCount);
            // second metric: this is the difference between the records exported
            // and the total amount of records counted in the DB, should always be 0 !!!
            metrics.put("DIFF_", Math.abs(totalTableCount-sumCount));

            ArrayList<Dimension> dimensions = new ArrayList<>();
            dimensions.add(new Dimension().withName("tableName").withValue(tableName));
            dimensions.add(new Dimension().withName("tenantName").withValue(tenantName));
            dimensions.add(new Dimension().withName("sourceName").withValue(source));
            dimensions.add(new Dimension().withName("schemaName").withValue(schemaName));
            dimensions.add(new Dimension().withName("environment").withValue(environment));

            for(Map.Entry<String, Long> metric : metrics.entrySet()) {
                MetricDatum datum = new MetricDatum();
                datum.setMetricName(metric.getKey() + tableName);
                datum.setValue((double) metric.getValue());
                datum.setUnit("Count");
                datum.setDimensions(dimensions);

                final PutMetricDataRequest metricDataRequest = new PutMetricDataRequest()
                        .withNamespace("NIFI")
                        .withMetricData(datum);

                putMetricData(metricDataRequest);
            }

            // the alarm we create is a static one that will check if the diff is zero
            String comparisonOperator = context.getProperty(ALARM_COMPARISON_OPERATOR).getValue();
            String alarmStatistic = context.getProperty(ALARM_STATISTIC).getValue();
            String alarmPeriod = context.getProperty(ALARM_PERIOD).getValue();
            String alarmEvaluatePeriods = context.getProperty(ALARM_EVALUATE_PERIODS).getValue();
            String alarmAction = context.getProperty(ALARM_ACTION).getValue();

            PutMetricAlarmRequest putMetricAlarmRequest = new PutMetricAlarmRequest()
                    .withMetricName("DIFF_" + tableName)
                    .withAlarmName(environment + "_" + alarmPrefix + "_" + "DIFF_" + tableName)
                    .withDimensions(dimensions)
                    .withComparisonOperator(comparisonOperator)
                    .withNamespace("NIFI")
                    .withStatistic(alarmStatistic)
                    .withPeriod(Integer.parseInt(alarmPeriod))
                    .withEvaluationPeriods(Integer.parseInt(alarmEvaluatePeriods))
                    .withThreshold((double) 0)
                    //.withTreatMissingData("notBreaching") // aws java SDK has to be upgraded for this
                    .withAlarmDescription("The daily Count Alarm for table " + tableName)
                    .withActionsEnabled(true)
                    .withAlarmActions(alarmAction);
            putAlarmData(putMetricAlarmRequest);

            session.transfer(flowFile, REL_SUCCESS);
            getLogger().info("Successfully published cloudwatch metric for {}", new Object[]{flowFile});
        } catch (final Exception e) {
            getLogger().error("Failed to publish cloudwatch metric for {} due to {}", new Object[]{flowFile, e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    protected PutMetricDataResult putMetricData(PutMetricDataRequest metricDataRequest) throws AmazonClientException {
        final AmazonCloudWatchClient client = getClient();
        final PutMetricDataResult result = client.putMetricData(metricDataRequest);
        return result;
    }

    public PutMetricAlarmResult putAlarmData(PutMetricAlarmRequest metricAlarmRequest) {
        final AmazonCloudWatchClient client = getClient();
        final PutMetricAlarmResult result = client.putMetricAlarm(metricAlarmRequest);
        return result;
    }
}