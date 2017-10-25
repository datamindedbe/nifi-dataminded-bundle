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


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.google.common.base.Throwables;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;


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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            String flowFileContent = readFlowFileContent(session, flowFile);

            // The MergeContent controller will be configured to append the JSON content with commas
            // We have to surround this list with square brackets to become a valid JSON Array
            JSONArray jsonArray = new JSONArray("[" + flowFileContent + "]");
            JSONObject firstElement = jsonArray.getJSONObject(0);

            final long totalTableCount = firstElement.getLong(context.getProperty(NAME_ELEMENT_TOTAL_COUNT).getValue());
            final long sumCount = takeSumOfColumn(context, jsonArray) ;
            final String tableName = firstElement.getString(TABLE_NAME);
            final String schemaName = firstElement.getString(SCHEMA_NAME);
            final String source = firstElement.getString(SOURCE_NAME);
            final String tenantName = firstElement.getString(TENANT_NAME);
            final String environment = context.getProperty(ENVIRONMENT).getValue();
            final String alarmPrefix = context.getProperty(NAME_PREFIX_ALARM).getValue();
            final List<Dimension> dimensions = makeDimensions(tableName, schemaName, source, tenantName, environment);

            putMetricData(totalTableCount, sumCount, tableName, dimensions);
            createAlarm(context, tableName, environment, alarmPrefix, dimensions);

            session.transfer(flowFile, REL_SUCCESS);
            logger.info("Successfully published CloudWatch metric for {}", new Object[]{flowFile});
        } catch (JSONException e) {
            logger.error("Something when trying to parse the JSON body of the flowFile: " + e.getMessage(), e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        } catch (Exception e) {
            logger.error("Failed to publish CloudWatch metric for {} due to: ", new Object[]{flowFile}, e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(
                Arrays.asList(NAME_ELEMENT_TOTAL_COUNT, NAME_ELEMENT_TO_SUM, ENVIRONMENT, NAME_PREFIX_ALARM,
                        ALARM_STATISTIC, ALARM_PERIOD, ALARM_EVALUATE_PERIODS, ALARM_COMPARISON_OPERATOR,
                        ALARM_ACTION, REGION, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE,
                        ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT)
        );
    }

    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        logger.debug("Creating client with aws credentials");
        return new AmazonCloudWatchClient(awsCredentials, clientConfiguration);
    }

    @Override
    protected AmazonCloudWatchClient createClient(ProcessContext processContext, AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        logger.debug("Creating client using aws credentials provider");
        return new AmazonCloudWatchClient(awsCredentialsProvider, clientConfiguration);
    }

    private void createAlarm(ProcessContext context, String tableName, String environment, String alarmPrefix, List<Dimension> dimensions) {
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
                .withThreshold(0d)
                //.withTreatMissingData("notBreaching") // aws java SDK has to be upgraded for this
                .withAlarmDescription("The daily Count Alarm for table " + tableName)
                .withActionsEnabled(true)
                .withAlarmActions(alarmAction);

        getClient().putMetricAlarm(putMetricAlarmRequest);
    }

    private void putMetricData(long totalTableCount, long sumCount, String tableName, List<Dimension> dimensions) {
        // first metric: this is the total count of the records that were exported
        MetricDatum count = new MetricDatum()
                .withMetricName(String.format("COUNT_%s", tableName))
                .withValue((double )sumCount)
                .withUnit("Count")
                .withDimensions(dimensions);

        // second metric: this is the difference between the records exported
        // and the total amount of records counted in the DB, should always be 0 !!!
        // we take a margin into account because we can't be sure there won't be any deletes
        // between counting and executing the queries
        MetricDatum diff = new MetricDatum()
                .withMetricName(String.format("DIFF_%s", tableName))
                .withValue((double) Math.round((Math.abs(totalTableCount - sumCount) / totalTableCount) * 1000))
                .withUnit("Count")
                .withDimensions(dimensions);

        getClient().putMetricData(new PutMetricDataRequest().withMetricData(count, diff).withNamespace("NIFI"));
    }

    private long takeSumOfColumn(ProcessContext context, JSONArray json) {
        long sum = 0L;
        for (Object record : json) {
            JSONObject jsonObject = (JSONObject) record;
            sum += jsonObject.getLong(context.getProperty(NAME_ELEMENT_TO_SUM).getValue());
        }
        return sum;
    }

    private List<Dimension> makeDimensions(String tableName, String schemaName, String source, String tenantName, String environment) {
        List<Dimension> dimensions = new ArrayList<>();
        dimensions.add(new Dimension().withName("tableName").withValue(tableName));
        dimensions.add(new Dimension().withName("tenantName").withValue(tenantName));
        dimensions.add(new Dimension().withName("sourceName").withValue(source));
        dimensions.add(new Dimension().withName("schemaName").withValue(schemaName));
        dimensions.add(new Dimension().withName("environment").withValue(environment));
        return dimensions;
    }

    private String readFlowFileContent(ProcessSession session, FlowFile flowFile) {
        try (InputStream inputStream = session.read(flowFile)) {
            StringWriter writer = new StringWriter();
            IOUtils.copy(inputStream, writer, "UTF-8");
            return writer.toString();
        } catch (IOException e) {
            logger.error("Something went wrong trying to read the content of the flow-file: {}", new Object[]{flowFile}, e);
            throw Throwables.propagate(e);
        }
    }
}