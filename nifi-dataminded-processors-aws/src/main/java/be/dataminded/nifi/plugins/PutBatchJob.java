package be.dataminded.nifi.plugins;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.batch.AWSBatchClient;
import com.amazonaws.services.batch.model.ContainerOverrides;
import com.amazonaws.services.batch.model.KeyValuePair;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import java.util.*;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@Tags({"amazon", "aws", "batch", "put", "publish", "dataminded"})
@DynamicProperty(name = "Environment variable", value = "The value to set it to", supportsExpressionLanguage = true,
        description = "Environment variables to pass to the container")
@CapabilityDescription("Publishes a job on an AWS Batch Queue.")
public class PutBatchJob extends AbstractAWSCredentialsProviderProcessor<AWSBatchClient> {

    private final ComponentLog logger = getLogger();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final PropertyDescriptor IMAGE = new PropertyDescriptor.Builder()
            .name("Image")
            .displayName("Image")
            .description("The fully qualified docker image")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IMAGE_TAG = new PropertyDescriptor.Builder()
            .name("ImageTag")
            .displayName("ImageTag")
            .description("The tag of the image to pull from repo (default: latest)")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("latest")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor JOB_QUEUE = new PropertyDescriptor.Builder()
            .name("JobQueue")
            .displayName("JobQueue")
            .description("The name of the queue to submit the job (should be already existing in AWS Batch)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CPU = new PropertyDescriptor.Builder()
            .name("CPU")
            .displayName("CPU")
            .description("vCPU's to allocate the job (min. 1 vCPU)")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MEMORY = new PropertyDescriptor.Builder()
            .name("Memory")
            .displayName("Memory")
            .description("Memory/RAM to allocate the job in GBs (min. 4 MB)")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("1GB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor.Builder()
            .name("JobName")
            .displayName("JobName")
            .description("Name of the particular job-run. Only letters (uppercase & lowercase), hyphens & underscore is allowed")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor JOB_ROLE = new PropertyDescriptor.Builder()
            .name("JobRole")
            .displayName("JobRole")
            .description("AWS Role that should be assumed by the task/container to get access to resources required")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public static final List<PropertyDescriptor> properties =
            Collections.unmodifiableList(
                    Arrays.asList(IMAGE, IMAGE_TAG, JOB_QUEUE, CPU, MEMORY, JOB_NAME, JOB_ROLE, REGION,
                            AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, SSL_CONTEXT_SERVICE,
                            ENDPOINT_OVERRIDE, PROXY_HOST, PROXY_HOST_PORT)
            );

    /**
     * Create client using aws credentials provider. This is the preferred way for creating clients
     */

    @Override
    protected AWSBatchClient createClient(ProcessContext processContext, AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        getLogger().info("Creating client using aws credentials provider");
        return new AWSBatchClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Create client using AWSCredentials
     *
     * @deprecated use {@link #createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)} instead
     */
    @Override
    protected AWSBatchClient createClient(ProcessContext processContext, AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        getLogger().debug("Creating client with aws credentials");
        return new AWSBatchClient(awsCredentials, clientConfiguration);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        try {
            // Evaluate the variables
            final String image;
            final String imageTag;
            final String jobQueue;
            final Integer cpu;
            final Integer memoryInMb;
            final String jobName;
            final String jobRole;

            if (flowFile == null) {
                image = context.getProperty(IMAGE).evaluateAttributeExpressions().getValue();
                imageTag = context.getProperty(IMAGE_TAG).evaluateAttributeExpressions().getValue();
                jobQueue = context.getProperty(JOB_QUEUE).evaluateAttributeExpressions().getValue();
                cpu = context.getProperty(CPU).evaluateAttributeExpressions().asInteger();
                memoryInMb = context.getProperty(MEMORY).evaluateAttributeExpressions().asDataSize(DataUnit.MB).intValue();
                jobName = context.getProperty(JOB_NAME).evaluateAttributeExpressions().getValue();
                jobRole = context.getProperty(JOB_ROLE).evaluateAttributeExpressions().getValue();
            } else {
                image = context.getProperty(IMAGE).evaluateAttributeExpressions(flowFile).getValue();
                imageTag = context.getProperty(IMAGE_TAG).evaluateAttributeExpressions(flowFile).getValue();
                jobQueue = context.getProperty(JOB_QUEUE).evaluateAttributeExpressions(flowFile).getValue();
                cpu = context.getProperty(CPU).evaluateAttributeExpressions(flowFile).asInteger();
                memoryInMb = context.getProperty(MEMORY).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.MB).intValue();
                jobName = context.getProperty(JOB_NAME).evaluateAttributeExpressions(flowFile).getValue();
                jobRole = context.getProperty(JOB_ROLE).evaluateAttributeExpressions(flowFile).getValue();
            }

            HashMap<String, String> environmentVariables = new HashMap<>();
            for (PropertyDescriptor entry : context.getProperties().keySet()) {
                if (entry.isDynamic()) {
                    if(!environmentVariables.containsKey(entry.getName())) {
                        environmentVariables.put(entry.getName(),
                                context.getProperty(entry).evaluateAttributeExpressions(flowFile).getValue());
                    }
                }
            }

            // Submit the job
            submitJob(image + ":" + imageTag, jobQueue, cpu, memoryInMb, jobName, jobRole, environmentVariables);

        } catch(final Exception e) {
            if (flowFile == null) {
                logger.error("Failed to launch batch job", new Object[]{e});
            } else {
                logger.error("Failed to launch batch job", new Object[]{flowFile, e});
                flowFile = session.penalize(flowFile);
                session.transfer (flowFile, REL_FAILURE);
            }
        }
    }

    private void submitJob(String image, String jobQueue, Integer cpu, Integer memoryInMb,
                           String jobName, String jobRole, HashMap<String, String> environmentVariables) {

        final AWSBatchClient client = getClient();

        Collection<KeyValuePair> environment = new ArrayList<>();
        for (String key : environmentVariables.keySet()) {
            environment.add(new KeyValuePair().withName(key).withValue(environmentVariables.get(key)));
        }

        ContainerOverrides overrides = new ContainerOverrides();
        overrides.setVcpus(cpu);
        overrides.setMemory(memoryInMb);
        overrides.setEnvironment(environment);

        String jobDefinition = getJobDefinition(client, image, jobRole);
        if(jobDefinition.isEmpty()) {
            jobDefinition = registerJobDefinition(client, image, jobRole);
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put("TriggeredFromNifi", "True");

        SubmitJobRequest request = new SubmitJobRequest();
        request.setJobName(jobName);
        request.setJobQueue(jobQueue);
        request.setJobDefinition(jobDefinition);
        request.setParameters(parameters);
        request.setContainerOverrides(overrides);

        SubmitJobResult result = client.submitJob(request);
        logger.info("Job submitted with id " + result.getJobId());
    }

    private String registerJobDefinition(AWSBatchClient client, String image, String jobRole) {


        return "";
    }

    private String getJobDefinition(AWSBatchClient client, String image, String jobRole) {


        return "";
    }
}
