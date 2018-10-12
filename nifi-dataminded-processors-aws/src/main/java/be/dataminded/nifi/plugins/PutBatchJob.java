package be.dataminded.nifi.plugins;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.batch.AWSBatchClient;
import com.amazonaws.services.batch.model.*;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
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
            .addValidator(StandardValidators.createLongValidator(1, 256, true))
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

    public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder()
            .name("Command")
            .displayName("Command")
            .description("The command which the Docker container has to launch, basically the entry-point of the job. If it's not configured, take the default.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
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
                    Arrays.asList(IMAGE, IMAGE_TAG, JOB_QUEUE, CPU, MEMORY, COMMAND, JOB_NAME, JOB_ROLE, REGION,
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
        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            incomingFlowFile = session.create();
        }

        try {
            // Evaluate the variables
            final String image;
            final String imageTag;
            final String jobQueue;
            final Integer cpu;
            final Integer memoryInMb;
            final String command;
            final String jobName;
            final String jobRole;

            image = context.getProperty(IMAGE).evaluateAttributeExpressions(incomingFlowFile).getValue();
            imageTag = context.getProperty(IMAGE_TAG).evaluateAttributeExpressions(incomingFlowFile).getValue();
            jobQueue = context.getProperty(JOB_QUEUE).evaluateAttributeExpressions(incomingFlowFile).getValue();
            cpu = context.getProperty(CPU).evaluateAttributeExpressions(incomingFlowFile).asInteger();
            memoryInMb = context.getProperty(MEMORY).evaluateAttributeExpressions(incomingFlowFile).asDataSize(DataUnit.MB).intValue();
            command = context.getProperty(COMMAND).evaluateAttributeExpressions(incomingFlowFile).getValue();
            jobName = context.getProperty(JOB_NAME).evaluateAttributeExpressions(incomingFlowFile).getValue();
            jobRole = context.getProperty(JOB_ROLE).evaluateAttributeExpressions(incomingFlowFile).getValue();


            // Environment variables
            HashMap<String, String> environmentVariables = new HashMap<>();
            for (PropertyDescriptor entry : context.getProperties().keySet()) {
                if (entry.isDynamic()) {
                    if(!environmentVariables.containsKey(entry.getName())) {
                        environmentVariables.put(entry.getName(),
                                context.getProperty(entry).evaluateAttributeExpressions(incomingFlowFile).getValue());
                    }
                }
            }

            // Command
            List<String> commandList = new ArrayList<>();
            if(command != null && !command.isEmpty()) {
                commandList.add(command);
            }

            // Submit the job
            String jobId = submitJob(image + ":" + imageTag,
                    jobQueue,
                    cpu,
                    memoryInMb,
                    jobName,
                    jobRole,
                    environmentVariables,
                    commandList);

            session.putAttribute(incomingFlowFile, "batch.jobId", jobId);
            session.transfer (incomingFlowFile, REL_SUCCESS);

        } catch(final Exception e) {
            //TODO remove this since it's checked for nullity before ?
            if (incomingFlowFile == null) {
                getLogger().error("Failed to launch batch job", new Object[]{e});
            } else {
                getLogger().error("Failed to launch batch job", new Object[]{incomingFlowFile, e});
                incomingFlowFile = session.penalize(incomingFlowFile);
                session.transfer (incomingFlowFile, REL_FAILURE);
            }
        }
    }

    private String submitJob(String image, String jobQueue, Integer cpu, Integer memoryInMb,
                           String jobName, String jobRole, HashMap<String, String> environmentVariables,
                           Collection<String> command) {

        final AWSBatchClient client = getClient();

        Collection<KeyValuePair> environment = new ArrayList<>();
        for (String key : environmentVariables.keySet()) {
            environment.add(new KeyValuePair().withName(key).withValue(environmentVariables.get(key)));
        }

        ContainerOverrides overrides = new ContainerOverrides();
        overrides.setVcpus(cpu);
        overrides.setMemory(memoryInMb);
        overrides.setEnvironment(environment);

        if(command != null && !command.isEmpty()) {
            overrides.setCommand(command);
        }

        String jobDefinition = getJobDefinition(client, image, jobRole);
        if(jobDefinition.isEmpty()) {
            jobDefinition = registerJobDefinition(client, image, jobRole);
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put("TriggeredBy", "Nifi");

        SubmitJobRequest request = new SubmitJobRequest();
        request.setJobName(jobName);
        request.setJobQueue(jobQueue);
        request.setJobDefinition(jobDefinition);
        request.setParameters(parameters);
        request.setContainerOverrides(overrides);

        SubmitJobResult result = client.submitJob(request);
        getLogger().info("Job submitted with id " + result.getJobId());
        return result.getJobId();
    }

    /**
     * Find the most recent revision of the job definition named after the image + with the right inage & tag
     * @param image Take fully qualified image (host/org + name + tag e.g.: '724521671933.dkr.ecr.eu-west-1.amazonaws.com/ci-test:latest')
     * @param jobRole AWS Role that should be assumed by the task/container to get access to resources required
     * @return job definition arn
     */
    private String getJobDefinition(AWSBatchClient client, String image, String jobRole) {

        DescribeJobDefinitionsRequest request = new DescribeJobDefinitionsRequest()
                .withJobDefinitionName(extractImageNameFromIdentifier(image))
                .withStatus("ACTIVE")
                .withMaxResults(100);

        DescribeJobDefinitionsResult result = client.describeJobDefinitions(request);

        // for definition in definitions:
        //  Confirm the definition use right image + tag
        //  Confirm the job-role we want is attached this job defintion (or that no role is attached at all if wanted)
        List<JobDefinition> matches = new ArrayList<>();
        for(JobDefinition def : result.getJobDefinitions()) {
            if(def.getContainerProperties().getImage().equals(image) &&
                    def.getContainerProperties().getJobRoleArn().equals(jobRole)) {
                matches.add(def);
            }
        }

        if(matches.isEmpty()) {
            return "";
        } else {
           matches.sort(Comparator.comparing(JobDefinition::getRevision).reversed());
           return matches.get(0).getJobDefinitionArn();
        }
    }

    /**
     *   Extract the image-name from a full image-path
     *   @param image Take fully qualified image (host/org + name + tag e.g.: '724521671933.dkr.ecr.eu-west-1.amazonaws.com/ci-test:latest')
     *   @return image name without host or tag
     */
    private String extractImageNameFromIdentifier(String image) {
        String[] tokens = image.split("/");

        String imageName = tokens[tokens.length-1];
        if(imageName.contains(":")) {
            return imageName.split(":")[0];
        } else {
            return imageName;
        }
    }

    /**
     * Register a new job definition named after the image and mark it as "computer generated"
     * @param image Take fully qualified image (host/org + name + tag e.g.: '724521671933.dkr.ecr.eu-west-1.amazonaws.com/ci-test:latest')
     * @param jobRole AWS Role that should be assumed by the task/container to get access to resources required
     * @return job definition arn
     */
    private String registerJobDefinition(AWSBatchClient client, String image, String jobRole) {

        RegisterJobDefinitionRequest request = new RegisterJobDefinitionRequest()
                .withJobDefinitionName(extractImageNameFromIdentifier(image))
                .withType("container")
                .withContainerProperties(new ContainerProperties()
                        .withImage(image)
                        .withMemory(512)
                        .withVcpus(1)
                        .withJobRoleArn(jobRole));


        RegisterJobDefinitionResult result = client.registerJobDefinition(request);
        return result.getJobDefinitionArn();
    }
}