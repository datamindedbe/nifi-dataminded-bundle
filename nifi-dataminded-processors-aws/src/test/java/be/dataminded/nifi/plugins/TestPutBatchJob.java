package be.dataminded.nifi.plugins;

import org.apache.nifi.expression.ExpressionLanguageCompiler;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockVariableRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.file.FileUtils;
import org.junit.*;

import java.util.HashMap;

public class TestPutBatchJob {

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(new PutBatchJob());

        // TODO: set credentials
        runner.setProperty(PutBatchJob.ACCESS_KEY, "");
        runner.setProperty(PutBatchJob.SECRET_KEY, "");

        // Other properties
        runner.setProperty(PutBatchJob.IMAGE, "561010060099.dkr.ecr.eu-west-1.amazonaws.com/persgroep/spark-standalone");
        runner.setProperty(PutBatchJob.IMAGE_TAG, "latest");
        runner.setProperty(PutBatchJob.JOB_QUEUE, "acc-high-priority");
        runner.setProperty(PutBatchJob.CPU, "4");
        runner.setProperty(PutBatchJob.MEMORY, "8192 MB");
        runner.setProperty(PutBatchJob.COMMAND, "spark-submit --packages org.apache.spark:spark-hive_2.11:2.3.0 --master local[*] --driver-memory 6g --conf spark.hadoop.fs.s3a.connection.maximum=100 --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --class be.persgroep.datalake.google.CleanDfpLogs s3a://dpg-datalake-artifacts/snapshot/be/persgroep/datalake/google_2.11/0.0.2-1620-g2094339/google_2.11-0.0.2-1620-g2094339-assembly.jar s3a://dpg-datalake-acc/raw/dpp/dfplogs/networkactiveviews s3a://dpg-datalake-acc/clean/dpp/dfplogs/networkactiveviews 1529485200000");
        runner.setProperty(PutBatchJob.JOB_NAME, "clean-dpp-networkactiveviews");
        runner.setProperty(PutBatchJob.JOB_ROLE, "acc_aws_batch_job_role");
    }

    @Test
    public void testRunningBatchWithoutIncomingFlowFile() {
        runner.setIncomingConnection(false);
        runner.run();
        runner.assertTransferCount(PutBatchJob.REL_SUCCESS, 1);
        runner.assertTransferCount(PutBatchJob.REL_FAILURE, 0);
    }
}
