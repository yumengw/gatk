package org.broadinstitute.hellbender.tools.examples;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.utils.test.IntegrationTestSpec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

public class ExampleStreamingPythonExecutorIntegrationTest extends CommandLineProgramTest {

    private static final String TEST_DATA_DIRECTORY = publicTestDir + "org/broadinstitute/hellbender/engine/";
    private static final String TEST_OUTPUT_DIRECTORY = publicTestDir + "org/broadinstitute/hellbender/tools/examples/";

    @Test
    public void testExampleStreamingPythonExecutor() throws IOException {
        final IntegrationTestSpec testSpec = new IntegrationTestSpec(
                        " -I " + TEST_DATA_DIRECTORY + "reads_data_source_test1.bam" + " -O %s",
                Arrays.asList(TEST_OUTPUT_DIRECTORY + "expected_ExampleStreamingPythonExecutorIntegrationTest_output.txt")
        );
        testSpec.executeTest("testExampleStreamingPythonExecutor", this);
    }

}
