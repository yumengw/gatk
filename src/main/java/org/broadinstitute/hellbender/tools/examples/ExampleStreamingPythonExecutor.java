package org.broadinstitute.hellbender.tools.examples;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ExampleProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.python.StreamingPythonScriptExecutor;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.*;
import java.util.Collections;

/**
 * Example ReadWalker program that uses a Python streaming executor to stream summary data from a BAM
 * input file to a Python Processes through a FIFO. The Python process in turn writes the data to the tools'
 * output file:
 *
 * <ol>
 * <li>Opens a FIFO for writing.</li>
 * <li>Writes a string of attributes for each read to the FIFO.</li>
 * <li>Uses Python to read each attribute line from the FIFO, and write it to the output file.</li>
 * <li>NOTE: For purposes of illustration, this tool writes summary data for each read, flushes the FIFO after,
 * and instructs Python to synchronously transfer each item from the read end of the FIFO to the output file
 * immediately after its written. In reality, this is inefficient. A production tool would aggregate
 * summary data and transfer it in batches, only writing, flushing, and reading in batches, rather than
 * on every apply call during traversal.</li>
 * </ol>
 */
@CommandLineProgramProperties(
        summary = "Example/toy program that uses a Python script.",
        oneLineSummary = "Example/toy program that uses a Python script.",
        programGroup = ExampleProgramGroup.class,
        omitFromCommandLine = true
)
public class ExampleStreamingPythonExecutor extends ReadWalker {
    private final static String NL = String.format("%n");

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            doc = "Output file")
    private File outputFile; // output file produced by Python code

    // Create the Python executor. This doesn't actually start the Python process, but verifies that
    // the requestedPython executable exists and can be located.
    final StreamingPythonScriptExecutor pythonExecutor = new StreamingPythonScriptExecutor(true);

    private FileWriter fifoWriter;

    @Override
    public void onTraversalStart() {

        // Start the Python process, and get a FIFO from the executor to use to send data to Python. The lifetime
        // of the FIFO is managed by the executor; the FIFO will be destroyed when the executor is terminated.
        pythonExecutor.start(Collections.emptyList());
        final File fifoFile = pythonExecutor.getFIFOForWrite();

        // Open the FIFO for writing. Opening a FIFO for read or write will block until there is reader/writer
        // on the other end, so before we open it, send an ASYNCHRONOUS command, that doesn't wait for a
        // response, to the Python process to open the FIFO for reading. The Python process will then block until
        // we open the FIFO. We can then call getAccumulatedOutput.
        pythonExecutor.sendAsynchronousCommand(String.format("fifoFile = open('%s', 'r')" + NL, fifoFile.getAbsolutePath()));
        try {
            fifoWriter = new FileWriter(fifoFile);
        } catch ( IOException e ) {
            throw new GATKException("Failure opening FIFO for writing", e);
        }

        // Also, ask Python to open our output file, where it will write the contents of everything it reads
        // from the FIFO. <code sendSynchronousCommand/>
        pythonExecutor.sendSynchronousCommand(String.format("tempFile = open('%s', 'w')" + NL, outputFile.getAbsolutePath()));
    }

    @Override
    public void apply(GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        // Extract data from the read and send to Python
        transferReadSummaryToPython(read);
    }

    private void transferReadSummaryToPython(final GATKRead read) {
        try {
            // write summary data to the FIFO
            fifoWriter.write(String.format(
                    "Read at %s:%d-%d:\n%s\n",
                    read.getContig(), read.getStart(), read.getEnd(), read.getBasesString()));

            // NOTE: flush the stream since we're going to immediately issue a Python command to read it.
            // Failure to flush the stream could result in Python blocking on the read.
            fifoWriter.flush();
        }
        catch ( IOException e ) {
            throw new GATKException("Failure writing to FIFO", e);
        }

        // Send a synchronous command (wait for the response prompt) to Python to consume the line written to the FIFO
        pythonExecutor.sendSynchronousCommand("tempFile.write(fifoFile.readline())" + NL);
    }

    @Override
    public void closeTool() {
        // Send synchronous commands to Python to close the temp file and the FIFO file
        pythonExecutor.sendSynchronousCommand("tempFile.close()" + NL);
        pythonExecutor.sendSynchronousCommand("fifoFile.close()" + NL);

        // Terminate the Python executor in closeTool, since that always gets called.
        pythonExecutor.terminate();
    }
}
