package org.broadinstitute.hellbender.utils.runtime;

import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Facade to Runtime.exec() and java.lang.Process.  Handles running an interactive, keep-alive process and returns
 * stdout and stderr as strings.  Creates separate threads for reading stdout and stderr.
 */
public final class StreamingProcessController extends ProcessControllerBase<CapturedStreamOutputSnapshot> {
    private static final Logger logger = LogManager.getLogger(StreamingProcessController.class);

    private final ProcessSettings settings;
    private final String promptForSynchronization;

    private File fifoTempDir = null;
    private File fifoFile = null;

    // Timeout to prevent the GATK main tool thread from hanging.
    private final int timeOutCycleMillis = 1000;
    private final int timeOutCycles = 5;

    // keep an optional journal of all IPC; disabled/no-op by default
    private ProcessJournal processJournal = new ProcessJournal();

    private OutputStream processStdinStream; // stream via which we send remote commands

    /**
     * @param settings Settings to be used.
     */
    public StreamingProcessController(final ProcessSettings settings) {
        this(settings, null);
    }

    /**
     * Create a controller using the specified settings.
     *
     * @param settings Settings to be run.
     * @param promptForSynchronization Prompt to be used as a synchronization point/boundary for retrieving process
     *                                 output. Blocking calls that retrieve data will block until a prompt-terminated
     *                                 block is available.
     */
    public StreamingProcessController(final ProcessSettings settings, final String promptForSynchronization) {
        this(settings, promptForSynchronization, false);
    }

    /**
     * Create a controller using the specified settings.
     *
     * @param settings Settings to be run.
     * @param promptForSynchronization Prompt to be used as a synchronization point/boundary for blocking calls that
     *                                retrieve process output.
     * @param enableJournaling Turn on process I/O journaling. This records all inter-process communication to a file.
     *                         Journaling incurs a performance penalty, and should be used for debugging purposes only.
     */
    public StreamingProcessController(
            final ProcessSettings settings,
            final String promptForSynchronization,
            final boolean enableJournaling) {
        Utils.nonNull(settings, "Process settings are required");
        this.settings = settings;
        this.promptForSynchronization = promptForSynchronization;

        if (enableJournaling) {
            processJournal.enable(settings.getCommandString());
        }

        stdoutCapture = new OutputCapture(this, controllerThreadGroup, this.getClass().getSimpleName(), ProcessStream.Stdout, controllerId);
        stdoutCapture.start();
        if (!settings.isRedirectErrorStream()) {
            // don't waste a thread on stderr if its just going to be redirected to stdout
            stderrCapture = new OutputCapture(this, controllerThreadGroup, this.getClass().getSimpleName(), ProcessStream.Stderr, controllerId);
            stderrCapture.start();
        }
    }

    /**
     * Starts the remote process running based on the setting specified in the constructor.
     *
     * @return true if the process has been successfully started
     */
    public boolean start() {
        if (process != null) {
            throw new IllegalStateException("This controller is already running a process");
        }
        process = launchProcess(settings);
        registerOutputListeners();
        processStdinStream = getProcess().getOutputStream();

        return process.isAlive();
    }

    /**
     * Write some input to the remote process.
     *
     * @param line
     */
    public void writeProcessInput(final String line) {
        try {
            // get an output stream on the process' input
            processStdinStream.write(line.getBytes());
            processStdinStream.flush();
            processJournal.writeOutbound(line);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error writing (%s) to stdin on command", line), e);
        }
    }

    public int getDefaultTimeoutMillis() {
        return timeOutCycleMillis * timeOutCycleMillis;
    }

    /**
     * Non-blocking call to see if output is available. It is always safe to call getAccumulatedOutput immediately
     * after this returns true.
     * @return
     */
    public boolean isOutputAvailable() {
        synchronized (fromCapture) {
            return fromCapture.containsKey(ProcessStream.Stderr) || fromCapture.containsKey(ProcessStream.Stdout);
        }
    }

    /**
     * Blocking call to retrieve output from the remote process by prompt synchronization. This call collects
     * data from the remote process until it receives a prompt on either the stdout or stderr stream, or
     * the timeout is reached.
     *
     * Use isOutputAvailable to ensure the the thread will not block
     *
     * @return ProcessOutput containing a prompt terminated string in either std or stderr
     * @throws TimeoutException if the timeout is exceeded an no output is available
     */
    public ProcessOutput getProcessOutputByPrompt() throws TimeoutException {
        if (promptForSynchronization == null) {
            throw new IllegalStateException("A prompt must be specified in order to use prompt-synchronized I/O");
        }
        return getOutputSynchronizedBy(promptForSynchronization);
    }

    /**
     * Blocking call to retrieve output from the remote process by prompt synchronization. This call collects
     * data from the remote process until it receives a prompt on either the stdout or stderr stream or
     * the timeout is reached.
     *
     * @return ProcessOutput containing a newline terminated string in either std or stderr
     * @throws TimeoutException if the timeout is exceeded an no output is available
     */
    public ProcessOutput getProcessOutputByLine() throws TimeoutException {
        return getOutputSynchronizedBy("\n");
    }

    /**
     * Accumulate output from the target process until either a timeout is reached (no output for timeout duration),
     * or output containing the {@code #synchronizationString} is detected.
     *
     * @param synchronizationString string to synchronize on
     * @return ProcessOutput containing a {@code synchronizationString} terminated string in either std or stderr
     * @throws TimeoutException
     */
    private ProcessOutput getOutputSynchronizedBy(final String synchronizationString) throws TimeoutException {
        boolean gotPrompt = false;
        try (final ByteArrayOutputStream stdOutAccumulator = new ByteArrayOutputStream(CapturedStreamOutput.STREAM_BLOCK_TRANSFER_SIZE);
             final ByteArrayOutputStream stdErrAccumulator = new ByteArrayOutputStream(CapturedStreamOutput.STREAM_BLOCK_TRANSFER_SIZE))
        {
            while (gotPrompt == false) {
                final ProcessOutput processOutput = getProcessOutput();
                gotPrompt = scanForSynchronizationPoint(processOutput, synchronizationString);
                final StreamOutput stdOut = processOutput.getStdout();
                if (stdOut != null) {
                    stdOutAccumulator.write(stdOut.getBufferBytes());
                }
                final StreamOutput stderr = processOutput.getStderr();
                if (stderr != null) {
                    stdErrAccumulator.write(stderr.getBufferBytes());
                }
            }
            stdOutAccumulator.flush();
            final byte[] stdOutOutput = stdOutAccumulator.toByteArray();
            stdErrAccumulator.flush();
            final byte[] stdErrOutput = stdErrAccumulator.toByteArray();

            return new ProcessOutput(0,
                    stdOutOutput.length != 0 ?
                            new AggregateStreamOutput(stdOutOutput) :
                            null,
                    stdErrOutput.length != 0 ?
                            new AggregateStreamOutput(stdErrOutput) :
                            null);
        } catch (final IOException e) {
            throw new GATKException("Failure writing to process accumulator stream", e);
        }
    }

    /**
     * Wait for any output from the process; uses a timeout to prevent the calling thread from hanging.
     * Use isOutputAvailable for non-blocking check.
     */
    private ProcessOutput getProcessOutput() throws TimeoutException {
        StreamOutput stdout = null;
        StreamOutput stderr = null;
        registerOutputListeners();
        int cycles = 0;
        while (!destroyed && stdout == null && stderr == null) {
            synchronized (fromCapture) {
                if (fromCapture.containsKey(ProcessStream.Stderr)) {
                    stderr = fromCapture.remove(ProcessStream.Stderr);
                }
                if (fromCapture.containsKey(ProcessStream.Stdout)) {
                    stdout = fromCapture.remove(ProcessStream.Stdout);
                }
                if (stdout == null && stderr == null) {
                    try {
                        // Block waiting for something to happen or timeout. We do this in cycles
                        // in order to detect timeouts.
                        if (cycles > timeOutCycles) {
                            throw new TimeoutException("Timeout waiting for process output");
                        }
                        cycles++;
                        fromCapture.wait(timeOutCycleMillis, 0);
                    } catch (InterruptedException e) {
                        // Log the error, ignore the interrupt and wait patiently
                        // for the OutputCaptures to (via finally) return their
                        // stdout and stderr.
                        logger.error(e);
                    }
                }
            }
        }

        processJournal.writeInbound(stdout, stderr);

        return new ProcessOutput(0, stdout, stderr);
    }

    /**
     * Create a temporary FIFO suitable for sending output to the remote process. This only  lives for the lifetime
     * of the controller.
     *
     * @return a FIFO File
     */
    public File createFIFO() {
        fifoTempDir = Files.createTempDir();
        final String fifoTempFileName = String.format("%s/%s", fifoTempDir.getAbsolutePath(), "gatkStreamingController.fifo");

        final ProcessSettings mkFIFOSettings = new ProcessSettings(new String[]{"mkfifo", fifoTempFileName});
        mkFIFOSettings.getStdoutSettings().setBufferSize(-1);
        mkFIFOSettings.setRedirectErrorStream(true);

        // use a one-shot controller to create the FIFO
        final ProcessController mkFIFOController = new ProcessController();
        final ProcessOutput result = mkFIFOController.exec(mkFIFOSettings);
        int exitValue = result.getExitValue();

        fifoFile = new File(fifoTempFileName);
        if (exitValue != 0) {
            throw new RuntimeException(String.format("Failure creating FIFO (%s) got exit code (%d)", fifoTempFileName, exitValue));
        } else if (!fifoFile.exists()) {
            throw new RuntimeException(String.format("FIFO (%s) created but doesn't exist", fifoTempFileName));
        } else if (!fifoFile.canWrite()) {
            throw new RuntimeException(String.format("FIFO (%s) created isn't writable", fifoTempFileName));
        }

        return fifoFile;
    }

    /**
     * Close the FIFO; called on controller termination
     */
    private void closeFIFO() {
        if (fifoFile != null) {
            fifoFile.delete();
            fifoTempDir.delete();
        }
    }

    /**
     * Return true if either stdout or stderr ends with a synchronizationPoint
     */
    private boolean scanForSynchronizationPoint(final ProcessOutput processOutput, final String synchronizationString) {
        final StreamOutput stdOut = processOutput.getStdout();
        if (stdOut != null) {
            final String output = stdOut.getBufferString();
            if (output != null && output.endsWith(synchronizationString)){
                return true;
            }
        }
        final StreamOutput stdErr = processOutput.getStderr();
        if (stdErr != null) {
            final String output = stdErr.getBufferString();
            if (output != null && output.endsWith(synchronizationString)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Notify the output listener thread(s) to start listening for output
     */
    private void registerOutputListeners() {
        // Notify the background threads to start capturing.
        synchronized (toCapture) {
            toCapture.put(ProcessStream.Stdout,
                    new CapturedStreamOutputSnapshot(settings.getStdoutSettings(), process.getInputStream(), System.out));
            if (!settings.isRedirectErrorStream()) {
                // don't waste a thread for stderr if it's redirected to stdout
                toCapture.put(ProcessStream.Stderr,
                        new CapturedStreamOutputSnapshot(settings.getStderrSettings(), process.getErrorStream(), System.err));
            }
            toCapture.notifyAll();
        }
    }

    /**
     * Stops the process from running and tries to ensure the process is cleaned up properly.
     * NOTE: sub-processes started by process may be zombied with their parents set to pid 1.
     * NOTE: capture threads may block on read.
     */
    @Override
    protected void tryDestroy() {
        destroyed = true;
        synchronized (toCapture) {
            if (process != null) {
                // terminate the app by closing the process' INPUT stream
                IOUtils.closeQuietly(process.getOutputStream());
            }
            stdoutCapture.interrupt();
            if (stderrCapture != null) {
                stderrCapture.interrupt();
            }
            toCapture.notifyAll();
        }
    }

    /**
     * Terminate close the input stream. close the FIFO, and wait for the remote process to terminate
     * destroying it if necessary.
     *
     * @return true if the process was successfully destroyed
     */
    public boolean terminate() {
        closeFIFO();
        tryDestroy();
        boolean exited = false;
        try {
            exited = process.waitFor(10, TimeUnit.SECONDS);
            processJournal.close();
            if (!exited) {
                // we timed out waiting for the process to exit; it may be in a blocking call, so just force
                // it to shutdown
                process.destroy();
                process.waitFor();
            }
        } catch (InterruptedException e) {
            logger.error(String.format("Interrupt exception waiting for process (%s) to terminate", settings.getCommandString()));
        }
        return !process.isAlive();
    }

    // Stream output class used to aggregate output pulled fom the process stream while waiting for
    // a synchronization point.
    private class AggregateStreamOutput extends StreamOutput {
        final byte[] aggregateOutput;

        public AggregateStreamOutput(final byte[] aggregateOutput) {
            this.aggregateOutput = aggregateOutput;
        }

        @Override
        public byte[] getBufferBytes() {
            return aggregateOutput;
        }

        @Override
        public boolean isBufferTruncated() {
            return false;
        }
    };

    /**
     * Keep a journal of all inter-process communication. Expensive. For debugging only.
     */
    private class ProcessJournal {
        private File journalingFile = null;
        private FileWriter journalingFileWriter;

        public void enable(final String commandString) {
            final String journalingFileName = String.format("gatkStreamingProcessJournal-%d.txt", new Random().nextInt());
            journalingFile = new File(journalingFileName);
            try {
                journalingFileWriter = new FileWriter(journalingFile);
                journalingFileWriter.write("Initial process command line: ");
                journalingFileWriter.write(settings.getCommandString() + "\n\n");
            } catch (IOException e) {
                throw new GATKException(String.format("Error creating streaming process journaling file %s for command \"%s\"",
                        commandString,
                        journalingFile.getAbsolutePath()), e);
            }
            System.err.println(String.format("Enabling streaming process journaling file %s", journalingFileName));
        }

        public void writeOutbound(final String line) {
            try {
                if (journalingFileWriter != null) {
                    journalingFileWriter.write("Sending: [[[");
                    journalingFileWriter.write(line);
                    journalingFileWriter.write("]]]\n\n");
                    journalingFileWriter.flush();
                }
            } catch (IOException e) {
                throw new GATKException("Error writing to output to process journal", e);
            }
        }

        public void writeInbound(final StreamOutput stdout, final StreamOutput stderr) {

            if (journalingFileWriter != null) {
                try {
                    if (stdout != null) {
                        journalingFileWriter.write("Received from stdout: [[[");
                        journalingFileWriter.write(stdout.getBufferString());
                        journalingFileWriter.write("]]]\n\n");
                        journalingFileWriter.flush();
                    }
                    if (stderr != null) {
                        journalingFileWriter.write("Received from stderr: [[[");
                        journalingFileWriter.write(stderr.getBufferString());
                        journalingFileWriter.write("]]]\n\n");
                        journalingFileWriter.flush();
                    }
                } catch (IOException e) {
                    throw new GATKException(String.format("Error writing to journaling file %s", journalingFile.getAbsolutePath()), e);
                }
            }
        }

        public void close() {
            try {
                if (journalingFileWriter != null) {
                    journalingFileWriter.flush();
                    journalingFileWriter.close();
                }
            } catch (IOException e) {
                throw new GATKException(String.format("Error closing streaming process journaling file %s", journalingFile.getAbsolutePath()), e);
            }
        }
    }

}

