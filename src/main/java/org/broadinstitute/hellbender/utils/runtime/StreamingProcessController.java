package org.broadinstitute.hellbender.utils.runtime;

import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;

import java.io.*;
import java.util.Random;
import java.util.concurrent.*;

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

    // Timeout used when retrieving output from the remote process to prevent the GATK main tool thread from\
    // excessive blocking. {@link #isOutputAvailable} can be used to to check for output before making a
    // blocking call in order to avoid exceeding timeouts.
    private final int timeOutMillis = 5000;

    // keep an optional journal of all IPC; disabled/no-op by default
    private ProcessJournal processJournal = new ProcessJournal();

    private OutputStream processStdinStream; // stream to which we send remote commands

    /**
     * @param settings Settings to be used.
     */
    public StreamingProcessController(final ProcessSettings settings) {
        this(settings, null);
    }

    /**
     * Create a controller using the specified settings.
     *
     * @param settings                 Settings to be run.
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
     * @param settings                 Settings to be run.
     * @param promptForSynchronization Prompt to be used as a synchronization point/boundary for blocking calls that
     *                                 retrieve process output.
     * @param enableJournaling         Turn on process I/O journaling. This records all inter-process communication to a file.
     *                                 Journaling incurs a performance penalty, and should be used for debugging purposes only.
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
        startListeners();
        processStdinStream = getProcess().getOutputStream();

        return process.isAlive();
    }

    /**
     * Write some input to the remote process, without waiting for output.
     *
     * @param line data to be written to the remote process
     */
    public void writeProcessInput(final String line) {
        try {
            // Its possible that we already have completed futures from output from a previous command that
            // weren't consumed before we found the prompt at the last synchronization point. If so, drop them
            // before we issue a new command (since retaining them could confound synchronization on output from
            // this command), and warn.
            if (stdErrFuture != null && stdErrFuture.isDone()) {
                //logger.warn("Dropping stale stderr output: {}", stdErrFuture.get().getBufferString());
                processJournal.writeLogMessage("Dropping stale stderr output: " + stdErrFuture.get().getBufferString());
                stdErrFuture = null;
            }
            if (stdOutFuture != null && stdOutFuture.isDone()) {
                //logger.warn("Dropping stale stdout output: {}", stdOutFuture.get().getBufferString());
                processJournal.writeLogMessage("Dropping stale stdout output: " + stdOutFuture.get().getBufferString());
                stdOutFuture = null;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(String.format("Interrupted retrieving stale future: " + line, e));
        } catch (ExecutionException e) {
            throw new RuntimeException(String.format("Execution exception retrieving stale future: " + line, e));
        }

        startListeners();

        try {
            // write to the output stream that is the process' input
            processStdinStream.write(line.getBytes());
            processStdinStream.flush();
            processJournal.writeOutbound(line);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error writing (%s) to stdin on command", line), e);
        }
    }

    /**
     * Non-blocking call to see if output is available. It is always safe to retrieve output immediately
     * after this returns true.
     *
     * @return true if output is currently available, and output can safely be retrieved without blocking
     */
    public boolean isOutputAvailable() {
        return (stdOutFuture != null && stdOutFuture.isDone()) || (stdErrFuture != null && stdErrFuture.isDone());
    }

    /**
     * Blocking call to retrieve output from the remote process by prompt synchronization. This call collects
     * data from the remote process until it receives a prompt on either the stdout or stderr stream, or
     * the timeout is reached.
     *
     * Use isOutputAvailable to ensure the the thread will not block.
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
     * Blocking call to retrieve output from the remote process by line. This call collects
     * data from the remote process until it receives a terminated line on either the stdout or stderr stream, or
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
     * Wait for *any* output from the process (to stdout or stderr), by draining the stream(s) until no more
     * output is available. Uses a timeout to prevent the calling thread from hanging. Use isOutputAvailable
     * for non-blocking check.
     */
    private ProcessOutput getProcessOutput() throws TimeoutException {
        StreamOutput stdout = null;
        StreamOutput stderr = null;
        boolean gotStdErrTimeout = false;

        // We need to ensure that we get output from either stdout or stderr, or both when available, but we don't
        // want to block waiting for output that may never materialize on a second stream if we have already have
        // output in hand from one. So, we optimistically return whatever we can get within the timeout period.
        // Failure to do so would kill performance by ensuring that we ALWAYS block until timeout whenever output
        // is only written to one stream but not the other, which is a common case. So optimistically return
        // whatever we can get, and let the caller  decide whether to make another call to this method to get
        // more output in order to reach a synchronization point.
        //
        // TODO: Its possible this could be done using an ExecutorCompletionService:
        // ExecutorService can wait on multiple futures, but it either returns a single completed Future (invokeAny)
        // for the first stream that completes (cancelling the others and thus dropping that output), or waits for
        // all to complete (invokeAll), which always blocks and times out if there is only output on one stream.
        try {
            if (stdErrFuture != null) {
                // before we risk a timeout looking for stderr, see if stdout is available, and if so, just take that
                if (stdErrFuture.isDone()) {
                    stderr = stdErrFuture.get();
                    stdErrFuture = null;
                } else if (stdOutFuture == null || !stdOutFuture.isDone()) {
                    // neither stderr nor stdout is ready, so start our timeout on stderr
                    stderr = stdErrFuture.get(timeOutMillis, TimeUnit.MILLISECONDS);
                    stdErrFuture = null;
                }
                // else, no stderr, but stdout is done, so fall through to pick that up
            }
        } catch (TimeoutException e) {
            gotStdErrTimeout = true;
            if (!stdOutFuture.isDone()) { // we exceeded the stderr timeout and there is no stdout
                throw e;
            }
        } catch (InterruptedException e) {
            throw new GATKException("InterruptedException out", e);
        } catch (ExecutionException e) {
            throw new GATKException("ExecutionException out", e);
        }

        try {
            if (gotStdErrTimeout) {
                // we already exceeded our timeout on stderr; so don't timeout again; bail if we can't get stdout now
                if (stdOutFuture != null && stdOutFuture.isDone()) {
                    stdout = stdOutFuture.get();
                    stdOutFuture = null;
                } else { // stderr timeout, and no stdout to read
                    throw new TimeoutException("No stdout or stderr was available. The timeout period was exceeded.");
                }
            } else { // no stderr timeout, try to get stdout
                if (stdOutFuture != null) {
                    if (stdOutFuture.isDone()) {
                        // stdout is done; take it directly
                        stdout = stdOutFuture.get();
                        stdOutFuture = null;
                    } else if (stderr == null) {
                        // no stderr timeout, and stdout isn't done, so use our timeout on stdout
                        stdout = stdOutFuture.get(timeOutMillis, TimeUnit.MILLISECONDS);
                    } else {
                        // no stderr timeout, and no stdout, but we have stderr output, so take it
                    }
                } else {
                    throw new TimeoutException("No stdout or stderr was available. The timeout period was exceeded.");
                }
            }
        } catch (TimeoutException e) {
            // we didn't get any stderr, but thats ok since we got stdout
        } catch (InterruptedException e) {
            throw new GATKException("InterruptedException retrieving stderr", e);
        } catch (ExecutionException e) {
            throw new GATKException("ExecutionException retrieving stderr", e);
        }

        //log the output, and restart the listeners for next time
        processJournal.writeInbound(stdout, stderr);
        startListeners();

        return new ProcessOutput(0, stdout, stderr);
    }

    /**
     * Create a temporary FIFO suitable for sending output to the remote process. The FIFO is only valid for the
     * lifetime of the controller; the FIFO is destroyed when the controller is destroyed.
     *
     * @return a FIFO File
     */
    public File createFIFO() {
        if (fifoTempDir != null || fifoFile != null) {
            throw new IllegalArgumentException("Only one FIFO per controller is supported");
        }

        fifoTempDir = Files.createTempDir();
        final String fifoTempFileName = String.format("%s/%s", fifoTempDir.getAbsolutePath(), "gatkStreamingController.fifo");

        // create the FIFO by executing mkfifo via another ProcessController
        final ProcessSettings mkFIFOSettings = new ProcessSettings(new String[]{"mkfifo", fifoTempFileName});
        mkFIFOSettings.getStdoutSettings().setBufferSize(-1);
        mkFIFOSettings.setRedirectErrorStream(true);
        final ProcessController mkFIFOController = new ProcessController();
        final ProcessOutput result = mkFIFOController.exec(mkFIFOSettings);
        final int exitValue = result.getExitValue();

        fifoFile = new File(fifoTempFileName);
        if (exitValue != 0) {
            throw new GATKException(String.format("Failure creating FIFO (%s) got exit code (%d)", fifoTempFileName, exitValue));
        } else if (!fifoFile.exists()) {
            throw new GATKException(String.format("FIFO (%s) created but doesn't exist", fifoTempFileName));
        } else if (!fifoFile.canWrite()) {
            throw new GATKException(String.format("FIFO (%s) created isn't writable", fifoTempFileName));
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
     * Return true if either stdout or stderr ends with a synchronization string
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
     * Submit a task to start listening for output
     */
    private void startListeners() {
        // Submit runnable task to start capturing.
        if (stdOutFuture == null) {
            stdOutFuture = executorService.submit(
                    new OutputCapture(
                            new CapturedStreamOutputSnapshot(settings.getStdoutSettings(), process.getInputStream(), System.out),
                            ProcessStream.Stdout,
                            this.getClass().getSimpleName(),
                            controllerId));
        }
        if (!settings.isRedirectErrorStream() && stdErrFuture == null) {
            // don't waste a callable on stderr if its just going to be redirected to stdout
            stdErrFuture = executorService.submit(
                    new OutputCapture(
                            new CapturedStreamOutputSnapshot(settings.getStderrSettings(), process.getErrorStream(), System.err),
                            ProcessStream.Stderr,
                            this.getClass().getSimpleName(),
                            controllerId));
        }
    }

    /**
     * Stops the process from running and tries to ensure the process is cleaned up properly.
     * NOTE: sub-processes started by process may be zombied with their parents set to pid 1.
     * NOTE: capture threads may block on read.
     */
    @Override
    protected void tryDestroy() {
        if (stdErrFuture != null && !stdErrFuture.isDone()) {
            boolean isCancelled = stdErrFuture.cancel(true);
            if (!isCancelled) {
                logger.error("Failure cancelling stderr task");
            }
        }
        if (stdOutFuture != null && !stdOutFuture.isDone()) {
            boolean isCancelled = stdOutFuture.cancel(true);
            if (!isCancelled) {
                logger.error("Failure cancelling stdout task");
            }
        }
        if (process != null) {
            // terminate the app by closing the process' INPUT stream
            IOUtils.closeQuietly(process.getOutputStream());
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
                    journalingFileWriter.write("Sending: [");
                    journalingFileWriter.write(line);
                    journalingFileWriter.write("]\n\n");
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
                        journalingFileWriter.write("Received from stdout: [");
                        journalingFileWriter.write(stdout.getBufferString());
                        journalingFileWriter.write("]\n");
                    }
                    if (stderr != null) {
                        journalingFileWriter.write("Received from stderr: [");
                        journalingFileWriter.write(stderr.getBufferString());
                        journalingFileWriter.write("]\n");
                    }
                    journalingFileWriter.write("\n");
                    journalingFileWriter.flush();
                } catch (IOException e) {
                    throw new GATKException(String.format("Error writing to journaling file %s", journalingFile.getAbsolutePath()), e);
                }
            }
        }

        public void writeLogMessage(final String message) {
            if (journalingFileWriter != null) {
                try {
                    journalingFileWriter.write(message);
                    journalingFileWriter.flush();
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

