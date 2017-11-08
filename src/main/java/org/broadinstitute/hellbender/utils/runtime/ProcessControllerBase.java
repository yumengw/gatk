package org.broadinstitute.hellbender.utils.runtime;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.Utils;

import java.io.IOException;
import java.util.*;

public abstract class ProcessControllerBase<CAPTURE_POLICY extends CapturedStreamOutput> implements Thread.UncaughtExceptionHandler
{
    private static final Logger logger = LogManager.getLogger(ProcessControllerBase.class);

    protected enum ProcessStream {Stdout, Stderr}

    // Tracks running controllers.
    protected static final Set<ProcessControllerBase<?>> running = Collections.synchronizedSet(new LinkedHashSet<>());

    // Threads that capture stdout and stderr
    protected Thread stdoutCapture;
    protected Thread stderrCapture;

    private final static String threadGroupName = "gatkProcessControllers";
    protected final static ThreadGroup controllerThreadGroup = new ThreadGroup(threadGroupName);

    // Tracks the running process associated with this controller.
    protected Process process;

    // When a caller destroys a controller a new thread local version will be created
    protected boolean destroyed = false;

    // Holds the stdout and stderr sent to the background capture threads
    protected final Map<ProcessStream, CAPTURE_POLICY> toCapture = new EnumMap<>(ProcessStream.class);

    // Holds the results of the capture from the background capture threads.
    // May be the content via toCapture or an StreamOutput.EMPTY if the capture was interrupted.
    protected final Map<ProcessStream, StreamOutput> fromCapture = new EnumMap<>(ProcessStream.class);

    // Useful for debugging if background threads have shut down correctly
    private static int nextControllerId = 0;
    protected final int controllerId;

    public ProcessControllerBase() {
        synchronized (running) {
            controllerId = nextControllerId++;
        }
    }

    /**
     * @return The set of still running processes.
     */
    public static Set<ProcessControllerBase<?>> getRunning() {
        synchronized (running) {
            return new LinkedHashSet<>(running);
        }
    }

    /**
     * Executes a command line program with the settings and waits for it to return,
     * processing the output on a background thread.
     *
     * @param settings Settings to be run.
     * @return The output of the command.
     */
    protected Process launchProcess(ProcessSettings settings) {
        Utils.validate(!destroyed, "This controller was destroyed");

        final ProcessBuilder builder = new ProcessBuilder(settings.getCommand());
        builder.directory(settings.getDirectory());

        final Map<String, String> settingsEnvironment = settings.getEnvironment();
        if (settingsEnvironment != null) {
            final Map<String, String> builderEnvironment = builder.environment();
            builderEnvironment.clear();
            builderEnvironment.putAll(settingsEnvironment);
        }

        builder.redirectErrorStream(settings.isRedirectErrorStream());

        // Start the process running.
        try {
            synchronized (toCapture) {
                process = builder.start();
            }
            running.add(this);
        } catch (IOException e) {
            final String message = String.format("Unable to start command: %s\nReason: %s",
                    StringUtils.join(builder.command(), " "),
                    e.getMessage());
            throw new GATKException(message);
        }
        return process;
    }

    @VisibleForTesting
    public Process getProcess() {
        return process;
    }

    /**
     * Stops the process from running and tries to ensure process is cleaned up properly.
     * NOTE: sub-processes started by process may be zombied with their parents set to pid 1.
     * NOTE: capture threads may block on read.
     * TODO: Try to use NIO to interrupt streams.
     */
    abstract void tryDestroy();

    @Override
    protected void finalize() throws Throwable {
        try {
            tryDestroy();
        } catch (Exception e) {
            logger.error(e);
        }
        super.finalize();
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (t.getThreadGroup().getName().equals(threadGroupName)) {
            System.err.println(String.format(
                    "Uncaught exception handler called on thread %s for thread %s", Thread.currentThread().getName(), t.getName()));
        }
    }

    protected class OutputCapture extends Thread {
        private final ProcessStream key;

        /**
         * Reads in the output of a stream on a background thread to keep the output pipe from backing up and freezing the called process.
         *
         * @param key The stdout or stderr key for this output capture.
         * @param controllerId Unique id of the controller.
         */
        public OutputCapture(final ProcessControllerBase<?> b, final ThreadGroup threadGroup, final String className, ProcessStream key, int controllerId) {
            super(threadGroup,
                    String.format("OutputCapture-%s-%d-%s-%s-%d", className, controllerId, key.name().toLowerCase(),
                    Thread.currentThread().getName(), Thread.currentThread().getId()));
            this.key = key;
            setDaemon(true);
            this.setUncaughtExceptionHandler(b);
        }

        /**
         * Runs the capture.
         */
        @Override
        public void run() {
            while (!destroyed && !isInterrupted()) {
                StreamOutput processStream = StreamOutput.EMPTY;
                try {
                    // Wait for a new input stream to be passed from this process controller.
                    CAPTURE_POLICY capturedProcessStream = null;
                    while (!destroyed && !isInterrupted() && capturedProcessStream == null) {
                        synchronized (toCapture) {
                            if (toCapture.containsKey(key)) {
                                capturedProcessStream = toCapture.remove(key);
                            } else {
                                toCapture.wait();
                            }
                        }
                    }

                    if (!destroyed && !isInterrupted() ) {
                        // Read in the input stream
                        boolean eof = capturedProcessStream.read();
                        //TODO: handle EOF on closed stream
                        processStream = capturedProcessStream;
                    }
                } catch (InterruptedException e) {
                    logger.info("OutputCapture interrupted, exiting");
                    break;
                } catch (IOException e) {
                    logger.error("Error reading process output", e);
                } finally {
                    // Send the output back to the process controller.
                    synchronized (fromCapture) {
                        fromCapture.put(key, processStream);
                        fromCapture.notify();
                    }
                }
            }
        }
    }
}

