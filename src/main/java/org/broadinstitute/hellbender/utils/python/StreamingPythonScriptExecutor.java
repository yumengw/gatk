package org.broadinstitute.hellbender.utils.python;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.utils.runtime.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

// TODO: Should we have a mode that just discards stdout (after synchronizing on prompt), and only retrieves stderr ?

/**
 * Python executor used to interact with a Python process. The lifecycle of an executor is typically:
 *
 *  start an executor
 *  create a fifo
 *  execute Python code to open the fifo for reading
 *  execute local code to eopen the fifo for writing
 *  send/receive one inpt/output one or more times
 *  terminate the executor
 *
 * Guidelines for writing GATK tools that use Python interactively:
 *
 *   Program correctness should not rely on anything written by Python to stdout/stderr output. All data
 *   should be transferred through a FIFO or file.
 *   Prefer single line commands, always terminated with newlines (otherwise python will block)
 *   Terminate commands with newline!!
 *   Try not to be chatty
 *
 * NOTE: Python implementations are unreliable about honoring standard I/O stream redirection. Its not safe to
 * try to synchronize based on anything written to standard I/O streams, since Python sometimes prints the
 * prompt to stdout, and sometimes to stderr:
 *
 *  https://bugs.python.org/issue17620
 *  https://bugs.python.org/issue1927
 */
public class StreamingPythonScriptExecutor extends PythonExecutorBase {
    private static final Logger logger = LogManager.getLogger(StreamingPythonScriptExecutor.class);
    private static final String NL = String.format("%n");

    private final List<String> curatedCommandLineArgs = new ArrayList<>();

    private StreamingProcessController spController;
    private ProcessSettings processSettings;

    final public static String PYTHON_PROMPT = ">>> ";

    /**
     * The start method must be called to actully start the remote executable.
     *
     * @param ensureExecutableExists throw if the python executable cannot be located
     */
    public StreamingPythonScriptExecutor(boolean ensureExecutableExists) {
        this(PythonExecutableName.PYTHON, ensureExecutableExists);
    }

    /**
     * The start method must be called to actully start the remote executable.
     *
     * @param pythonExecutableName name of the python executable to start
     * @param ensureExecutableExists throw if the python executable cannot be found
     */
    public StreamingPythonScriptExecutor(final PythonExecutableName pythonExecutableName, final boolean ensureExecutableExists) {
        super(pythonExecutableName, ensureExecutableExists);
    }

    /**
     * Start the Python process.
     *
     * @param pythonProcessArgs args to be passed to the python process
     * @return true if the process is successfully started
     */
    public boolean start(final List<String> pythonProcessArgs) {
        return start(pythonProcessArgs, false);
    }

    /**
     * Start the Python process.
     *
     * @param pythonProcessArgs args to be passed to the python process
     * @param enableJournaling true to enable Journaling, which records all interprocess IO to a file. This is
     *                         expensive and should only be used for debugging.
     * @return true if the process is successfully started
     */
    public boolean start(final List<String> pythonProcessArgs, final boolean enableJournaling) {
        final List<String> args = new ArrayList<>();
        args.add(externalScriptExecutableName);
        args.add("-u");
        args.add("-i");
        if (pythonProcessArgs != null) {
            args.addAll(pythonProcessArgs);
        }

        curatedCommandLineArgs.addAll(args);

        final InputStreamSettings isSettings = new InputStreamSettings();
        final OutputStreamSettings stdOutSettings = new OutputStreamSettings();
        stdOutSettings.setBufferSize(-1);
        final OutputStreamSettings stdErrSettings = new OutputStreamSettings();
        stdErrSettings.setBufferSize(-1);

        processSettings = new ProcessSettings(
                args.toArray(new String[args.size()]),
                false,  // redirect error
                null,           // directory
                null,         // env
                isSettings,
                stdOutSettings,
                stdErrSettings
        );

        spController = new StreamingProcessController(processSettings, PYTHON_PROMPT, enableJournaling);
        return spController.start();
    }

    /**
     * Send a command to Python, and wait for a response prompt, returning all accumulated output
     * since the last call to either <link #sendSynchronousCommand/> or <line #getAccumulatedOutput/>
     *
     * The caller is required to terminate commands with a newline. The executor doesn't do this
     * automatically since doing so would alter the number of prompts issued by the remote process.
     *
     * @param line data to be sent to he remote process
     * @return ProcessOutput
     */
    public ProcessOutput sendSynchronousCommand(final String line) {
        if (!line.endsWith(NL)) {
            throw new IllegalArgumentException(
                    "Python commands must be newline-terminated in order to be executed. " +
                            "Indented Python code blocks must be terminated with additional newlines");
        }
        spController.writeProcessInput(line);
        return getAccumulatedOutput();
    }

    /**
     * Send a command to the remote prorcess without waiting for a response.
     *
     * The caller is required to terminate commands with a newline. The executor doesn't do this
     * automatically since it can alter the number of prompts issued by the remote process.
     *
     * @param line data to send to the remote process
     */
    public void sendAsynchronousCommand(final String line) {
        if (!line.endsWith(NL)) {
            throw new IllegalArgumentException("Python commands must be newline-terminated");
        }
        spController.writeProcessInput(line);
    }

    /**
     * See if any output is currently available. This is non-blocking, and can be used to determin if a blocking
     * call can be made; it is always safe to call getAccumulatedOutput if isOutputAvailable is true.
     * @return true if data is available from the remote process.
     */
    public boolean isOutputAvailable() {
        return spController.isOutputAvailable();
    }

    /**
     * Return all data accumulated since the last call to getAccumulatedOutput (either directly, or indirectly
     * through <link #sendSynchronousCommand/>. If no data has been sent from the process, this call blocks,
     * waiting for data, or the timeout to be reached.
     *
     * @return ProcessOutput
     */
    public ProcessOutput getAccumulatedOutput() {
        try {
            return spController.getProcessOutputByPrompt();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Terminate the remote process, closing the fifo if any.
     *
     * @return true if the process has been successfuly terminated
     */
    public boolean terminate() {
        return spController.terminate();
    }

    /**
     * Obtain a FIFO to be sued to transfer data to Python. The FIFO returned by this method will only live
     * as long as the executor lives.
     *
     * NOTE: Since opening a FIFO for write blocks until it is opened for read, the caller is responsible
     * for ensuring that a Python command to open the FIFO has been executed before opening it for write.
     * For this reason, the opening of the FIFO is left to the caller.
     *
     * @return
     */
    public File getFIFOForWrite() {
        return spController.createFIFO();
    }

    /**
     * Return a (not necessarily executable) string representing the current command line for this executor
     * for error reporting purposes.
     * @return Command line string.
     */
    public String getApproximateCommandLine() {
        return curatedCommandLineArgs.stream().collect(Collectors.joining(" "));
    }

    /**
     * Get the Process object associated with this executor. For testing only.
     *
     * @return
     */
    @VisibleForTesting
    public Process getProcess() {
        return spController.getProcess();
    }
}
