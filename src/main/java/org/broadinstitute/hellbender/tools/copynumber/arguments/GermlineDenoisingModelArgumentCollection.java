package org.broadinstitute.hellbender.tools.copynumber.arguments;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.hellbender.tools.copynumber.GermlineCNVCaller;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Mehrtash Babadi &lt;mehrtash@broadinstitute.org&gt;
 */
public final class GermlineDenoisingModelArgumentCollection implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum CopyNumberPosteriorExpectationMode {
        MAP("map"),
        EXACT("exact"),
        HYBRID("hybrid");

        final String pythonArgumentString;

        CopyNumberPosteriorExpectationMode(final String pythonArgumentString) {
            this.pythonArgumentString = pythonArgumentString;
        }
    }

    @Argument(
            doc = "Maximum number of bias factors.",
            fullName = "max-bias-factors",
            minValue = 0,
            optional = true
    )
    private int maxBiasFactors = 5;

    @Argument(
            doc = "Typical mapping error rate.",
            fullName = "mapping-error-rate",
            minValue = 0.,
            optional = true
    )
    private double mappingErrorRate = 0.01;

    @Argument(
            doc = "Typical scale of interval-specific unexplained variance.",
            fullName = "interval-psi-scale",
            minValue = 0.,
            optional = true
    )
    private double intervalPsiScale = 0.001;

    @Argument(
            doc = "Typical scale of sample-specific unexplained variance.",
            fullName = "sample-psi-scale",
            minValue = 0.,
            optional = true
    )
    private double samplePsiScale = 0.0001;

    @Argument(
            doc = "Precision of read depth pinning to its global value.",
            fullName = "depth-correction-tau",
            minValue = 0.,
            optional = true
    )
    private double depthCorrectionTau = 10000.0;

    @Argument(
            doc = "Standard deviation of log mean bias.",
            fullName = "log-mean-bias-standard-deviation",
            minValue = 0.,
            optional = true
    )
    private double logMeanBiasStandardDeviation = 0.1;

    @Argument(
            doc = "Initial value of ARD prior precision relative to the typical interval-specific unexplained variance scale.",
            fullName = "init-ard-rel-unexplained-variance",
            minValue = 0.,
            optional = true
    )
    private double initARDRelUnexplainedVariance = 0.1;

    @Argument(
            doc = "Number of knobs on the GC curves.",
            fullName = "num-gc-bins",
            minValue = 1,
            optional = true
    )
    private int numGCBins = 20;

    @Argument(
            doc = "Prior standard deviation of the GC curve from flat.",
            fullName = "gc-curve-standard-deviation",
            minValue = 0.,
            optional = true
    )
    private double gcCurveStandardDeviation = 1.;

    @Argument(
            doc = "The strategy for calculating copy number posterior expectations in the denoising model.",
            fullName = "copy-number-posterior-expectation-runMode",
            optional = true
    )
    private CopyNumberPosteriorExpectationMode copyNumberPosteriorExpectationMode =
            CopyNumberPosteriorExpectationMode.HYBRID;

    @Argument(
            doc = "Enable discovery of bias factors.",
            fullName = "enable-bias-factors",
            optional = true
    )
    private boolean enableBiasFactors = true;

    public List<String> generatePythonArguments(final GermlineCNVCaller.RunMode runMode) {
        final List<String> arguments = new ArrayList<>(Arrays.asList(
                String.format("--psi_s_scale=%e", samplePsiScale),
                String.format("--mapping_error_rate=%e", mappingErrorRate),
                String.format("--depth_correction_tau=%e", depthCorrectionTau),
                String.format("--q_c_expectation_mode=%s", copyNumberPosteriorExpectationMode.pythonArgumentString)));
        if (runMode == GermlineCNVCaller.RunMode.COHORT) {
            arguments.addAll(Arrays.asList(
                    String.format("--max_bias_factors=%d", maxBiasFactors),
                    String.format("--psi_t_scale=%e", intervalPsiScale),
                    String.format("--log_mean_bias_std=%e", logMeanBiasStandardDeviation),
                    String.format("--init_ard_rel_unexplained_variance=%e", initARDRelUnexplainedVariance),
                    String.format("--num_gc_bins=%d", numGCBins),
                    String.format("--gc_curve_sd=%e", gcCurveStandardDeviation)));
            if (enableBiasFactors) {
                arguments.add("--enable_bias_factors=True");
            }
        }
        return arguments;
    }

    /* todo */
    public void validate() {

    }
}
