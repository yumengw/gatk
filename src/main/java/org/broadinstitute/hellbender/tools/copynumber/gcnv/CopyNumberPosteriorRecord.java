package org.broadinstitute.hellbender.tools.copynumber.gcnv;


import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.tools.exome.germlinehmm.IntegerCopyNumberState;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.Map;

/**
 * A single copy number posterior record for a specific interval
 */
public class CopyNumberPosteriorRecord implements Locatable {
    private final SimpleInterval interval;
    private final Map<IntegerCopyNumberState, Double> copyNumberStatePosteriors;

    CopyNumberPosteriorRecord(final SimpleInterval interval, final Map<IntegerCopyNumberState, Double> copyNumberStatePosteriors) {
        this.interval = Utils.nonNull(interval);
        this.copyNumberStatePosteriors = Utils.nonNull(copyNumberStatePosteriors);
    }

    @Override
    public String getContig() {
        return interval.getContig();
    }

    @Override
    public int getStart() {
        return interval.getStart();
    }

    @Override
    public int getEnd() {
        return interval.getEnd();
    }
}
