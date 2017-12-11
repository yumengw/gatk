package org.broadinstitute.hellbender.tools.copynumber.gcnv;

import htsjdk.variant.variantcontext.*;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderVersion;
import org.broadinstitute.hellbender.utils.Utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Helper class for single sample gCNV postprocessing
 */
public class GCNVPostProcessor {


    /**
     * VCF header keys
     */
    public static final String NUM_MODEL_CHUNKS = "NMC";
    public static final String CN_MAP = "CNMAP";
    public static final String CN_MEAN = "CNMEAN";
    public static final String CN_STD = "CNSTD";
    public static final String CNQ = "CNQ";

    private final List<Allele> alleles;
    private final IntegerCopyNumberStateCollection integerCopyNumberStateCollection;
    private final String sampleName;

    GCNVPostProcessor(final IntegerCopyNumberStateCollection integerCopyNumberStateCollection, final String sampleName) {
        Utils.nonNull(integerCopyNumberStateCollection);
        this.integerCopyNumberStateCollection = integerCopyNumberStateCollection;
        this.alleles = integerCopyNumberStateCollection.getAlleles();
        this.sampleName = sampleName;
    }


    /**
     * Composes the VCF header
     */
    private VCFHeader composeHeader(@Nullable final String commandLine) {
        final VCFHeader result = new VCFHeader(Collections.emptySet(), Arrays.asList(sampleName));

        /* add VCF version */
        result.addMetaDataLine(new VCFHeaderLine(VCFHeaderVersion.VCF4_2.getFormatString(),
                VCFHeaderVersion.VCF4_2.getVersionString()));


        /* add command line */
        if (commandLine != null) {
            result.addMetaDataLine(new VCFHeaderLine("command", commandLine));
        }

        result.addMetaDataLine(new VCFFormatHeaderLine());



    }


    /**
     *
     * @param copyNumberPosteriorRecord
     * @param variantPrefix
     * @return
     */
    protected VariantContext composeVariantContext(final CopyNumberPosteriorRecord copyNumberPosteriorRecord,
                                                 final String variantPrefix) {
        final VariantContextBuilder variantContextBuilder = new VariantContextBuilder();
        variantContextBuilder.alleles(alleles);
        variantContextBuilder.chr(copyNumberPosteriorRecord.getContig());
        variantContextBuilder.start(copyNumberPosteriorRecord.getStart());
        variantContextBuilder.stop(copyNumberPosteriorRecord.getEnd());
        variantContextBuilder.id(String.format(variantPrefix + "_%s_%d_%d",
                copyNumberPosteriorRecord.getContig(),
                copyNumberPosteriorRecord.getStart(),
                copyNumberPosteriorRecord.getEnd()));
        final GenotypeBuilder genotypeBuilder = new GenotypeBuilder(sampleName);
        //TODO add variant context fields to genotype






        final Genotype genotype = genotypeBuilder.make();



    }


}
