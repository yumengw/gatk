package org.broadinstitute.hellbender.tools.spark.pathseq;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.programgroups.MetagenomicsProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import scala.Tuple2;

import java.io.IOException;

/**
 * Align reads to a microbe reference using BWA-MEM and Spark. Second step in the PathSeq pipeline.
 *
 * <p>See PathSeqPipelineSpark for an overview of the PathSeq pipeline.</p>
 *
 * <p>This is a specialized version of BwaSpark designed for the PathSeq pipeline. The main difference is that only
 * primary and alternate alignments (i.e. those appearing in the SA/XA tags) are retained, while secondary and
 * supplementary alignments are ignored.</p>
 *
 * <h3>Input</h3>
 *
 * <ul>
 *     <li>Unaligned queryname-sorted BAM file containing only paired reads</li>
 *     <li>Unaligned BAM file containing only unpaired reads</li>
 *     <li>*Microbe reference BWA-MEM index image generated using BwaMemIndexImageCreator</li>
 *     <li>*Indexed microbe reference FASTA file</li>
 * </ul>
 *
 * <p>*Available in <a href="https://software.broadinstitute.org/gatk/download/bundle">resource bundle</a></p>
 *
 * <h3>Output</h3>
 *
 * <ul>
 *     <li>Aligned BAM file containing the paired reads</li>
 *     <li>Aligned BAM file containing the unpaired reads</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 *
 * <pre>
 * gatk PathSeqBwaSpark  \
 *   --paired-input input_reads_paired.bam \
 *   --unpaired-input input_reads_unpaired.bam \
 *   --paired-output output_reads_paired.bam \
 *   --unpaired-output output_reads_unpaired.bam \
 *   --microbe-bwa-image reference.img \
 *   --microbe-fasta reference.fa
 * </pre>
 *
 * <h3>Notes</h3>
 *
 * <p>For small input BAMs, it is recommended that the user reduce the BAM partition size in order to increase parallelism. Note
 * that insert size is estimated separately in each Spark partition. Consequently partition size (and other Spark runtime
 * environment parameters) can affect the output for paired-end alignment.</p>
 *
 * <p>To minimize output file size, header lines are included only for sequences with at least one alignment.</p>
 */

@CommandLineProgramProperties(summary = "Align reads to a microbe reference using BWA-MEM and Spark. Second step in the PathSeq pipeline.",
        oneLineSummary = "Step 2: Aligns reads to the microbe reference",
        programGroup = MetagenomicsProgramGroup.class)
@DocumentedFeature
public final class PathSeqBwaSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    public static final String PAIRED_INPUT_LONG_NAME = "paired-input";
    public static final String PAIRED_INPUT_SHORT_NAME = "PI";
    public static final String UNPAIRED_INPUT_LONG_NAME = "unpaired-input";
    public static final String UNPAIRED_INPUT_SHORT_NAME = "UI";
    public static final String PAIRED_OUTPUT_LONG_NAME = "paired-output";
    public static final String PAIRED_OUTPUT_SHORT_NAME = "PO";
    public static final String UNPAIRED_OUTPUT_LONG_NAME = "unpaired-output";
    public static final String UNPAIRED_OUTPUT_SHORT_NAME = "UO";

    @Argument(doc = "Input queryname-sorted BAM containing only paired reads",
            fullName = PAIRED_INPUT_LONG_NAME,
            shortName = PAIRED_INPUT_SHORT_NAME,
            optional = true)
    public String inputPaired = null;

    @Argument(doc = "Input BAM containing only unpaired reads",
            fullName = UNPAIRED_INPUT_LONG_NAME,
            shortName = UNPAIRED_INPUT_SHORT_NAME,
            optional = true)
    public String inputUnpaired = null;

    @Argument(doc = "Output BAM containing only paired reads",
            fullName = PAIRED_OUTPUT_LONG_NAME,
            shortName = PAIRED_OUTPUT_SHORT_NAME,
            optional = true)
    public String outputPaired = null;

    @Argument(doc = "Output BAM containing only unpaired reads",
            fullName = UNPAIRED_OUTPUT_LONG_NAME,
            shortName = UNPAIRED_OUTPUT_SHORT_NAME,
            optional = true)
    public String outputUnpaired = null;

    @ArgumentCollection
    public PSBwaArgumentCollection bwaArgs = new PSBwaArgumentCollection();

    /**
     * Reads bam from path and returns tuple of the header and reads RDD
     */
    private Tuple2<SAMFileHeader, JavaRDD<GATKRead>> loadBam(final String path, final PipelineOptions options,
                                                             final ReadsSparkSource readsSource) {
        if (path == null) return null;
        if (BucketUtils.fileExists(path)) {
            final SAMFileHeader header = readsSource.getHeader(path, null);
            if (header.getSequenceDictionary() != null && !header.getSequenceDictionary().isEmpty()) {
                throw new UserException.BadInput("Input BAM should be unaligned, but found one or more sequences in the header.");
            }
            PSBwaUtils.addReferenceSequencesToHeader(header, bwaArgs.referencePath, getReferenceWindowFunction(), options);
            final JavaRDD<GATKRead> reads = readsSource.getParallelReads(path, null, null, bamPartitionSplitSize);
            return new Tuple2<>(header, reads);
        }
        logger.warn("Could not find file " + path + ". Skipping...");
        return null;
    }

    /**
     * Writes RDD of reads to path. Note writeReads() is not used because there are separate paired/unpaired outputs.
     * Header sequence dictionary is reduced to only those that were aligned to.
     */
    private void writeBam(final JavaRDD<GATKRead> reads, final String inputBamPath, final boolean isPaired,
                          final JavaSparkContext ctx, SAMFileHeader header) {

        //Only retain header sequences that were aligned to.
        //This invokes an action and therefore the reads must be cached.
        reads.persist(StorageLevel.MEMORY_AND_DISK_SER());
        header = PSBwaUtils.removeUnmappedHeaderSequences(header, reads, logger);

        final String outputPath = isPaired ? outputPaired : outputUnpaired;
        try {
            ReadsSparkSink.writeReads(ctx, outputPath, bwaArgs.referencePath, reads, header,
                    shardedOutput ? ReadsWriteFormat.SHARDED : ReadsWriteFormat.SINGLE,
                    PSUtils.pathseqGetRecommendedNumReducers(inputBamPath, numReducers, getTargetPartitionSize()));
        } catch (final IOException e) {
            throw new UserException.CouldNotCreateOutputFile(outputPath, "Writing failed", e);
        }
    }

    /**
     * Loads a bam, aligns using the given aligner, and writes to a new bam. Returns false if the input bam could not
     * be read.
     */
    private boolean alignBam(final String inputBamPath, final PSBwaAlignerSpark aligner, final boolean isPaired,
                             final JavaSparkContext ctx, final PipelineOptions options, final ReadsSparkSource readsSource) {

        final Tuple2<SAMFileHeader, JavaRDD<GATKRead>> loadedBam = loadBam(inputBamPath, options, readsSource);
        if (loadedBam == null) return false;
        final SAMFileHeader header = loadedBam._1;
        final JavaRDD<GATKRead> reads = loadedBam._2;
        Utils.nonNull(header);
        Utils.nonNull(reads);
        if (isPaired && !header.getSortOrder().equals(SAMFileHeader.SortOrder.queryname)) {
            throw new UserException.BadInput("Paired input BAM must be sorted by queryname");
        }

        final JavaRDD<GATKRead> alignedReads = aligner.doBwaAlignment(reads, isPaired, ctx.broadcast(header));
        writeBam(alignedReads, inputBamPath, isPaired, ctx, header);
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        if (!readArguments.getReadFiles().isEmpty()) {
            throw new UserException.BadInput("Please use --pairedInput or --unpairedInput instead of --input");
        }
        final ReadsSparkSource readsSource = new ReadsSparkSource(ctx, readArguments.getReadValidationStringency());
        final PipelineOptions options = getAuthenticatedGCSOptions();

        final PSBwaAlignerSpark aligner = new PSBwaAlignerSpark(ctx, bwaArgs);
        boolean bPairedSuccess = alignBam(inputPaired, aligner, true, ctx, options, readsSource);
        boolean bUnpairedSuccess = alignBam(inputUnpaired, aligner, false, ctx, options, readsSource);
        if (!bPairedSuccess && !bUnpairedSuccess) {
            throw new UserException.BadInput("No reads were loaded. Ensure --pairedInput and/or --unpairedInput are set and valid.");
        }
        aligner.close();
    }

}
