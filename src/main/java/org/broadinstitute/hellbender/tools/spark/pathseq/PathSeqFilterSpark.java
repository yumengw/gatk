package org.broadinstitute.hellbender.tools.spark.pathseq;

import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.GATKPlugin.GATKReadFilterPluginDescriptor;
import org.broadinstitute.hellbender.cmdline.programgroups.MetagenomicsProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.pathseq.loggers.PSFilterEmptyLogger;
import org.broadinstitute.hellbender.tools.spark.pathseq.loggers.PSFilterFileLogger;
import org.broadinstitute.hellbender.tools.spark.pathseq.loggers.PSFilterLogger;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import scala.Tuple2;

import java.io.IOException;

/**
 * Filters low complexity, low quality, duplicate, and host reads. First step in the PathSeq pipeline.
 *
 * <p>See PathSeqPipelineSpark for an overview of the PathSeq pipeline.</p>
 *
 * <h4>Methods</h4>
 *
 * <p>The tool works by filtering out reads in a series of stages:</p>
 *
 * <ol>
 *      <li>Remove secondary and supplementary alignments</li>
 *      <li>If the input reads are aligned to a host reference, remove mapped reads with sufficient alignment identity</li>
 *      <li>Trim adapter sequences</li>
 *      <li>Mask sequences with excessive A/T or G/C content</li>
 *      <li>Mask repetitive sequences (see Liebert et al. 2006)</li>
 *      <li>Hard clip read ends using base qualities</li>
 *      <li>Remove reads that are too short or were trimmed excessively</li>
 *      <li>Mask low-quality bases</li>
 *      <li>Remove reads with too many masked bases</li>
 *      <li>Remove reads containing one or more host k-mers</li>
 *      <li>Remove reads with sufficient alignment identity to the host reference</li>
 *      <li>Remove exact duplicate reads</li>
 *</ol>
 *
 * <h3>Input</h3>
 *
 * <ul>
 *     <li>BAM containing input reads (either unaligned or aligned to a host reference)</li>
 *     <li>*Host k-mer file generated using PathSeqBuildKmers</li>
 *     <li>*Host BWA-MEM index image generated using BwaMemIndexImageCreator</li>
 * </ul>
 *
 * <p>*Available in <a href="https://software.broadinstitute.org/gatk/download/bundle">resource bundle</a></p>
 *
 * <h3>Output</h3>
 *
 * <ul>
 *     <li>BAM containing high quality, non-host paired reads</li>
 *     <li>BAM containing high quality, non-host unpaired reads</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 *
 * <pre>
 * gatk PathSeqFilterSpark  \
 *   --input input_reads.bam \
 *   --paired-output output_reads_paired.bam \
 *   --unpaired-output output_reads_unpaired.bam \
 *   --min-clipped-read-length 60 \
 *   --kmer-file host_kmers.bfi \
 *   --filter-bwa-image host_reference.img \
 *   --filter-metrics metrics.txt
 * </pre>
 *
 * <h3>Notes</h3>
 *
 * <p>The input BAM may be unaligned (a uBAM) or aligned to a host reference (e.g. hg38). If it is aligned then
 * --is-host-aligned should be enabled. This will substantially increase performance, as host reads can then be immediately
 * subtracted prior to quality filtering and host alignment.</p>
 *
 * <h3>References</h3>
 * <ol>
 *     <li>Liebert, M. A. et al. (2006). A Fast and Symmetric DUST Implementation to Mask Low-Complexity DNA Sequences. J. Comput. Biol., 13, 1028-1040. </li>
 * </ol>
 */

@CommandLineProgramProperties(summary = "Filters low complexity, low quality, duplicate, and host reads. First step in the PathSeq pipeline.",
        oneLineSummary = "Step 1: Filters low quality, low complexity, duplicate, and host reads",
        programGroup = MetagenomicsProgramGroup.class)
@DocumentedFeature
public final class PathSeqFilterSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    public static final String PAIRED_OUTPUT_LONG_NAME = "paired-output";
    public static final String PAIRED_OUTPUT_SHORT_NAME = "PO";
    public static final String UNPAIRED_OUTPUT_LONG_NAME = "unpaired-output";
    public static final String UNPAIRED_OUTPUT_SHORT_NAME = "UO";

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
    public PSFilterArgumentCollection filterArgs = new PSFilterArgumentCollection();

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        filterArgs.doReadFilterArgumentWarnings(getCommandLineParser().getPluginDescriptor(GATKReadFilterPluginDescriptor.class), logger);
        final SAMFileHeader header = PSUtils.checkAndClearHeaderSequences(getHeaderForReads(), filterArgs, logger);

        final JavaRDD<GATKRead> reads = getReads();
        final PSFilter filter = new PSFilter(ctx, filterArgs, header);
        final Tuple2<JavaRDD<GATKRead>, JavaRDD<GATKRead>> filterResult;
        try (final PSFilterLogger filterLogger = filterArgs.filterMetricsFileUri != null ? new PSFilterFileLogger(getMetricsFile(), filterArgs.filterMetricsFileUri) : new PSFilterEmptyLogger()) {
            filterResult = filter.doFilter(reads, filterLogger);
        }
        final JavaRDD<GATKRead> pairedReads = filterResult._1;
        final JavaRDD<GATKRead> unpairedReads = filterResult._2;

        if (!pairedReads.isEmpty()) {
            header.setSortOrder(SAMFileHeader.SortOrder.queryname);
            writeReads(ctx, outputPaired, pairedReads, header);
        } else {
            logger.info("No paired reads to write - BAM will not be written.");
        }
        if (!unpairedReads.isEmpty()) {
            header.setSortOrder(SAMFileHeader.SortOrder.unsorted);
            writeReads(ctx, outputUnpaired, unpairedReads, header);
        } else {
            logger.info("No unpaired reads to write - BAM will not be written.");
        }
        filter.close();
    }

    private void writeReads(final JavaSparkContext ctx, final String outputFile, JavaRDD<GATKRead> reads,
                            final SAMFileHeader header) {
        try {
            ReadsSparkSink.writeReads(ctx, outputFile,
                    hasReference() ? referenceArguments.getReferenceFile().getAbsolutePath() : null,
                    reads, header, shardedOutput ? ReadsWriteFormat.SHARDED : ReadsWriteFormat.SINGLE,
                    getRecommendedNumReducers());
        } catch (IOException e) {
            throw new UserException.CouldNotCreateOutputFile(outputFile,"Writing failed", e);
        }
    }


}
