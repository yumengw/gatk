package org.broadinstitute.hellbender.tools.spark.pathseq;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.GATKPlugin.GATKReadFilterPluginDescriptor;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.MetagenomicsProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.pathseq.loggers.*;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Combined tool that performs all PathSeq steps: read filtering, microbe reference alignment and abundance scoring
 *
 * <p>PathSeq is a suite of tools for detecting microbial organisms in deep sequenced biological samples. It is capable of
 * (1) quantifying microbial abundances in metagenomic samples containing a mixture of organisms, (2) detecting extremely
 * low-abundance (<0.001%) organisms, and (3) identifying unknown sequences that may belong to novel organisms. The pipeline is based
 * on a previously published tool of the same name (Kostic et al. 2011), which has been
 * used in a wide range of studies to investigate novel associations between pathogens and human disease.</p>
 *
 * <p>The pipeline consists of three phases: (1) removing reads that are low quality, low complexity, or match a given
 * host (e.g. human) reference, (2) aligning the remaining reads to a microorganism reference, and (3) determining
 * the taxonomic classification of each read and estimating microbe abundances. These steps can be performed individually using
 * PathSeqFilterSpark, PathSeqBwaSpark, and PathSeqScoreSpark. To simplify using the pipeline, this tool combines the
 * three steps into one. Further details can be found in the individual tools' documentations.</p>
 *
 * <p>The filtering phase ensures that only high fidelity, non-host reads are classified, thus reducing computational costs
 * and false positives. Note that while generally applicable to any type of biological sample (e.g. saliva, stool), PathSeq
 * is particularly efficient for samples containing a high percentage of host reads (e.g. blood, tissue, CSF). PathSeq
 * is able to detect evidence of low-abundance organisms and scales to use comprehensive genomic database references
 * (e.g. > 100 Gbp). Lastly, because PathSeq works by identifying both host and known microbial sequences, it can also
 * be used to discover novel pathogens by deducing the sample to sequences of unknown origin, which may be followed
 * by de novo assembly.</p>
 *
 * <p>Because sequence alignment is computationally burdensome, PathSeq is integrated with Apache Spark,
 * enabling parallelization of all steps in the pipeline on multi-core workstations and cluster environments. This
 * overcomes the high computational cost and permits rapid turnaround times (minutes to hours) in deep sequenced samples.</p>
 *
 * <h4>Reference files</h4>
 *
 * <p>Before running the PathSeq pipeline, the host and microbe references must be built. Recommended pre-built references
 * are available as <a href="https://software.broadinstitute.org/gatk/download/bundle">resource bundles</a>.</p>
 *
 * <p>To build custom references, users must provide FASTA files of the host and pathogen sequences. Tools are included to
 * generate the necessary files: the host k-mer database (PathSeqBuildKmers), BWA-MEM index image files of the host and
 * pathogen references (BwaMemIndexImageCreator), and a taxonomic tree of the pathogen reference (PathSeqBuildReferenceTaxonomy).</p>
 *
 * <h3>Input</h3>
 *
 * <ul>
 *     <li>BAM containing input reads (either unaligned or aligned to a host reference)</li>
 *     <li>Host k-mer file generated using PathSeqBuildKmers</li>
 *     <li>Host BWA-MEM index image generated using BwaMemIndexImageCreator</li>
 *     <li>Microbe BWA-MEM index image generated using BwaMemIndexImageCreator</li>
 *     <li>Indexed microbe reference FASTA file</li>
 *     <li>Taxonomy file generated using PathSeqBuildReferenceTaxonomy</li>
 * </ul>
 *
 * <h3>Output</h3>
 *
 * <ul>
 *     <li>Taxonomic scores table</li>
 *     <li>Annotated BAM aligned to the microbe reference</li>
 *     <li>Filter metrics file (optional)</li>
 *     <li>Score metrics file (optional)</li>
 * </ul>
 *
 * <h3>Usage example</h3>
 *
 * <pre>
 * gatk PathSeqFilterSpark  \
 *   --input input_reads.bam \
 *   --kmer-file host_kmers.bfi \
 *   --filter-bwa-image host_reference.img \
 *   --microbe-bwa-image microbe_reference.img \
 *   --microbe-fasta reference.fa \
 *   --taxonomy-file taxonomy.db \
 *   --min-clipped-read-length 60 \
 *   --min-score-identity 0.90 \
 *   --identity-margin 0.02 \
 *   --scores-output scores.txt \
 *   --output output_reads.bam \
 *   --filter-metrics filter_metrics.txt \
 *   --score-metrics score_metrics.txt \
 * </pre>
 *
 * <h3>References</h3>
 * <ol>
 *     <li>Kostic, A. D. et al. (2011). PathSeq: software to identify or discover microbes by deep sequencing of human tissue. Nat. Biotechnol. 29, 393-396.</li>
 * </ol>
 *
 *
 */
@CommandLineProgramProperties(summary = "Combined tool that performs all PathSeq steps: read filtering, microbe reference alignment and abundance scoring",
        oneLineSummary = "Combined tool that performs all steps: read filtering, microbe reference alignment, and abundance scoring",
        programGroup = MetagenomicsProgramGroup.class)
@DocumentedFeature
public class PathSeqPipelineSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    public static final String READS_PER_PARTITION_LONG_NAME = "pipeline-reads-per-partition";
    public static final String READS_PER_PARTITION_SHORT_NAME = READS_PER_PARTITION_LONG_NAME;

    @ArgumentCollection
    public PSFilterArgumentCollection filterArgs = new PSFilterArgumentCollection();

    @ArgumentCollection
    public PSBwaArgumentCollection bwaArgs = new PSBwaArgumentCollection();

    @ArgumentCollection
    public PSScoreArgumentCollection scoreArgs = new PSScoreArgumentCollection();

    @Argument(doc = "Output BAM",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            optional = true)
    public String outputPath = null;

    @Argument(doc = "Number of reads per partition to use for alignment and scoring.",
            fullName = READS_PER_PARTITION_LONG_NAME,
            shortName = READS_PER_PARTITION_SHORT_NAME,
            optional = true,
            minValue = 100)
    public int readsPerPartition = 5000;

    /**
     * Because numReducers is based on the input size, it causes too many partitions to be produced when the output size is much smaller.
     */
    @Argument(doc = "Number of reads per partition for output. Use this to control the number of sharded BAMs (not --numReducers).",
            fullName = "readsPerPartitionOutput",
            optional = true,
            minValue = 100,
            minRecommendedValue = 100000)
    public int readsPerPartitionOutput = 1000000;

    /**
     * Reduces number of partitions of paired reads, keeping pairs together.
     */
    private static JavaRDD<GATKRead> repartitionPairedReads(final JavaRDD<GATKRead> pairedReads, final int alignmentPartitions, final long numReads) {
        final int readsPerPartition = 1 + (int) (numReads / alignmentPartitions);
        return pairedReads.mapPartitions(iter -> PathSeqPipelineSpark.pairPartitionReads(iter, readsPerPartition))
                .repartition(alignmentPartitions)
                .flatMap(List::iterator);
    }

    /**
     * Maps partition of paired reads to a partition of Lists containing each pair. Assumes pairs are adjacent.
     */
    private static Iterator<List<GATKRead>> pairPartitionReads(final Iterator<GATKRead> iter, final int readsPerPartition) {
        final ArrayList<List<GATKRead>> readPairs = new ArrayList<>(readsPerPartition / 2);
        while (iter.hasNext()) {
            final List<GATKRead> list = new ArrayList<>(2);
            list.add(iter.next());
            if (!iter.hasNext()) throw new GATKException("Odd number of read pairs in paired reads partition");
            list.add(iter.next());
            if (!list.get(0).getName().equals(list.get(1).getName())) throw new GATKException("Pair did not have the same name in a paired reads partition");
            readPairs.add(list);
        }
        return readPairs.iterator();
    }

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        filterArgs.doReadFilterArgumentWarnings(getCommandLineParser().getPluginDescriptor(GATKReadFilterPluginDescriptor.class), logger);
        SAMFileHeader header = PSUtils.checkAndClearHeaderSequences(getHeaderForReads(), filterArgs, logger);

        //Do not allow use of numReducers
        if (numReducers > 0) {
            throw new UserException.BadInput("Use --readsPerPartitionOutput instead of --numReducers.");
        }

        //Filter
        final Tuple2<JavaRDD<GATKRead>, JavaRDD<GATKRead>> filterResult;
        final PSFilter filter = new PSFilter(ctx, filterArgs, header);
        try (final PSFilterLogger filterLogger = filterArgs.filterMetricsFileUri != null ? new PSFilterFileLogger(getMetricsFile(), filterArgs.filterMetricsFileUri) : new PSFilterEmptyLogger()) {
            final JavaRDD<GATKRead> inputReads = getReads();
            filterResult = filter.doFilter(inputReads, filterLogger);
        }
        JavaRDD<GATKRead> pairedReads = filterResult._1;
        JavaRDD<GATKRead> unpairedReads = filterResult._2;

        //Counting forces an action on the RDDs to guarantee we're done with the Bwa image and kmer filter
        final long numPairedReads = pairedReads.count();
        final long numUnpairedReads = unpairedReads.count();
        final long numTotalReads = numPairedReads + numUnpairedReads;

        //Closes Bwa image, kmer filter, and metrics file if used
        //Note the host Bwa image before must be unloaded before trying to load the pathogen image
        filter.close();

        //Rebalance partitions using the counts
        final int numPairedPartitions = 1 + (int) (numPairedReads / readsPerPartition);
        final int numUnpairedPartitions = 1 + (int) (numUnpairedReads / readsPerPartition);
        pairedReads = repartitionPairedReads(pairedReads, numPairedPartitions, numPairedReads);
        unpairedReads = unpairedReads.repartition(numUnpairedPartitions);

        //Bwa pathogen alignment
        final PSBwaAlignerSpark aligner = new PSBwaAlignerSpark(ctx, bwaArgs);
        final PipelineOptions options = getAuthenticatedGCSOptions();
        PSBwaUtils.addReferenceSequencesToHeader(header, bwaArgs.referencePath, getReferenceWindowFunction(), options);
        final Broadcast<SAMFileHeader> headerBroadcast = ctx.broadcast(header);
        JavaRDD<GATKRead> alignedPairedReads = aligner.doBwaAlignment(pairedReads, true, headerBroadcast);
        JavaRDD<GATKRead> alignedUnpairedReads = aligner.doBwaAlignment(unpairedReads, false, headerBroadcast);

        //Cache this expensive result. Note serialization significantly reduces memory consumption.
        alignedPairedReads.persist(StorageLevel.MEMORY_AND_DISK_SER());
        alignedUnpairedReads.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //Score pathogens
        final PSScorer scorer = new PSScorer(scoreArgs);
        final JavaRDD<GATKRead> readsFinal = scorer.scoreReads(ctx, alignedPairedReads, alignedUnpairedReads, header);

        //Clean up header
        header = PSBwaUtils.removeUnmappedHeaderSequences(header, readsFinal, logger);

        //Log read counts
        if (scoreArgs.scoreMetricsFileUri != null) {
            try (final PSScoreLogger scoreLogger = new PSScoreFileLogger(getMetricsFile(), scoreArgs.scoreMetricsFileUri)) {
                scoreLogger.logReadCounts(readsFinal);
            }
        }

        //Write reads to BAM, if specified
        if (outputPath != null) {
            try {
                //Reduce number of partitions since we previously went to ~5K reads per partition, which
                // is far too small for sharded output.
                final int numPartitions = Math.max(1, (int) (numTotalReads / readsPerPartitionOutput));
                final JavaRDD<GATKRead> readsFinalRepartitioned = readsFinal.coalesce(numPartitions, false);
                ReadsSparkSink.writeReads(ctx, outputPath, null, readsFinalRepartitioned, header,
                        shardedOutput ? ReadsWriteFormat.SHARDED : ReadsWriteFormat.SINGLE, numPartitions);
            } catch (final IOException e) {
                throw new UserException.CouldNotCreateOutputFile(outputPath, "writing failed", e);
            }
        }
        aligner.close();
    }

}
