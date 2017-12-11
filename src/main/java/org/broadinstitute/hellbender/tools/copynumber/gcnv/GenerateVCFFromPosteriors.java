package org.broadinstitute.hellbender.tools.copynumber.gcnv;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.programgroups.CopyNumberProgramGroup;
import org.broadinstitute.hellbender.engine.GATKTool;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.GenomeLoc;
import org.broadinstitute.hellbender.utils.GenomeLocParser;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.text.XReadLines;
import org.broadinstitute.hellbender.utils.tsv.TableUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import org.broadinstitute.hellbender.utils.variant.writers.GVCFWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * This tool takes in a list of directories to chunked gCNV calls output containing posteriors for different copy number states,
 * and outputs a VCF containing records
 */
@CommandLineProgramProperties(
        summary = "Collects ref/alt counts at sites.",
        oneLineSummary = "Collects ref/alt counts at sites.",
        programGroup = CopyNumberProgramGroup.class
)
public class GenerateVCFFromPosteriors extends GATKTool {
    private static final Logger logger = LogManager.getLogger(GenerateVCFFromPosteriors.class);

    private static final String POSTERIOR_FILE_FULL_NAME = "CopyNumberPosterior";
    private static final String POSTERIOR_FILE_SHORT_NAME = "CNP";

    private static final String SAMPLE_INDEX_FULL_NAME = "SampleIndex";
    private static final String SAMPLE_INDEX_SHORT_NAME = "SI";

    @Argument(
            doc = "List of paths to chunks' directories",
            fullName = POSTERIOR_FILE_FULL_NAME,
            shortName = POSTERIOR_FILE_SHORT_NAME
    )
    private List<String> chunkDirectoryList;

    @Argument(
            doc = "Index of the sample in the gCNV pipeline",
            fullName = SAMPLE_INDEX_FULL_NAME,
            shortName = SAMPLE_INDEX_SHORT_NAME
    )
    private String sampleIndex;

    /**
     * Complete list of intervals encompassing all chunks
     */
    final List<SimpleInterval> intervals = null;

    /**
     * Interval to copy number posterior record map
     */
    final Map<SimpleInterval, CopyNumberPosteriorRecord> copyNumberPosteriorRecordMap = null;

    @Override
    public boolean requiresIntervals() {
        return true;
    }

    @Override
    public void traverse() {
        logger.info("Initializing and validating intervals...");

        final SAMSequenceDictionary sequenceDictionary = getBestAvailableSequenceDictionary();
        final List<SimpleInterval> intervals = intervalArgumentCollection.getIntervals(sequenceDictionary);

        final int numChunks = chunkDirectoryList.size();

        final GenomeLocParser genomeLocParser = new GenomeLocParser(getBestAvailableSequenceDictionary());

        int currentChunk = 0;
        for(String chunkRootDirectory: chunkDirectoryList) {
            //TODO open file based on the starting chunk directory and the sample index
            //TODO i.e. look for a file chunk_dir/SAMPLE_(SampleIndex)/log_q_c_tc.tsv
            logger.info(String.format("Analyzing copy number posterior chunk number %d", currentChunk));

            //get a list of intervals associated with chunk currently being processed
            final List<GenomeLoc> chunkGenomeLocList = IntervalUtils.intervalFileToList(genomeLocParser, getIntervalFileFromChunkDirectory(chunkRootDirectory).getAbsolutePath());
            final List<SimpleInterval> chunkIntervalList = IntervalUtils.convertGenomeLocsToSimpleIntervals(chunkGenomeLocList);

            final File chunkPosteriorFile = getPosteriorFileFromChunkDirectory(chunkRootDirectory, sampleIndex);

            final List<String> copyNumberStateColumns = getPosteriorFileColumns(chunkPosteriorFile);
            final IntegerCopyNumberStateCollection copyNumberStateCollection = new IntegerCopyNumberStateCollection(copyNumberStateColumns);

            //TODO create a collection of posteriors
            final ChunkedCopyNumberPosteriorCollection chunkedCopyNumberPosteriorCollection =
                    new ChunkedCopyNumberPosteriorCollection(chunkPosteriorFile, chunkIntervalList, copyNumberStateCollection);
        }
        //TODO postprocess the collection of posterior records and for each (interval, posteriorRecord) pair
        //VariantContextWriter writer = GATKVariantContextUtils.createVCFWriter(new File(outputVCF), null, false);
        //writer = new GVCFWriter(writer, hcArgs.GVCFGQBands, hcArgs.genotypeArgs.samplePloidy);
        return;
    }




    /**
     * Get list of column names of the copy number posterior file
     */
    private static List<String> getPosteriorFileColumns(final File copyNumberPosteriorFile) {
        List<String> columns = null;
        try (final XReadLines reader = new XReadLines(copyNumberPosteriorFile)) {
            while (reader.hasNext()) {
                String nextLine = reader.next();
                if (!nextLine.startsWith(TableUtils.COMMENT_PREFIX)) {
                    columns = Arrays.asList(nextLine.split(TableUtils.COLUMN_SEPARATOR_STRING));
                    break;
                }
            }
        } catch (final IOException e) {
            throw new UserException.CouldNotReadInputFile(copyNumberPosteriorFile);
        }
        if (columns == null) {
            throw new UserException.BadInput("Copy number posterior file does not have a header");
        }
        return columns;
    }

    /**
     * Get the posterior file
     * TODO extract this to the GermlineCNVNamingConstants class
     */
    private static File getPosteriorFileFromChunkDirectory(final String chunkDirectoryPath, final String sampleIndex) {
        final String posteriorCallsDirectory = chunkDirectoryPath + File.separator + GermlineCNVNamingConstants.COPY_NUMBER_STATE_STRING_START + sampleIndex;
        final File posteriorFile = new File(posteriorCallsDirectory, GermlineCNVNamingConstants.COPY_NUMBER_POSTERIOR_FILE_NAME);
        return posteriorFile;
    }

    /**
     * Get the intervals file
     * TODO extract this to the GermlineCNVNamingConstants class
     */
    private static File getIntervalFileFromChunkDirectory(final String chunkDirectoryPath) {
        final File intervalsFile = new File(chunkDirectoryPath, GermlineCNVNamingConstants.INTERVAL_LIST_FILE_NAME);
        return intervalsFile;
    }
}
