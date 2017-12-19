package org.broadinstitute.hellbender.tools.copynumber.utils.annotatedregion;

import com.google.common.collect.Sets;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.copynumber.formats.collections.AbstractLocatableCollection;
import org.broadinstitute.hellbender.tools.copynumber.formats.metadata.LocatableMetadata;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.tsv.DataLine;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;
import org.broadinstitute.hellbender.utils.tsv.TableReader;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SimpleAnnotatedGenomicRegionCollection extends AbstractLocatableCollection<LocatableMetadata, SimpleAnnotatedGenomicRegion> {
    SimpleAnnotatedGenomicRegionCollection(LocatableMetadata metadata, List<SimpleAnnotatedGenomicRegion> simpleAnnotatedGenomicRegions, TableColumnCollection mandatoryColumns, Function<DataLine, SimpleAnnotatedGenomicRegion> recordFromDataLineDecoder, BiConsumer<SimpleAnnotatedGenomicRegion, DataLine> recordToDataLineEncoder) {
        super(metadata, simpleAnnotatedGenomicRegions, mandatoryColumns, recordFromDataLineDecoder, recordToDataLineEncoder);
    }

    SimpleAnnotatedGenomicRegionCollection(File inputFile, TableColumnCollection mandatoryColumns, Function<DataLine, SimpleAnnotatedGenomicRegion> recordFromDataLineDecoder, BiConsumer<SimpleAnnotatedGenomicRegion, DataLine> recordToDataLineEncoder) {
        super(inputFile, mandatoryColumns, recordFromDataLineDecoder, recordToDataLineEncoder);
    }

    /**
     *  Reads entire TSV file in one command and stores in RAM.  Please see {@link SimpleAnnotatedGenomicRegion::CONTIG_HEADER},
     *  {@link SimpleAnnotatedGenomicRegion::START_HEADER}, and
     *  {@link SimpleAnnotatedGenomicRegion::END_HEADER} for defining the genomic region.
     *
     * @param tsvRegionFile -- File containing tsv of genomic regions and annotations per line.  E.g. a segment file.
     * @param headersOfInterest -- should not include any headers that are used to define the region (e.g. contig, start, end)
     * @return annotated regions with one line in the input file for each entry of the list.  Never {@code null}
     */
    public static List<SimpleAnnotatedGenomicRegion> readAnnotatedRegions(final File tsvRegionFile, final Set<String> headersOfInterest) {
        try (final TableReader<SimpleAnnotatedGenomicRegion> reader = new TableReader<SimpleAnnotatedGenomicRegion>(tsvRegionFile) {

            // TODO: Make SAM header optional (store it, if it's there)

            @Override
            protected SimpleAnnotatedGenomicRegion createRecord(final DataLine dataLine) {

                final Set<String> headersOfInterestPresent = Sets.intersection(headersOfInterest, new HashSet<>(this.columns().names()));
                final Map<String, String> annotationMap = headersOfInterestPresent.stream()
                        .collect(Collectors.toMap(Function.identity(), dataLine::get));

                return new SimpleAnnotatedGenomicRegion( new SimpleInterval(dataLine.get(CONTIG_HEADER), dataLine.getInt(START_HEADER), dataLine.getInt(END_HEADER)),
                        new TreeMap<>(annotationMap));
            }
        }) {
            return reader.toList();
        } catch (final IOException ioe) {
            throw new UserException.CouldNotReadInputFile("Cannot read input file: " + tsvRegionFile.getAbsolutePath(), ioe);
        }
    }
}
