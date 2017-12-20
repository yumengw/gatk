package org.broadinstitute.hellbender.tools.copynumber.utils.annotatedregion;

import com.google.common.collect.Sets;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.PeekableIterator;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Simple class that just has an interval and name value pairs.
 *
 * When reading a TSV file of simple annotated genomic regions, the genomic region columns for the header when reading
 *  are specified in {@link SimpleAnnotatedGenomicRegion::CONTIG_HEADER},
 * {@link SimpleAnnotatedGenomicRegion::START_HEADER}, and {@link SimpleAnnotatedGenomicRegion::END_HEADER}
 */
final public class SimpleAnnotatedGenomicRegion implements Locatable {
    public final static String CONTIG_HEADER = "CONTIG";
    public final static String START_HEADER = "START";
    public final static String END_HEADER = "END";

    private SimpleInterval interval;
    private final SortedMap<String, String> annotations;

    public SimpleAnnotatedGenomicRegion(final SimpleInterval interval, final SortedMap<String, String> annotations) {
        this.interval = interval;
        this.annotations = annotations;
    }

    public SimpleInterval getInterval() {
        return interval;
    }

    public SortedMap<String, String> getAnnotations() {
        return annotations;
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

    public void setInterval(final Locatable interval) {
        this.interval = new SimpleInterval(interval);
    }

    public void setEnd(final int end) {
        this.interval = new SimpleInterval(this.interval.getContig(), this.interval.getStart(), end);
    }

    public String getAnnotationValue(final String annotationName) {
        return annotations.get(annotationName);
    }

    public boolean hasAnnotation(final String annotationName) {
        return annotations.containsKey(annotationName);
    }

    /**
     * Creates the annotation if it does not exist.
     *
     * @param annotationName the name for the annotation
     * @param annotationValue the value
     * @return the previous value or null
     */
    public String setAnnotation(final String annotationName, final String annotationValue) {
        return annotations.put(annotationName, annotationValue);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SimpleAnnotatedGenomicRegion that = (SimpleAnnotatedGenomicRegion) o;
        return this.interval.equals(that.getInterval()) && this.getAnnotations().equals(that.getAnnotations());
    }

    @Override
    public int hashCode() {
        int result = interval.hashCode();
        result = 31 * result + annotations.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return interval.toString() + " :: " + annotations.entrySet().stream()
                .map(e -> e.getKey() + "->" + e.getValue()).collect(Collectors.joining(","));
    }

    /** TODO: Docs
     * Throws exception if the two regions cannot be merged.  This is usually due to being on different contigs.
     * When annotations conflict, use the separator to put separate values.
     */
     private static SimpleAnnotatedGenomicRegion merge(final SimpleAnnotatedGenomicRegion region1, final SimpleAnnotatedGenomicRegion region2,
                                                      final String separator) {
         final SimpleInterval interval = IntervalUtils.mergeSegments(region1.getInterval(), region2.getInterval());

         final Set<Map.Entry<String, String>> allEntries = Sets.union(region1.getAnnotations().entrySet(),
                 region2.getAnnotations().entrySet());

         // For each remaining entry, if the annotation name only exists in one region, then just pass it through.
         //     if it exists in both entries, then merge it using the separator.
         final BiFunction<String, String,String> conflictFunction = (s1, s2) -> renderConflict(s1, s2, separator);
         final SortedMap<String, String> annotations = new TreeMap<>();
         allEntries.forEach(e -> annotations.put(e.getKey(), mergeAnnotationValue(e.getKey(), region1, region2, conflictFunction)));

         return new SimpleAnnotatedGenomicRegion(interval, annotations);
     }

    /** TODO: Docs and param checks
     * Only merges overlaps, not abutters.
     * @param initialSegments
     * @param dictionary
     * @param annotationSeparator
     * @return Segments will be sorted by the sequence dictionary
     */
    public static List<SimpleAnnotatedGenomicRegion> mergeRegions(final List<SimpleAnnotatedGenomicRegion> initialSegments,
                                                                  final SAMSequenceDictionary dictionary, final String annotationSeparator) {

        final List<SimpleAnnotatedGenomicRegion> segments = IntervalUtils.sortLocatablesBySequenceDictionary(initialSegments,
                dictionary);

        final List<SimpleAnnotatedGenomicRegion> finalSegments = new ArrayList<>();
        final PeekableIterator<SimpleAnnotatedGenomicRegion> segmentsIterator = new PeekableIterator<>(segments.iterator());
        while (segmentsIterator.hasNext()) {
            SimpleAnnotatedGenomicRegion currentRegion = segmentsIterator.next();
            while (segmentsIterator.peek() != null && IntervalUtils.overlaps(currentRegion, segmentsIterator.peek())) {
                final SimpleAnnotatedGenomicRegion toBeMerged = segmentsIterator.next();
                currentRegion = SimpleAnnotatedGenomicRegion.merge(currentRegion, toBeMerged, annotationSeparator);
            }
            finalSegments.add(currentRegion);
        }
        return finalSegments;
    }

    /**
     *  Return a merged annotation value for the two regions and given annotation name.
     * TODO: Docs
     * @param annotationName
     * @param region1
     * @param region2
     * @param conflictFunction
     * @return
     */
     private static String mergeAnnotationValue(final String annotationName, final SimpleAnnotatedGenomicRegion region1,
                                                final SimpleAnnotatedGenomicRegion region2, final BiFunction<String, String, String> conflictFunction) {
         final boolean doesRegion1ContainAnnotation = region1.hasAnnotation(annotationName);
         final boolean doesRegion2ContainAnnotation = region2.hasAnnotation(annotationName);

         if (doesRegion1ContainAnnotation && doesRegion2ContainAnnotation) {

             // Both regions contain an annotation and presumably these are of different values.
             return conflictFunction.apply(region1.getAnnotationValue(annotationName),
                     region2.getAnnotationValue(annotationName));
         } else if (doesRegion1ContainAnnotation) {
             return region1.getAnnotationValue(annotationName);
         } else if (doesRegion2ContainAnnotation) {
             return region2.getAnnotationValue(annotationName);
         }

         return null;
     }

    /**
     * TODO: Docs
     * @param s1
     * @param s2
     * @param separator
     * @return
     */
     private static String renderConflict(final String s1, final String s2, final String separator) {
        final String[] s1Vals = StringUtils.splitByWholeSeparator(s1, separator);
        final String[] s2Vals = StringUtils.splitByWholeSeparator(s2, separator);

        final Set<String> allValsSet = new HashSet<>(Arrays.asList(s1Vals));
        allValsSet.addAll(Arrays.asList(s2Vals));

        final List<String> allVals = new ArrayList<>(allValsSet);
        allVals.sort(String::compareTo);

        return Utils.join(separator, allVals);
     }
}
