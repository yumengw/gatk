package org.broadinstitute.hellbender.engine.datasources;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.reference.ReferenceBases;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class to load a reference sequence from a fasta file.
 */
public class ReferenceFileSource implements ReferenceSource, Serializable {
    private static final long serialVersionUID = 1L;

    private final Path referencePath;

    /**
     * @param referencePathString the path to the reference file
     */
    public ReferenceFileSource(final String referencePathString) {
        this(IOUtils.getPath(referencePathString));
    }

    /**
     * @param referencePath the path to the reference file
     */
    public ReferenceFileSource(final Path referencePath) {
        this.referencePath = referencePath;
        if (!Files.exists(this.referencePath)) {
            throw new UserException.MissingReference("The specified fasta file (" + referencePath + ") does not exist.");
        }
    }

    @Override
    public ReferenceBases getReferenceBases(final PipelineOptions pipelineOptions, final SimpleInterval interval) throws IOException {
        try ( ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(referencePath) ) {
            ReferenceSequence sequence = referenceSequenceFile.getSubsequenceAt(interval.getContig(), interval.getStart(), interval.getEnd());
            return new ReferenceBases(sequence.getBases(), interval);
        }
    }

    public Map<String, ReferenceBases> getAllReferenceBases() throws IOException {
        try ( ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(referencePath) ) {
            Map<String, ReferenceBases> bases = new LinkedHashMap<>();
            ReferenceSequence seq;
            while ( (seq = referenceSequenceFile.nextSequence()) != null ) {
                String name = seq.getName();
                bases.put(name, new ReferenceBases(seq.getBases(), new SimpleInterval(name, 1, seq.length())));
            }
            return bases;
        }
    }

    @Override
    public SAMSequenceDictionary getReferenceSequenceDictionary(final SAMSequenceDictionary optReadSequenceDictionaryToMatch) throws IOException {
        try ( ReferenceSequenceFile referenceSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(referencePath) ) {
            return referenceSequenceFile.getSequenceDictionary();
        }
    }
}
