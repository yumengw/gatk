package org.broadinstitute.hellbender.engine;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import htsjdk.samtools.util.IOUtil;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.GATKBaseTest;

import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

public class ReferenceFileSourceUnitTest extends GATKBaseTest {

    private static final Path TEST_REFERENCE = IOUtils.getPath(hg19MiniReference);

    @Test(expectedExceptions = UserException.MissingReferenceFaiFile.class)
    public void testReferenceInPathWithoutFai() throws IOException {
        try (FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix())) {
            final Path refPath = jimfs.getPath("reference.fasta");
            Files.createFile(refPath);
            final Path dictPath = jimfs.getPath("reference.dict");
            Files.createFile(dictPath);

            new ReferenceFileSource(refPath).close();
        }
    }

    @Test(expectedExceptions = UserException.MissingReferenceDictFile.class)
    public void testReferenceInPathWithoutDict() throws IOException {
        try (FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix())) {
            final Path refPath = jimfs.getPath("reference.fasta");
            Files.createFile(refPath);
            final Path faiFile = jimfs.getPath("reference.fasta.fai");
            Files.createFile(faiFile);

            new ReferenceFileSource(refPath).close();
        }
    }

    @Test
    public void testReferenceInPath() throws IOException {
        try (FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix())) {
            final Path refPath = jimfs.getPath("reference.fasta");
            Files.createFile(refPath);
            final Path faiFile = jimfs.getPath("reference.fasta.fai");
            Files.createFile(faiFile);
            final Path dictPath = jimfs.getPath("reference.dict");
            Files.createFile(dictPath);

            new ReferenceFileSource(refPath).close();
        }
    }

    @Test
    public void testGetSequenceDictionary() throws IOException {
        try (FileSystem jimfs = Jimfs.newFileSystem(Configuration.unix())) {
            final Path refPath = jimfs.getPath("reference.fasta");
            final Path faiPath = jimfs.getPath("reference.fasta.fai");
            final Path dictPath = jimfs.getPath("reference.dict");
            Files.copy(TEST_REFERENCE, refPath);
            Files.copy(IOUtil.addExtension(TEST_REFERENCE, ".fai"), faiPath);
            Files.copy(ReferenceSequenceFileFactory.getDefaultDictionaryForReferenceSequence(TEST_REFERENCE),dictPath);
            try (ReferenceDataSource refDataSource = new ReferenceFileSource(refPath)) {
                SAMSequenceDictionary sequenceDictionary = refDataSource.getSequenceDictionary();
                Assert.assertEquals(sequenceDictionary.size(), 4, "Wrong number of sequences in sequence dictionary returned from refDataSource.getSequenceDictionary()");
                for ( String contig : Arrays.asList("1", "2", "3", "4") ) {
                    Assert.assertNotNull(sequenceDictionary.getSequence(contig), "Sequence dictionary returned from refDataSource.getSequenceDictionary() lacks expected contig " + contig);
                }
            }
        }
    }

}
