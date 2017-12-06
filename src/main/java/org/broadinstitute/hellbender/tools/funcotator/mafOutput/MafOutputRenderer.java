package org.broadinstitute.hellbender.tools.funcotator.mafOutput;

import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.tools.funcotator.Funcotation;
import org.broadinstitute.hellbender.tools.funcotator.OutputRenderer;

import java.util.List;

/**
 * A Funcotator output renderer for writing to MAF files.
 * Complies with version 2.4 of the MAF standard found here:
 * https://wiki.nci.nih.gov/display/TCGA/Mutation+Annotation+Format+%28MAF%29+Specification+-+v2.4
 * Created by jonn on 12/5/17.
 */
public class MafOutputRenderer extends OutputRenderer {

    //==================================================================================================================
    // Public Static Members:

    //==================================================================================================================
    // Private Static Members:

    protected static final String UNKNOWN_VALUE_STRING = "__UNKNOWN__";

    //==================================================================================================================
    // Private Members:

    //==================================================================================================================
    // Constructors:

    //==================================================================================================================
    // Override Methods:

    @Override
    public void open() {

    }

    @Override
    public void close() {

    }

    @Override
    public void write(final VariantContext variant, final List<Funcotation> funcotations) {

    }


    //==================================================================================================================
    // Static Methods:

    //==================================================================================================================
    // Instance Methods:

    //==================================================================================================================
    // Helper Data Types:

    //------------------------------------------------------------------------------------------------------------------
    // Required Columns:

// 1	Hugo_Symbol
// 2	Entrez_Gene_Id
// 3	Center
// 4	NCBI_Build
// 5	Chromosome
// 6	Start_Position
// 7	End_Position
// 8	Strand
// 9	Variant_Classification
//10	Variant_Type
//11	Reference_Allele
//12	Tumor_Seq_Allele1
//13	Tumor_Seq_Allele2
//14	dbSNP_RS
//15	dbSNP_Val_Status
//16	Tumor_Sample_Barcode
//17	Matched_Norm_Sample_Barcode
//18	Match_Norm_Seq_Allele1
//19	Match_Norm_Seq_Allele2
//20	Tumor_Validation_Allele1
//21	Tumor_Validation_Allele2
//22	Match_Norm_Validation_Allele1
//23	Match_Norm_Validation_Allele2
//24	Verification_Status 4
//25	Validation_Status 5
//26	Mutation_Status
//27	Sequencing_Phase
//28	Sequence_Source
//29	Validation_Method
//30	Score
//31	BAM_File
//32	Sequencer

}
