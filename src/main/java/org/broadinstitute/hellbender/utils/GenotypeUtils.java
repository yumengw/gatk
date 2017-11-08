package org.broadinstitute.hellbender.utils;

import htsjdk.variant.variantcontext.*;

public final class GenotypeUtils {
    private GenotypeUtils(){}

    /**
     * Returns true of the genotype is a called diploid genotype with likelihoods.
     */
    public static boolean isDiploidWithLikelihoods(final Genotype g) {
        return Utils.nonNull(g).hasLikelihoods() && g.getPloidy() == 2;
    }

    /**
     * Returns a triple of ref/het/hom genotype "counts".
     *
     * The exact meaning of the count is dependent on the rounding behavior.
     * if {@code roundContributionFromEachGenotype}: the counts are discrete integer counts of the most probable genotype for each {@link Genotype}
     * else: they are the sum of the normalized likelihoods of each genotype and will not be integers
     *
     * Skips non-diploid genotypes.
     *
     *
     * @param vc the VariantContext that the {@link Genotype}s originated from, non-null
     * @param genotypes a GenotypesContext containing genotypes to count, these must be a subset of {@code vc.getGenotypes()}, non-null
     * @param roundContributionFromEachGenotype if this is true, the normalized likelihood from each genotype will be rounded before
     *                                          adding to the total count
     */
    public static GenotypeCounts computeDiploidGenotypeCounts(final VariantContext vc, final GenotypesContext genotypes,
                                                              final boolean roundContributionFromEachGenotype){
        Utils.nonNull(vc, "vc");
        Utils.nonNull(genotypes, "genotypes");

        int idxAA = 0;
        int idxAB;

        double refCount = 0;
        double hetCount = 0;
        double homCount = 0;

        for (final Genotype g : genotypes) {
            if (! isDiploidWithLikelihoods(g)){
                continue;
            }

            // Genotype::getLikelihoods returns a new array, so modification in-place is safe
            final double[] normalizedLikelihoods = MathUtils.normalizeFromLog10ToLinearSpace(g.getLikelihoods().getAsVector());

            double refLikelihood = normalizedLikelihoods[idxAA];
            double hetLikelihood = 0;

            //sum up contributions of all ref-alt hets into hetCount
            for (final Allele currAlt : vc.getAlternateAlleles()) {
                idxAB = GenotypeLikelihoods.calculatePLindex(0,vc.getAlleleIndex(currAlt));
                hetLikelihood += normalizedLikelihoods[idxAB];
            }
            //NOTE: rounding is special cased for [0,0,X] PLs  ([X,0,0] isn't a problem)
            if( roundContributionFromEachGenotype ) {
                refCount += MathUtils.fastRound(refLikelihood);
                if (refLikelihood != hetLikelihood) {   //if GQ = 0 (specifically [0,0,X] PLs) count as homRef and don't add to the other counts
                    hetCount += MathUtils.fastRound(hetLikelihood);
                    homCount += 1 - MathUtils.fastRound(refLikelihood) - MathUtils.fastRound(hetLikelihood);
                }
            } else {
                refCount += refLikelihood;
                hetCount += hetLikelihood;
                homCount += 1 - refLikelihood - hetLikelihood;
            }



        }
        return new GenotypeCounts(refCount, hetCount, homCount);
    }
}
