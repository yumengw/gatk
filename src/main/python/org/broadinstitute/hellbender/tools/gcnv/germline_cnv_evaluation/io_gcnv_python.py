import os
import numpy as np
import pandas as pd
from typing import Set, List, Dict
import gcnvkernel
from gcnvkernel import Interval, SampleMetadataCollection
import logging
from .core import GenericCNVCallSet, GenericCopyNumberVariant

_logger = logging.getLogger(__name__)


def import_sample_copy_number_log_posterior(sample_posterior_path: str,
                                            delimiter='\t',
                                            comment='@') -> np.ndarray:
    log_q_c_tc_tsv_file = os.path.join(sample_posterior_path,
                                       gcnvkernel.io_consts.default_copy_number_log_posterior_tsv_filename)
    log_q_c_tc_pd = pd.read_csv(log_q_c_tc_tsv_file, delimiter=delimiter, comment=comment)
    imported_log_q_c_tc = log_q_c_tc_pd.values
    assert imported_log_q_c_tc.ndim == 2
    return imported_log_q_c_tc


def get_cnv_segments_in_calls_path(interval_list: List[Interval],
                                   sample_posterior_path: str,
                                   sample_metadata_collection: SampleMetadataCollection,
                                   allosomal_contigs: Set[str] = {'X', 'Y'},
                                   default_autosomal_ploidy: int = 2):
    sample_name = gcnvkernel.io_commons.get_sample_name_from_txt_file(sample_posterior_path)
    log_q_c_array = import_sample_copy_number_log_posterior(sample_posterior_path)
    assert log_q_c_array.shape[0] == len(interval_list)
    for ti, interval in enumerate(interval_list):
        log_q_c = log_q_c_array[ti, :]
        var_copy_number, quality = gcnvkernel.models.commons.perform_genotyping(log_q_c)
        if interval.contig in allosomal_contigs:
            ref_copy_number = sample_metadata_collection \
                .get_sample_ploidy_metadata(sample_name).get_contig_ploidy(interval.contig)
        else:
            ref_copy_number = default_autosomal_ploidy
        yield GenericCopyNumberVariant(interval.contig, interval.start, interval.end, var_copy_number, quality,
                                       ref_copy_number, variant_frequency=None, num_intervals=1)


def load_gcnv_python_calls(gcnvkernel_denoising_chunks_call_paths: List[str],
                           ploidy_calls_path: str,
                           allosomal_contigs: Set[str] = {'X', 'Y'},
                           autosomal_ref_copy_number: int = 2) -> Dict[str, GenericCNVCallSet]:
    """Loads GATK gCNV calls from the intermediate gcnvkernel analysis chunks and generates a dictionary
        of call sets.
    Args:
        gcnvkernel_denoising_chunks_call_paths: list of analysis chunks
        ploidy_calls_path: output calls path of the ploidy determination tool
        allosomal_contigs: list of allosomal contigs
        autosomal_ref_copy_number: ref copy number on autosomal contigs

    Returns:
        a dict of call sets
    """
    # load ploidy calls
    sample_metadata_collection: SampleMetadataCollection = gcnvkernel.SampleMetadataCollection()
    gcnvkernel.io_metadata.update_sample_metadata_collection_from_ploidy_determination_calls(
        sample_metadata_collection, ploidy_calls_path)

    # load denoising calls
    gcnv_call_set_dict = dict()
    for call_path_idx, call_path in enumerate(gcnvkernel_denoising_chunks_call_paths):
        _logger.info("Loading chunk {0}/{1}...".format(call_path_idx + 1, len(gcnvkernel_denoising_chunks_call_paths)))
        call_interval_list_file = os.path.join(call_path, gcnvkernel.io_consts.default_interval_list_filename)
        call_interval_list = gcnvkernel.io_intervals_and_counts.load_interval_list_tsv_file(call_interval_list_file)
        for subdir, _, _ in os.walk(call_path):
            if subdir.find(gcnvkernel.io_consts.sample_folder_prefix) < 0:
                continue
            sample_name = gcnvkernel.io_commons.get_sample_name_from_txt_file(subdir)
            if sample_name not in gcnv_call_set_dict:
                sample_gcnv_call_set = GenericCNVCallSet(sample_name, tags={"from gCNV"})
                gcnv_call_set_dict[sample_name] = sample_gcnv_call_set
            else:
                sample_gcnv_call_set = gcnv_call_set_dict[sample_name]
            for variant in get_cnv_segments_in_calls_path(
                    call_interval_list, subdir, sample_metadata_collection,
                    allosomal_contigs=allosomal_contigs,
                    default_autosomal_ploidy=autosomal_ref_copy_number):
                sample_gcnv_call_set.add(variant)

    # calculate variant frequencies
    _logger.info("Calculating variant frequencies...")
    contigs_set = gcnv_call_set_dict.values().__iter__().__next__().contigs_set
    sample_names = list(gcnv_call_set_dict.keys())
    for contig in contigs_set:
        generators = [gcnv_call_set_dict[sample_name].iter_in_contig(contig) for sample_name in sample_names]
        while True:
            try:
                variants = [generator.__next__() for generator in generators]
                assert len(set(variants)) == 1  # assert that the variants indeed come from the same interval
                num_var_samples = sum([variant.is_var for variant in variants])
                variant_frequency = float(num_var_samples) / len(variants)
                for variant in variants:
                    variant.variant_frequency = variant_frequency
            except StopIteration:
                break

    # step 4. non-variant "variants" are not needed anymore -- remove them from the interval tree
    _logger.info("Removing non-variant loci from call sets...")
    var_only_gcnv_call_set_dict = dict()
    for sample_name, gcnv_call_set in gcnv_call_set_dict.items():
        var_only_gcnv_call_set = GenericCNVCallSet(sample_name)
        for contig in gcnv_call_set.contigs_set:
            for variant in gcnv_call_set.iter_in_contig(contig):
                if variant.is_var:
                    var_only_gcnv_call_set.add(variant)
        var_only_gcnv_call_set.tags.add("Removed non-variant calls")
        var_only_gcnv_call_set_dict[sample_name] = var_only_gcnv_call_set

    return var_only_gcnv_call_set_dict
