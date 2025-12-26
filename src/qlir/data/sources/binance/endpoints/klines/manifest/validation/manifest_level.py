
from qlir.utils.str.color import Ansi, colorize
import logging 
log = logging.getLogger(__name__)

def validate_manifest(manifest):
    log.info(colorize("Manifest structure validation not fully implemented", Ansi.BOLD, Ansi.YELLOW))
    return
    errors = []
    if "slices" not in manifest:
        raise RuntimeError(
        "Manifest is missing required 'slices' key — cannot evaluate state"
        )

    slices = manifest['slices']
    for slice in slices:
        validate_manifest_slice(slice)
    ensure_uniform_canonical_sizing(slices)

    if errors:
        raise RuntimeError("Manifest structure is invalid", errors)


def has_expected_canonical_slice_count():
    '''Does the actual count of slices match the expected count of slices (the canonical list)'''


def do_all_slices_have_same_top_level_metadata(slices):
    
    structures = get_distinct_top_level_metadata_structures_and_group_by_key(slices)
    if len(structures):
        return False


def get_distinct_top_level_metadata_structures_and_group_by_key(slices):
    """
    Docstring for get_distinct_top_level_metadata_structures
    
    Returns 
    [{
    shape: {}
    keys: []
    }, ...]
    """
    return {}
