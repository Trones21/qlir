import hashlib

from qlir.data.sources.common.slices.slice_key import SliceKey


def make_canonical_slice_hash(slice_key: SliceKey) -> str:
    """
    Stable ID derived from the composite_key, used as filename.
    """
    key = slice_key.canonical_slice_composite_key().encode("utf-8")
    return hashlib.blake2b(key, digest_size=16).hexdigest()