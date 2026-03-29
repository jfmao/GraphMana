"""Tests for BGEN 1.2 export format."""

import struct
import zlib


class TestBGENProbabilityEncoding:
    """Test genotype probability encoding for BGEN."""

    def _encode_gt(self, gt_code):
        """Encode a single genotype as BGEN probabilities (P(RR), P(RA))."""
        max_val = 65535
        if gt_code == 0:  # HomRef
            return (max_val, 0)
        elif gt_code == 1:  # Het
            return (0, max_val)
        elif gt_code == 2:  # HomAlt
            return (0, 0)
        else:  # Missing
            return (0, 0)

    def test_homref_probabilities(self):
        p_rr, p_ra = self._encode_gt(0)
        assert p_rr == 65535
        assert p_ra == 0
        # P(AA) = 65535 - 65535 - 0 = 0
        p_aa = 65535 - p_rr - p_ra
        assert p_aa == 0

    def test_het_probabilities(self):
        p_rr, p_ra = self._encode_gt(1)
        assert p_rr == 0
        assert p_ra == 65535
        p_aa = 65535 - p_rr - p_ra
        assert p_aa == 0

    def test_homalt_probabilities(self):
        p_rr, p_ra = self._encode_gt(2)
        assert p_rr == 0
        assert p_ra == 0
        # P(AA) = 65535 - 0 - 0 = 65535
        p_aa = 65535 - p_rr - p_ra
        assert p_aa == 65535

    def test_missing_probabilities(self):
        p_rr, p_ra = self._encode_gt(3)
        assert p_rr == 0
        assert p_ra == 0


class TestBGENHeaderFormat:
    """Test BGEN header binary format."""

    def test_magic_number(self):
        """BGEN magic number is 4 zero bytes."""
        magic = b"\x00\x00\x00\x00"
        assert len(magic) == 4
        assert magic == bytes(4)

    def test_flags_encoding(self):
        """Test BGEN flags field encoding."""
        compression = 1  # zlib
        layout = 2
        sample_ids_present = 1
        flags = compression | (layout << 2) | (sample_ids_present << 31)
        # Layout 2 with zlib and sample IDs
        assert flags & 0x3 == 1  # compression bits
        assert (flags >> 2) & 0xF == 2  # layout bits
        assert (flags >> 31) & 1 == 1  # sample ID flag

    def test_header_struct_packing(self):
        """Test header can be packed as little-endian uint32s."""
        offset = 100
        header_size = 20
        n_variants = 1000
        n_samples = 50
        packed = struct.pack("<IIII", offset, header_size, n_variants, n_samples)
        assert len(packed) == 16
        unpacked = struct.unpack("<IIII", packed)
        assert unpacked == (100, 20, 1000, 50)

    def test_sample_id_encoding(self):
        """Sample IDs are length-prefixed UTF-8 strings."""
        sample_ids = ["S1", "Sample_2", "NA12878"]
        buf = bytearray()
        for sid in sample_ids:
            sid_bytes = sid.encode("utf-8")
            buf.extend(struct.pack("<H", len(sid_bytes)))
            buf.extend(sid_bytes)
        # Verify we can decode them back
        pos = 0
        decoded = []
        while pos < len(buf):
            length = struct.unpack_from("<H", buf, pos)[0]
            pos += 2
            decoded.append(buf[pos : pos + length].decode("utf-8"))
            pos += length
        assert decoded == sample_ids


class TestBGENVariantBlock:
    """Test BGEN variant data block format."""

    def test_variant_id_encoding(self):
        """Variant IDs are length-prefixed (uint16) strings."""
        varid = "chr1_100_A_T"
        varid_bytes = varid.encode("utf-8")
        packed = struct.pack("<H", len(varid_bytes)) + varid_bytes
        # Decode
        length = struct.unpack_from("<H", packed, 0)[0]
        decoded = packed[2 : 2 + length].decode("utf-8")
        assert decoded == varid

    def test_allele_encoding(self):
        """Alleles are length-prefixed (uint32) strings."""
        ref = "ACGT"
        ref_bytes = ref.encode("utf-8")
        packed = struct.pack("<I", len(ref_bytes)) + ref_bytes
        length = struct.unpack_from("<I", packed, 0)[0]
        decoded = packed[4 : 4 + length].decode("utf-8")
        assert decoded == ref

    def test_position_encoding(self):
        """Position is a uint32."""
        pos = 123456
        packed = struct.pack("<I", pos)
        assert struct.unpack("<I", packed)[0] == 123456

    def test_zlib_compression(self):
        """Probability data is zlib-compressed."""
        data = b"\x00" * 100
        compressed = zlib.compress(data)
        decompressed = zlib.decompress(compressed)
        assert decompressed == data
        assert len(compressed) < len(data)


class TestBGENProbabilityBlock:
    """Test Layout 2 probability data block format."""

    def test_ploidy_byte_encoding(self):
        """Ploidy byte: lower 6 bits = ploidy, bit 7 = missing."""
        # Diploid, not missing
        byte = 2 | (0 << 7)
        assert byte == 2
        assert byte & 0x3F == 2  # ploidy
        assert (byte >> 7) & 1 == 0  # not missing

        # Diploid, missing
        byte = 2 | (1 << 7)
        assert byte == 130
        assert byte & 0x3F == 2  # ploidy
        assert (byte >> 7) & 1 == 1  # missing

    def test_probability_uint16_encoding(self):
        """Probabilities are uint16 values in [0, 65535]."""
        # HomRef: P(RR)=65535, P(RA)=0
        packed = struct.pack("<HH", 65535, 0)
        p_rr, p_ra = struct.unpack("<HH", packed)
        assert p_rr == 65535
        assert p_ra == 0

    def test_bits_per_probability(self):
        """Layout 2 uses 16 bits per probability."""
        bits = 16
        max_val = (1 << bits) - 1
        assert max_val == 65535
