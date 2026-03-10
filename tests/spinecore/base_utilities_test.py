from unittest.case import TestCase

from eps_spine_shared.spinecore.base_utilities import handle_encoding_oddities, quoted


class HandleEncodingOdditiesTest(TestCase):
    """Test that handle_encoding_oddities handles encoding oddities"""

    def test_basic_ascii(self):
        """test that basic ascii is unchanged"""
        self.assertEqual(handle_encoding_oddities(b"simple ascii"), "simple ascii")

    def test_basic_unicode(self):
        """test that basic unicode (ascii compatible) is unchanged"""
        self.assertEqual(handle_encoding_oddities("simple unicode"), "simple unicode")

    def test_invalid_utf8(self):
        """test that invalid UTF-8 sequences are replaced with ?"""
        self.assertEqual(handle_encoding_oddities(b"valid \xe2sc\xef\xec"), "valid ?sc??")

    def test_invalid_utf8_attempt_replacement(self):
        """test that invalid UTF-8 sequences are replaced with characters where possible"""
        self.assertEqual(handle_encoding_oddities(b"valid \xe2sc\xef\xec", True), "valid ascii")

    def test_valid_utf8(self):
        """test that valid utf-8 has accents stripped"""
        self.assertEqual(
            handle_encoding_oddities("valid \u00e2sc\u00ef\u00ec".encode("utf8")), "valid ascii"
        )

    def test_valid_utf8_attempt_replacement(self):
        """test that the attempt replacement option has no effect for utf8"""
        self.assertEqual(
            handle_encoding_oddities("valid \u00e2sc\u00ef\u00ec".encode("utf8"), True),
            "valid ascii",
        )

    def test_valid_utf8_with_non_spacing_mark(self):
        """test that utf8 with a non-spacing mark is also handled"""
        self.assertEqual(
            handle_encoding_oddities("valid \u00e2sc\u00efi\u0300".encode("utf8")), "valid ascii"
        )

    def test_valid_unicode(self):
        """test that a unicode native string is returned with accents removed"""
        self.assertEqual(handle_encoding_oddities("valid \u00e2sc\u00ef\u00ec"), "valid ascii")

    def test_not_a_string(self):
        """test that non-strings are stringified"""
        self.assertEqual(handle_encoding_oddities(123), "123")


class QuotedTest(TestCase):
    """Test that quoted returns the value as a string surrounded by double quotes"""

    def test_basic_string(self):
        """test that a basic string is quoted"""
        self.assertEqual(quoted("simple string"), '"simple string"')

    def test_non_string(self):
        """test that a non-string is stringified and quoted"""
        self.assertEqual(quoted(123), '"123"')
