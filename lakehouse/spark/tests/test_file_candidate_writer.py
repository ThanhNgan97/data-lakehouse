from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

import file_candidate_writer as writer


class FileCandidateWriterTest(unittest.TestCase):
    @patch("file_candidate_writer.F.explode")
    def test_unwraps_api_json_data_envelope(self, explode):
        raw = MagicMock()
        raw.schema.fields = [
            StructField("data", ArrayType(StructType([StructField("record_id", StringType())])))
        ]
        nested = raw.select.return_value
        explode.return_value.alias.return_value = "record"

        result = writer._unwrap_data_envelope(raw)

        explode.assert_called_once_with("data")
        raw.select.assert_called_once_with("record")
        nested.select.assert_called_once_with("record.*")
        self.assertIs(result, nested.select.return_value)


if __name__ == "__main__":
    unittest.main()
