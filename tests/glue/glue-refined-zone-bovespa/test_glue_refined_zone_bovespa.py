import unittest
from unittest.mock import patch, MagicMock
import sys

sys.path.append('src/glue/glue-refined-zone-bovespa')
import glue_refined_zone_bovespa as grzb

class TestGlueRefinedZoneBovespa(unittest.TestCase):
    def test_validate_params(self):
        args = {
            'JOB_NAME': 'job',
            'TABLE_NAME': 'table',
            'DATABASE_NAME': 'db',
            'S3_BUCKET': 'bucket',
            'OBJECT_KEY': 'key',
            'S3_OUTPUT_BUCKET': 'bucket',
            'S3_OUTPUT_PREFIX': 'prefix',
            'AWS_REGION': 'us-east-1'
        }
        grzb.validate_params(args)  # Should not raise

        with self.assertRaises(ValueError):
            grzb.validate_params({})

    def test_validate_schema(self):
        class DummyDF:
            columns = ["a", "b"]
        grzb.validate_schema(DummyDF(), ["a", "b"])
        with self.assertRaises(ValueError):
            grzb.validate_schema(DummyDF(), ["a", "b", "c"])

    # Adicione mais testes para funções puras conforme necessário

if __name__ == "__main__":
    unittest.main()