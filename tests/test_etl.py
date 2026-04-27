"""Unit tests for the ViaCEP ETL pipeline.

Tests are designed to run without network access by mocking HTTP calls.
"""

import json
import sys
import os
import unittest
from unittest.mock import MagicMock, patch

# Ensure the project root is on sys.path so `src` is importable.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.adapters.viacep_adapter import ViaCepAdapter
from src.etl.extract import extract
from src.etl.transform import transform
from src.etl.load import load


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RAW = {
    "cep": "01310-100",
    "logradouro": "Avenida Paulista",
    "complemento": "de 1 a 610 - lado par",
    "bairro": "Bela Vista",
    "localidade": "São Paulo",
    "uf": "SP",
    "ibge": "3550308",
    "gia": "1004",
    "ddd": "11",
    "siafi": "7107",
}

SAMPLE_EXTRACTED = {
    "cep": "01310-100",
    "logradouro": "Avenida Paulista",
    "complemento": "de 1 a 610 - lado par",
    "bairro": "Bela Vista",
    "localidade": "São Paulo",
    "uf": "SP",
    "ddd": "11",
}


# ---------------------------------------------------------------------------
# Adapter tests
# ---------------------------------------------------------------------------

class TestViaCepAdapter(unittest.TestCase):

    def _make_mock_response(self, data: dict):
        raw_bytes = json.dumps(data).encode("utf-8")
        mock_response = MagicMock()
        mock_response.read.return_value = raw_bytes
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)
        return mock_response

    @patch("urllib.request.urlopen")
    def test_get_returns_dict(self, mock_urlopen):
        mock_urlopen.return_value = self._make_mock_response(SAMPLE_RAW)
        adapter = ViaCepAdapter()
        result = adapter.get("01310-100")
        self.assertEqual(result["cep"], "01310-100")
        self.assertEqual(result["localidade"], "São Paulo")

    @patch("urllib.request.urlopen")
    def test_get_strips_hyphen_from_cep(self, mock_urlopen):
        mock_urlopen.return_value = self._make_mock_response(SAMPLE_RAW)
        adapter = ViaCepAdapter()
        adapter.get("01310-100")
        called_url = mock_urlopen.call_args[0][0]
        self.assertIn("01310100", called_url)
        self.assertNotIn("-", called_url)

    @patch("urllib.request.urlopen")
    def test_get_raises_value_error_on_invalid_cep(self, mock_urlopen):
        mock_urlopen.return_value = self._make_mock_response({"erro": True})
        adapter = ViaCepAdapter()
        with self.assertRaises(ValueError):
            adapter.get("00000-000")

    @patch("urllib.request.urlopen")
    def test_get_raises_runtime_error_on_network_failure(self, mock_urlopen):
        mock_urlopen.side_effect = OSError("connection refused")
        adapter = ViaCepAdapter()
        with self.assertRaises(RuntimeError):
            adapter.get("01310-100")


# ---------------------------------------------------------------------------
# Extract tests
# ---------------------------------------------------------------------------

class TestExtract(unittest.TestCase):

    def test_extract_returns_expected_fields(self):
        result = extract(SAMPLE_RAW)
        self.assertEqual(set(result.keys()), {"cep", "logradouro", "complemento", "bairro", "localidade", "uf", "ddd"})

    def test_extract_values_match_raw(self):
        result = extract(SAMPLE_RAW)
        self.assertEqual(result["cep"], "01310-100")
        self.assertEqual(result["uf"], "SP")
        self.assertEqual(result["ddd"], "11")

    def test_extract_missing_field_defaults_to_empty_string(self):
        result = extract({"cep": "01310-100"})
        self.assertEqual(result["logradouro"], "")
        self.assertEqual(result["complemento"], "")

    def test_extract_drops_extra_fields(self):
        result = extract(SAMPLE_RAW)
        self.assertNotIn("ibge", result)
        self.assertNotIn("siafi", result)


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------

class TestTransform(unittest.TestCase):

    def test_transform_returns_string(self):
        result = transform(SAMPLE_EXTRACTED)
        self.assertIsInstance(result, str)

    def test_transform_contains_cep(self):
        result = transform(SAMPLE_EXTRACTED)
        self.assertIn("01310-100", result)

    def test_transform_contains_city_and_uf(self):
        result = transform(SAMPLE_EXTRACTED)
        self.assertIn("São Paulo", result)
        self.assertIn("SP", result)

    def test_transform_contains_ddd(self):
        result = transform(SAMPLE_EXTRACTED)
        self.assertIn("11", result)

    def test_transform_omits_ddd_when_empty(self):
        data = {**SAMPLE_EXTRACTED, "ddd": ""}
        result = transform(data)
        self.assertNotIn("DDD", result)

    def test_transform_omits_complemento_when_empty(self):
        data = {**SAMPLE_EXTRACTED, "complemento": ""}
        result = transform(data)
        # complemento was non-empty in sample; empty version should not add extra comma
        self.assertNotIn("lado par", result)


# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------

class TestLoad(unittest.TestCase):

    def test_load_fallback_when_no_queue_url(self):
        result = load("Test address string")
        self.assertEqual(result["status"], "printed_to_stdout")
        self.assertEqual(result["message"], "Test address string")

    @patch.dict(os.environ, {"SQS_QUEUE_URL": ""})
    def test_load_fallback_when_env_empty(self):
        result = load("Test address string")
        self.assertEqual(result["status"], "printed_to_stdout")

    @patch.dict(os.environ, {"SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"})
    def test_load_sends_to_sqs_when_configured(self):
        # Access the module directly to avoid shadowing by src.etl.__init__
        import importlib
        load_module = importlib.import_module("src.etl.load")

        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.send_message.return_value = {"MessageId": "abc-123"}

        original_boto3 = load_module.boto3
        original_available = load_module._BOTO3_AVAILABLE
        try:
            load_module.boto3 = mock_boto3
            load_module._BOTO3_AVAILABLE = True
            result = load_module.load("Test address string")
        finally:
            load_module.boto3 = original_boto3
            load_module._BOTO3_AVAILABLE = original_available

        mock_client.send_message.assert_called_once()
        self.assertEqual(result["status"], "sent_to_sqs")
        self.assertEqual(result["MessageId"], "abc-123")

    @patch.dict(os.environ, {"SQS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"})
    def test_load_falls_back_when_boto3_missing(self):
        import importlib
        load_module = importlib.import_module("src.etl.load")

        original_boto3 = load_module.boto3
        original_available = load_module._BOTO3_AVAILABLE
        try:
            load_module.boto3 = None
            load_module._BOTO3_AVAILABLE = False
            result = load_module.load("Test address string")
        finally:
            load_module.boto3 = original_boto3
            load_module._BOTO3_AVAILABLE = original_available

        self.assertEqual(result["status"], "printed_to_stdout")


if __name__ == "__main__":
    unittest.main()
