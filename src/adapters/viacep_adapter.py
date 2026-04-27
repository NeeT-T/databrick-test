"""Adapter for the ViaCEP public API (https://viacep.com.br)."""

import urllib.request
import json
from typing import Dict, Any


class ViaCepAdapter:
    """HTTP adapter that fetches address information for a given Brazilian CEP.

    The ViaCEP API returns a JSON object with fields such as:
        cep, logradouro, complemento, bairro, localidade, uf, ibge, gia, ddd, siafi
    """

    BASE_URL = "https://viacep.com.br/ws/{cep}/json/"

    def __init__(self, timeout: int = 10):
        self.timeout = timeout

    def get(self, cep: str) -> Dict[str, Any]:
        """Fetch address data for a given CEP.

        Args:
            cep: Brazilian postal code (8 digits, with or without hyphen).

        Returns:
            Dictionary with address fields returned by ViaCEP.

        Raises:
            ValueError: If the CEP is not found or the response contains an error.
            RuntimeError: If the HTTP request fails.
        """
        clean_cep = cep.replace("-", "").strip()
        url = self.BASE_URL.format(cep=clean_cep)

        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except Exception as exc:
            raise RuntimeError(f"Request to ViaCEP failed for CEP '{cep}': {exc}") from exc

        data: Dict[str, Any] = json.loads(raw)

        if data.get("erro"):
            raise ValueError(f"CEP '{cep}' not found in ViaCEP.")

        return data
