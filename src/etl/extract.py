"""ETL — Extract layer.

Receives the raw dictionary returned by ViaCepAdapter and extracts only
the relevant address fields, returning a clean, typed dictionary.
"""

from typing import Any, Dict


FIELDS = ("cep", "logradouro", "complemento", "bairro", "localidade", "uf", "ddd")


def extract(raw: Dict[str, Any]) -> Dict[str, str]:
    """Extract address fields from the raw ViaCEP response object.

    Args:
        raw: Raw dictionary returned by ViaCepAdapter.get().

    Returns:
        Dictionary containing only the relevant address fields.
        Fields missing from the API response default to an empty string.
    """
    return {field: raw.get(field, "") for field in FIELDS}
