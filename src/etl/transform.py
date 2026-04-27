"""ETL — Transform layer.

Receives the extracted address dictionary and produces a single,
human-readable string that uniquely describes the address.
"""

from typing import Dict


def transform(extracted: Dict[str, str]) -> str:
    """Transform extracted address fields into a unique formatted string.

    The output format is:
        "[CEP] logradouro, complemento - bairro, localidade/UF (DDD)"

    Empty optional fields (complemento, ddd) are omitted gracefully.

    Args:
        extracted: Dictionary produced by the Extract layer.

    Returns:
        A single string representing the full address.
    """
    cep = extracted.get("cep", "")
    logradouro = extracted.get("logradouro", "")
    complemento = extracted.get("complemento", "")
    bairro = extracted.get("bairro", "")
    localidade = extracted.get("localidade", "")
    uf = extracted.get("uf", "")
    ddd = extracted.get("ddd", "")

    street = logradouro
    if complemento:
        street = f"{street}, {complemento}"
    if bairro:
        street = f"{street} - {bairro}"

    city = f"{localidade}/{uf}" if localidade and uf else localidade or uf

    address = f"[{cep}] {street}, {city}"
    if ddd:
        address = f"{address} (DDD {ddd})"

    return address
