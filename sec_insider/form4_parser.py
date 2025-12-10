"""Parse Form 4 filings into normalized transactions."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from decimal import Decimal
from typing import List, Dict

TRANSACTION_CODE_MAP = {
    "P": "BUY",
    "S": "SELL",
    "M": "EXERCISE",
}


def _get_text(elem: ET.Element | None) -> str | None:
    if elem is None or elem.text is None:
        return None
    return elem.text.strip()


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(value)
    except Exception:
        return None


def _derive_relationship(rel_elem: ET.Element | None) -> str | None:
    if rel_elem is None:
        return None
    parts = []
    for tag in [
        "isDirector",
        "isOfficer",
        "isTenPercentOwner",
        "isOther",
        "officerTitle",
    ]:
        value = _get_text(rel_elem.find(tag))
        if value:
            if value.lower() in {"1", "true", "yes"}:
                parts.append(tag.replace("is", "").replace("TenPercent", "10% "))
            elif tag == "officerTitle":
                parts.append(value)
    return ", ".join(parts) if parts else None


def _transaction_type_from_code(code: str | None) -> str:
    if not code:
        return "OTHER"
    return TRANSACTION_CODE_MAP.get(code.upper(), "OTHER")


def _parse_transactions(root: ET.Element, issuer_info: Dict, owner_info: Dict, filing_date: str | None) -> List[Dict]:
    transactions: List[Dict] = []
    period_of_report = _get_text(root.find("periodOfReport"))
    accession_number = _get_text(root.find("accessionNumber")) or _get_text(root.find("documentType"))

    tables = [
        ("nonDerivativeTable", "nonDerivativeTransaction"),
        ("derivativeTable", "derivativeTransaction"),
    ]

    for table_tag, txn_tag in tables:
        table = root.find(table_tag)
        if table is None:
            continue
        for txn in table.findall(txn_tag):
            date_text = _get_text(txn.find("transactionDate/value"))
            code = _get_text(txn.find("transactionCoding/transactionCode"))
            security_title = _get_text(txn.find("securityTitle/value"))
            shares = _parse_decimal(_get_text(txn.find("transactionAmounts/transactionShares/value")))
            price = _parse_decimal(_get_text(txn.find("transactionAmounts/transactionPricePerShare/value")))
            shares_after = _parse_decimal(
                _get_text(txn.find("postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
            )
            ownership_type = _get_text(txn.find("ownershipNature/directOrIndirectOwnership/value"))

            txn_value = None
            if shares is not None and price is not None:
                txn_value = shares * price

            transactions.append(
                {
                    "issuer_cik": issuer_info.get("cik"),
                    "issuer_name": issuer_info.get("name"),
                    "issuer_ticker": issuer_info.get("ticker"),
                    "filing_date": filing_date,
                    "period_of_report": period_of_report,
                    "form_type": "4",
                    "accession_number": accession_number,
                    "owner_cik": owner_info.get("cik"),
                    "owner_name": owner_info.get("name"),
                    "owner_relationship": owner_info.get("relationship"),
                    "transaction_date": date_text,
                    "security_title": security_title,
                    "transaction_code": code,
                    "transaction_type": _transaction_type_from_code(code),
                    "shares_traded": shares,
                    "share_price": price,
                    "transaction_value_usd": txn_value,
                    "shares_owned_after": shares_after,
                    "ownership_type": ownership_type,
                }
            )
    return transactions


def parse_form4(raw_filing: str) -> List[Dict]:
    """Parse a raw Form 4 filing (XML) into transaction dictionaries."""
    try:
        root = ET.fromstring(raw_filing)
    except ET.ParseError:
        return []

    issuer_info = {
        "cik": _get_text(root.find("issuer/issuerCik")),
        "name": _get_text(root.find("issuer/issuerName")),
        "ticker": _get_text(root.find("issuer/issuerTradingSymbol")),
    }

    reporting_owner = root.find("reportingOwner")
    owner_info = {
        "cik": _get_text(reporting_owner.find("reportingOwnerId/rptOwnerCik")) if reporting_owner else None,
        "name": _get_text(reporting_owner.find("reportingOwnerId/rptOwnerName")) if reporting_owner else None,
        "relationship": _derive_relationship(
            reporting_owner.find("reportingOwnerRelationship") if reporting_owner else None
        ),
    }

    filing_date = _get_text(root.find("periodOfReport"))
    return _parse_transactions(root, issuer_info, owner_info, filing_date)


__all__ = ["parse_form4"]