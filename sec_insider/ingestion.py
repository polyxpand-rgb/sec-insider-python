"""Ingest Form 4 filings into the database."""
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Iterable

from sqlalchemy.exc import IntegrityError

from .models import Company, Insider, InsiderTransaction, get_session
from .sec_client import fetch_form4_filings_metadata, fetch_form4_raw
from .form4_parser import parse_form4


def _normalize_name(name: str | None) -> str | None:
    return name.lower().strip() if name else None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _get_or_create_company(session, issuer_cik: str, name: str, ticker: str | None):
    company = session.query(Company).filter_by(issuer_cik=issuer_cik).one_or_none()
    if company is None:
        company = Company(issuer_cik=issuer_cik, name=name, ticker=ticker)
        session.add(company)
        session.flush()
    else:
        if ticker and company.ticker != ticker:
            company.ticker = ticker
        if name and company.name != name:
            company.name = name
    return company


def _get_or_create_insider(session, owner_cik: str | None, owner_name: str):
    normalized_name = _normalize_name(owner_name)
    query = session.query(Insider)
    if owner_cik:
        insider = query.filter_by(owner_cik=owner_cik).one_or_none()
    else:
        insider = query.filter_by(normalized_name=normalized_name).one_or_none()
    if insider is None:
        insider = Insider(owner_cik=owner_cik, name=owner_name, normalized_name=normalized_name)
        session.add(insider)
        session.flush()
    else:
        if owner_cik and insider.owner_cik is None:
            insider.owner_cik = owner_cik
    return insider


def _create_transaction(session, company: Company, insider: Insider, txn: Dict):
    filing_date = _parse_date(txn.get("filing_date")) or _parse_date(txn.get("period_of_report"))
    transaction_date = _parse_date(txn.get("transaction_date"))
    if not filing_date or not transaction_date:
        return

    record = InsiderTransaction(
        company_id=company.id,
        insider_id=insider.id,
        filing_date=filing_date,
        transaction_date=transaction_date,
        form_type=txn.get("form_type", "4"),
        transaction_code=txn.get("transaction_code"),
        transaction_type=txn.get("transaction_type"),
        insider_relationship=txn.get("owner_relationship"),
        security_title=txn.get("security_title"),
        shares_traded=txn.get("shares_traded"),
        share_price=txn.get("share_price"),
        transaction_value_usd=txn.get("transaction_value_usd"),
        shares_owned_after=txn.get("shares_owned_after"),
        ownership_type=txn.get("ownership_type"),
        accession_number=txn.get("accession_number"),
    )
    session.add(record)


def _commit_with_retry(session) -> None:
    try:
        session.commit()
    except IntegrityError:
        session.rollback()


def _apply_metadata_defaults(transactions: Iterable[Dict], metadata: Dict) -> Iterable[Dict]:
    for txn in transactions:
        txn.setdefault("filing_date", (metadata.get("filed_at") or "")[:10])
        txn.setdefault("accession_number", metadata.get("accession_no"))
        txn.setdefault("form_type", metadata.get("form_type") or "4")
        yield txn


def ingest_form4_filings(start_date: date, end_date: date) -> None:
    """
    Ingest Form 4 filings between dates into the database.
    """
    session = get_session()
    try:
        metadata_list = fetch_form4_filings_metadata(start_date, end_date)
        for metadata in metadata_list:
            raw = fetch_form4_raw(metadata)
            transactions = _apply_metadata_defaults(parse_form4(raw), metadata)
            for txn in transactions:
                issuer_cik = txn.get("issuer_cik")
                issuer_name = txn.get("issuer_name") or "Unknown"
                company = _get_or_create_company(session, issuer_cik, issuer_name, txn.get("issuer_ticker"))
                insider_name = txn.get("owner_name") or "Unknown"
                insider = _get_or_create_insider(session, txn.get("owner_cik"), insider_name)
                session.flush()
                _create_transaction(session, company, insider, txn)
                _commit_with_retry(session)
    finally:
        session.close()


__all__ = ["ingest_form4_filings"]