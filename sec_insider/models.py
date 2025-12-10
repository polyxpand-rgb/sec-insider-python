"""SQLAlchemy models for SEC insider transactions."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    ForeignKey,
    UniqueConstraint,
    DateTime,
    create_engine,
    func,
)
from sqlalchemy.orm import declarative_base, relationship, Session, sessionmaker

from .config import load_config

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    issuer_cik = Column(String, unique=True, nullable=False)
    ticker = Column(String, nullable=True)
    name = Column(String, nullable=False)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    transactions = relationship("InsiderTransaction", back_populates="company")


class Insider(Base):
    __tablename__ = "insiders"

    id = Column(Integer, primary_key=True)
    owner_cik = Column(String, unique=True, nullable=True)
    name = Column(String, nullable=False)
    normalized_name = Column(String, nullable=False)
    person_type = Column(String, default="INSIDER")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    transactions = relationship("InsiderTransaction", back_populates="insider")


class InsiderTransaction(Base):
    __tablename__ = "insider_transactions"
    __table_args__ = (
        UniqueConstraint("accession_number", "insider_id", "transaction_date", "shares_traded"),
    )

    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id"))
    insider_id = Column(Integer, ForeignKey("insiders.id"))
    filing_date = Column(Date, nullable=False)
    transaction_date = Column(Date, nullable=False)
    form_type = Column(String, nullable=False)
    transaction_code = Column(String, nullable=False)
    transaction_type = Column(String, nullable=False)
    insider_relationship = Column(String, nullable=True)
    security_title = Column(String, nullable=True)
    shares_traded = Column(Numeric, nullable=True)
    share_price = Column(Numeric, nullable=True)
    transaction_value_usd = Column(Numeric, nullable=True)
    shares_owned_after = Column(Numeric, nullable=True)
    ownership_type = Column(String, nullable=True)
    accession_number = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())

    company = relationship("Company", back_populates="transactions")
    insider = relationship("Insider", back_populates="transactions")


_engine = None
_SessionLocal = None


def _get_engine():
    global _engine
    if _engine is None:
        config = load_config()
        _engine = create_engine(config.database_url, echo=False, future=True)
    return _engine


def get_session() -> Session:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=_get_engine(), autoflush=False, autocommit=False)
    return _SessionLocal()


def init_db():
    engine = _get_engine()
    Base.metadata.create_all(engine)


__all__ = [
    "Base",
    "Company",
    "Insider",
    "InsiderTransaction",
    "get_session",
    "init_db",
]