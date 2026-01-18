"""
CompIQ Financial ETL Pipeline

Runs financial enrichment in batch mode and persists
results using the existing Database schema.
"""

from typing import List, Dict
import logging
from datetime import datetime

from financial_data import FinancialDataEnricher
from database import Database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FinancialETLPipeline:
    def __init__(self, db_path: str = "comparables.db"):
        self.enricher = FinancialDataEnricher()
        self.db = Database(db_path=db_path)

    def extract_and_transform(self, companies: List[Dict]) -> List[Dict]:
        """
        Extract + transform financial data using Yahoo Finance.
        """
        logger.info("ETL: Extract + Transform")
        return self.enricher.enrich_batch(companies, show_progress=True)

    def load(self, enriched_companies: List[Dict]) -> int:
        """
        Load enriched company data using existing save_search logic.
        """
        logger.info("ETL: Load")

        metadata = {
            "source": "etl",
            "run_type": "financial_enrichment",
            "timestamp": datetime.utcnow().isoformat(),
            "num_records": len(enriched_companies),
        }

        search_id = self.db.save_search(
            target_name="ETL_FINANCIAL_INGEST",
            target_data={},
            comparables=enriched_companies,
            metadata=metadata,
        )

        logger.info(f"ETL completed. search_id={search_id}")
        return search_id

    def run(self, companies: List[Dict]) -> int:
        enriched = self.extract_and_transform(companies)
        return self.load(enriched)


if __name__ == "__main__":
    companies = [
        {"name": "Apple Inc.", "ticker": "AAPL", "exchange": "NASDAQ"},
        {"name": "Microsoft", "ticker": "MSFT", "exchange": "NASDAQ"},
        {"name": "Amazon", "ticker": "AMZN", "exchange": "NASDAQ"},
    ]

    pipeline = FinancialETLPipeline()
    pipeline.run(companies)
