"""
CompIQ Financial ETL Pipeline
Uses schemas for validation and observability for logging/metrics.
"""
import time
import hashlib
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

# Import our modules
from schemas import CompanyInput, FinancialMetrics, DataQuality, validate_company_batch
from observability import get_logger, get_metrics, get_tracer, log_execution

# Import existing modules
from financial_data import FinancialDataEnricher
from database import Database

# Initialize observability
logger = get_logger("compiq.etl")
metrics = get_metrics()
tracer = get_tracer()


class ETLStatus(Enum):
    """ETL job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class ETLMetrics:
    """Metrics for ETL pipeline execution."""
    records_input: int = 0
    records_enriched: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        if self.records_input == 0:
            return 0.0
        return self.records_enriched / self.records_input
    
    @property
    def throughput(self) -> float:
        if self.duration_seconds == 0:
            return 0.0
        return self.records_input / self.duration_seconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "records_input": self.records_input,
            "records_enriched": self.records_enriched,
            "records_failed": self.records_failed,
            "records_skipped": self.records_skipped,
            "success_rate": f"{self.success_rate:.2%}",
            "duration_seconds": round(self.duration_seconds, 2),
            "throughput_rps": round(self.throughput, 2),
            "errors_count": len(self.errors)
        }


@dataclass 
class ETLResult:
    """Result of an ETL pipeline run."""
    search_id: int
    status: ETLStatus
    metrics: ETLMetrics
    run_hash: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "search_id": self.search_id,
            "status": self.status.value,
            "metrics": self.metrics.to_dict(),
            "run_hash": self.run_hash,
            "timestamp": self.timestamp.isoformat()
        }


class FinancialETLPipeline:
    """
    Production-ready ETL pipeline with full observability.
    """
    
    def __init__(
        self,
        db_path: str = "comparables.db",
        batch_size: int = 10,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.enricher = FinancialDataEnricher()
        self.db = Database(db_path=db_path)
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.etl_metrics = ETLMetrics()
    
    @log_execution(logger, include_args=False, include_result=False)
    def run(self, companies: List[Dict]) -> ETLResult:
        """Execute the full ETL pipeline with tracing."""
        
        with tracer.trace("etl.pipeline", {"companies": str(len(companies))}):
            self.etl_metrics = ETLMetrics(
                records_input=len(companies),
                start_time=datetime.utcnow()
            )
            
            run_hash = self._generate_run_hash(companies)
            
            logger.info(
                "ETL pipeline started",
                records=len(companies),
                run_hash=run_hash[:8],
                batch_size=self.batch_size
            )
            
            # Track in global metrics
            metrics.increment("etl.runs.started")
            metrics.gauge("etl.current.records", len(companies))
            
            try:
                # Validate input using schemas
                with tracer.trace("etl.validate"):
                    valid_companies, validation_errors = self._validate_with_schemas(companies)
                    
                    if validation_errors:
                        logger.warning(
                            "Validation errors",
                            error_count=len(validation_errors),
                            errors=validation_errors[:5]  # Log first 5
                        )
                        self.etl_metrics.records_skipped = len(validation_errors)
                
                # Extract & Transform
                with tracer.trace("etl.extract_transform"):
                    enriched = self._extract_and_transform(valid_companies)
                
                # Load
                with tracer.trace("etl.load"):
                    search_id = self._load(enriched, run_hash)
                
                # Determine status
                if self.etl_metrics.records_failed == 0:
                    status = ETLStatus.COMPLETED
                elif self.etl_metrics.records_enriched > 0:
                    status = ETLStatus.PARTIAL
                else:
                    status = ETLStatus.FAILED
                
                metrics.increment(f"etl.runs.{status.value}")
                
            except Exception as e:
                logger.exception("ETL pipeline failed", e)
                self.etl_metrics.errors.append({"pipeline_error": str(e)})
                status = ETLStatus.FAILED
                search_id = -1
                metrics.increment("etl.runs.error")
            
            finally:
                self.etl_metrics.end_time = datetime.utcnow()
                
                # Record timing
                metrics.timer("etl.duration", self.etl_metrics.duration_seconds * 1000)
                metrics.histogram("etl.success_rate", self.etl_metrics.success_rate)
            
            result = ETLResult(
                search_id=search_id,
                status=status,
                metrics=self.etl_metrics,
                run_hash=run_hash
            )
            
            logger.info(
                "ETL pipeline completed",
                status=status.value,
                search_id=search_id,
                duration_seconds=self.etl_metrics.duration_seconds,
                success_rate=f"{self.etl_metrics.success_rate:.2%}"
            )
            
            return result
    
    def _validate_with_schemas(self, companies: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Validate companies using Pydantic schemas."""
        valid = []
        errors = []
        
        for i, company in enumerate(companies):
            try:
                # Validate with Pydantic
                validated = CompanyInput(
                    name=company.get('name', ''),
                    ticker=company.get('ticker', ''),
                    exchange=company.get('exchange', 'OTHER'),
                    description=company.get('description'),
                    homepage_url=company.get('homepage_url')
                )
                valid.append(validated.model_dump())
                
            except Exception as e:
                errors.append({
                    "index": i,
                    "ticker": company.get('ticker', 'unknown'),
                    "error": str(e)
                })
                self.etl_metrics.errors.append({
                    "type": "validation",
                    "ticker": company.get('ticker'),
                    "error": str(e)
                })
        
        return valid, errors
    
    def _extract_and_transform(self, companies: List[Dict]) -> List[Dict]:
        """Extract and transform with batch tracking."""
        
        logger.info(
            "Starting extract & transform",
            batch_size=self.batch_size,
            total_companies=len(companies)
        )
        
        enriched_all = []
        
        for i in range(0, len(companies), self.batch_size):
            batch = companies[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(companies) + self.batch_size - 1) // self.batch_size
            
            with tracer.trace("etl.batch", {"batch": str(batch_num)}):
                logger.info(
                    "Processing batch",
                    batch=batch_num,
                    total=total_batches,
                    size=len(batch)
                )
                
                try:
                    enriched_batch = self._process_batch_with_retry(batch)
                    enriched_all.extend(enriched_batch)
                    
                    # Count by data quality
                    for company in enriched_batch:
                        fin = company.get('financials', {})
                        quality = fin.get('data_quality', 'unavailable')
                        
                        if quality == 'unavailable':
                            self.etl_metrics.records_failed += 1
                        else:
                            self.etl_metrics.records_enriched += 1
                    
                    metrics.increment("etl.batches.success")
                    
                except Exception as e:
                    logger.error(
                        "Batch failed",
                        batch=batch_num,
                        error=str(e)
                    )
                    self.etl_metrics.records_failed += len(batch)
                    self.etl_metrics.errors.append({
                        "type": "batch",
                        "batch": batch_num,
                        "error": str(e)
                    })
                    metrics.increment("etl.batches.error")
        
        return enriched_all
    
    def _process_batch_with_retry(self, batch: List[Dict]) -> List[Dict]:
        """Process batch with retry logic."""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                return self.enricher.enrich_batch(batch, show_progress=False)
            except Exception as e:
                last_error = e
                logger.warning(
                    "Batch attempt failed",
                    attempt=attempt + 1,
                    max_attempts=self.max_retries,
                    error=str(e)
                )
                metrics.increment("etl.retries")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
        
        raise last_error
    
    def _load(self, enriched_companies: List[Dict], run_hash: str) -> int:
        """Load to database."""
        
        logger.info("Loading to database", records=len(enriched_companies))
        
        metadata = {
            "source": "etl_pipeline_v2",
            "run_type": "financial_enrichment",
            "run_hash": run_hash,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": self.etl_metrics.to_dict(),
            "pipeline_version": "2.0.0"
        }
        
        search_id = self.db.save_search(
            target_name=f"ETL_RUN_{run_hash[:8]}",
            target_data={"type": "etl_batch", "run_hash": run_hash},
            comparables=enriched_companies,
            metadata=metadata
        )
        
        metrics.increment("etl.records.loaded", value=len(enriched_companies))
        
        return search_id
    
    def _generate_run_hash(self, companies: List[Dict]) -> str:
        """Generate deterministic hash."""
        sorted_companies = sorted(
            companies,
            key=lambda x: (x.get("ticker", ""), x.get("exchange", ""))
        )
        payload = json.dumps(sorted_companies, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()
    
    def validate_input(self, companies: List[Dict]) -> tuple[bool, List[str]]:
        """Validate input data."""
        errors = []
        
        if not companies:
            errors.append("Empty company list")
            return False, errors
        
        for i, company in enumerate(companies):
            if not company.get('ticker'):
                errors.append(f"Company {i}: missing ticker")
            if not company.get('exchange'):
                errors.append(f"Company {i}: missing exchange")
        
        return len(errors) == 0, errors


# Convenience function
def run_financial_etl(
    companies: List[Dict],
    db_path: str = "comparables.db"
) -> Dict[str, Any]:
    """Run financial ETL pipeline."""
    pipeline = FinancialETLPipeline(db_path=db_path)
    result = pipeline.run(companies)
    return result.to_dict()


if __name__ == "__main__":
    # Test run
    test_companies = [
        {"name": "Apple Inc.", "ticker": "AAPL", "exchange": "NASDAQ"},
        {"name": "Microsoft", "ticker": "MSFT", "exchange": "NASDAQ"},
        {"name": "Google", "ticker": "GOOGL", "exchange": "NASDAQ"},
    ]
    
    result = run_financial_etl(test_companies)
    print(json.dumps(result, indent=2))
