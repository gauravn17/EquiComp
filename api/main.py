"""
CompIQ FastAPI Backend
Connects all modules: ETL, schemas, observability, migrations.

Run with: uvicorn api.main:app --reload --port 8000
"""
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Dict, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager
import logging
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from schemas import (
    CompanyInput, ComparableCompany, ETLJobRequest, ETLJobResult,
    SearchRequest, SearchResult, ValuationRequest, Exchange, DataQuality,
    validate_company_batch
)
from observability import (
    StructuredLogger, MetricsCollector, RequestTracer,
    get_logger, get_metrics, log_execution, track_metrics
)
from migrations import MigrationManager
from etl.pipeline import FinancialETLPipeline, ETLStatus
from database import Database

# ============================================================================
# Initialize Services
# ============================================================================

# Logger
logger = get_logger("compiq.api")

# Metrics
metrics = get_metrics()

# Tracer
tracer = RequestTracer(logger, metrics)

# Database
db = Database()

# ETL Pipeline
pipeline = FinancialETLPipeline()

# Track startup time
startup_time = datetime.utcnow()


# ============================================================================
# Lifespan (startup/shutdown)
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    # Startup
    logger.info("CompIQ API starting", version="2.0.0")
    
    # Run migrations
    try:
        migration_manager = MigrationManager()
        status = migration_manager.get_status()
        
        if status['pending_count'] > 0:
            logger.info(
                "Running pending migrations",
                pending=status['pending_versions']
            )
            applied = migration_manager.upgrade()
            logger.info("Migrations applied", versions=applied)
        else:
            logger.info(
                "Database up to date",
                current_version=status['current_version']
            )
    except Exception as e:
        logger.error("Migration failed", error=str(e))
    
    # Check database
    try:
        stats = db.get_stats()
        logger.info(
            "Database connected",
            total_searches=stats['total_searches'],
            unique_companies=stats['unique_companies']
        )
        metrics.gauge("database.searches_total", stats['total_searches'])
        metrics.gauge("database.companies_total", stats['unique_companies'])
    except Exception as e:
        logger.error("Database connection failed", error=str(e))
    
    yield  # Application runs here
    
    # Shutdown
    logger.info("CompIQ API shutting down")
    
    # Log final metrics
    final_stats = metrics.get_stats()
    logger.info("Final metrics", **final_stats)


# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(
    title="CompIQ API",
    description="""
## CompIQ - AI-Powered Comparable Company Analysis API

### Features:
- 🔄 **ETL Pipeline** - Financial data enrichment from Yahoo Finance
- 📊 **Search History** - Query past analyses  
- 📈 **Statistics** - Database metrics and health checks
- ✅ **Data Validation** - Pydantic schemas for type safety
- 📝 **Observability** - Structured logging and metrics

### Quick Start:
1. POST to `/etl/run` with a list of companies
2. GET `/searches` to view results
3. GET `/health` to check system status
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Middleware for Request Tracking
# ============================================================================

@app.middleware("http")
async def track_requests(request: Request, call_next):
    """Middleware to track all requests."""
    import time
    import uuid
    
    # Generate request ID
    request_id = str(uuid.uuid4())[:8]
    
    # Track request
    metrics.increment("http.requests.total", tags={"path": request.url.path})
    
    start = time.perf_counter()
    
    try:
        response = await call_next(request)
        
        # Record timing
        duration_ms = (time.perf_counter() - start) * 1000
        metrics.timer("http.request.duration", duration_ms, tags={"path": request.url.path})
        
        # Log request
        logger.info(
            "Request completed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2)
        )
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        
        return response
        
    except Exception as e:
        metrics.increment("http.requests.error", tags={"path": request.url.path})
        logger.error(
            "Request failed",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            error=str(e)
        )
        raise


# ============================================================================
# Pydantic Response Models
# ============================================================================

from pydantic import BaseModel, Field

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: datetime
    database: str
    migrations: str
    uptime_seconds: float


class StatsResponse(BaseModel):
    total_searches: int
    unique_companies: int
    api_version: str
    metrics: Dict[str, Any]


class ETLResponse(BaseModel):
    status: str
    search_id: int
    run_hash: str
    metrics: Dict[str, Any]
    timestamp: datetime


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", tags=["General"])
async def root():
    """API root - basic info and links."""
    return {
        "name": "CompIQ API",
        "version": "2.0.0",
        "documentation": "/docs",
        "health": "/health",
        "endpoints": {
            "health": "GET /health",
            "stats": "GET /stats",
            "run_etl": "POST /etl/run",
            "validate": "POST /etl/validate",
            "searches": "GET /searches",
            "search_detail": "GET /searches/{search_id}",
            "migrations": "GET /migrations/status"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["General"])
@track_metrics(metrics, "api.health")
async def health_check():
    """
    Health check endpoint for monitoring and load balancers.
    """
    uptime = (datetime.utcnow() - startup_time).total_seconds()
    
    # Check database
    try:
        db.get_stats()
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    # Check migrations
    try:
        manager = MigrationManager()
        status = manager.get_status()
        migration_status = f"v{status['current_version']}" if status['current_version'] else "none"
        if status['pending_count'] > 0:
            migration_status += f" ({status['pending_count']} pending)"
    except Exception:
        migration_status = "unknown"
    
    return HealthResponse(
        status="healthy" if db_status == "connected" else "degraded",
        version="2.0.0",
        timestamp=datetime.utcnow(),
        database=db_status,
        migrations=migration_status,
        uptime_seconds=round(uptime, 2)
    )


@app.get("/stats", response_model=StatsResponse, tags=["General"])
@track_metrics(metrics, "api.stats")
async def get_statistics():
    """
    Get database and API statistics.
    """
    stats = db.get_stats()
    api_metrics = metrics.get_stats()
    
    return StatsResponse(
        total_searches=stats.get('total_searches', 0),
        unique_companies=stats.get('unique_companies', 0),
        api_version="2.0.0",
        metrics=api_metrics
    )


@app.post("/etl/validate", tags=["ETL"])
async def validate_etl_input(request: ETLJobRequest):
    """
    Validate ETL input without running the pipeline.
    
    Use this to check data before submitting a full ETL job.
    """
    # Convert to dicts
    companies_dict = [c.model_dump() for c in request.companies]
    
    # Validate using pipeline
    is_valid, errors = pipeline.validate_input(companies_dict)
    
    # Also validate with our schema
    valid_companies, schema_errors = validate_company_batch(companies_dict)
    
    all_errors = errors + [e['error'] for e in schema_errors]
    
    return {
        "valid": len(all_errors) == 0,
        "company_count": len(request.companies),
        "valid_count": len(valid_companies),
        "errors": all_errors
    }


@app.post("/etl/run", response_model=ETLResponse, tags=["ETL"])
async def run_etl(request: ETLJobRequest):
    """
    Run financial ETL pipeline.
    
    Extracts financial data from Yahoo Finance, transforms it,
    and loads it into the database.
    """
    logger.info(
        "ETL request received",
        company_count=len(request.companies),
        batch_size=request.batch_size
    )
    
    with tracer.trace("etl.run", {"companies": str(len(request.companies))}):
        # Convert Pydantic models to dicts
        companies_dict = [c.model_dump() for c in request.companies]
        
        # Validate
        is_valid, errors = pipeline.validate_input(companies_dict)
        if not is_valid:
            raise HTTPException(status_code=400, detail={"errors": errors})
        
        try:
            # Run pipeline
            result = pipeline.run(companies_dict)
            
            # Track metrics
            metrics.increment("etl.jobs.total")
            metrics.increment(f"etl.jobs.{result.status.value}")
            metrics.histogram("etl.records.processed", result.metrics.records_input)
            
            logger.info(
                "ETL completed",
                status=result.status.value,
                search_id=result.search_id,
                records=result.metrics.records_input,
                success_rate=result.metrics.success_rate
            )
            
            return ETLResponse(
                status=result.status.value,
                search_id=result.search_id,
                run_hash=result.run_hash,
                metrics=result.metrics.to_dict(),
                timestamp=result.timestamp
            )
            
        except Exception as e:
            metrics.increment("etl.jobs.error")
            logger.exception("ETL failed", e)
            raise HTTPException(status_code=500, detail=str(e))


@app.post("/etl/run/async", tags=["ETL"])
async def run_etl_async(
    request: ETLJobRequest,
    background_tasks: BackgroundTasks
):
    """
    Run ETL pipeline asynchronously.
    
    Returns immediately with a job ID.
    """
    import uuid
    job_id = str(uuid.uuid4())
    
    companies_dict = [c.model_dump() for c in request.companies]
    
    # Add to background
    background_tasks.add_task(
        run_etl_background,
        job_id,
        companies_dict
    )
    
    logger.info("Async ETL job queued", job_id=job_id, companies=len(companies_dict))
    
    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"ETL job queued for {len(request.companies)} companies"
    }


async def run_etl_background(job_id: str, companies: List[Dict]):
    """Background task for async ETL."""
    logger.info("Starting background ETL", job_id=job_id)
    try:
        result = pipeline.run(companies)
        logger.info(
            "Background ETL completed",
            job_id=job_id,
            status=result.status.value
        )
    except Exception as e:
        logger.error("Background ETL failed", job_id=job_id, error=str(e))


@app.get("/searches", tags=["Searches"])
@track_metrics(metrics, "api.searches.list")
async def list_searches(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """
    List recent searches/ETL runs.
    """
    searches = db.get_recent_searches(limit=limit)
    
    return {
        "searches": searches,
        "count": len(searches),
        "limit": limit,
        "offset": offset
    }


@app.get("/searches/{search_id}", tags=["Searches"])
@track_metrics(metrics, "api.searches.get")
async def get_search(search_id: int):
    """
    Get detailed results for a specific search.
    """
    with tracer.trace("search.get", {"search_id": str(search_id)}):
        results = db.get_search_results(search_id)
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"Search {search_id} not found"
            )
        
        return results


@app.get("/companies/search", tags=["Companies"])
@track_metrics(metrics, "api.companies.search")
async def search_companies(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Search for companies in the database.
    """
    results = db.search_companies(q, limit=limit)
    
    return {
        "query": q,
        "results": results,
        "count": len(results)
    }


@app.get("/migrations/status", tags=["Admin"])
async def get_migration_status():
    """
    Get database migration status.
    """
    manager = MigrationManager()
    return manager.get_status()


@app.post("/migrations/upgrade", tags=["Admin"])
async def run_migrations():
    """
    Apply pending database migrations.
    """
    manager = MigrationManager()
    
    pending = manager.get_pending_migrations()
    if not pending:
        return {"message": "No pending migrations", "applied": []}
    
    applied = manager.upgrade()
    
    logger.info("Migrations applied via API", versions=applied)
    
    return {
        "message": f"Applied {len(applied)} migrations",
        "applied": applied
    }


@app.get("/metrics", tags=["Admin"])
async def get_metrics_endpoint():
    """
    Get application metrics.
    
    In production, this would be Prometheus-formatted.
    """
    return metrics.get_stats()


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions."""
    logger.warning(
        "HTTP error",
        status_code=exc.status_code,
        detail=exc.detail,
        path=request.url.path
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.exception("Unhandled error", exc, path=request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if os.getenv("DEBUG") else "An unexpected error occurred"
        }
    )


# ============================================================================
# Run
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
