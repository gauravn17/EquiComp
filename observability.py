"""
CompIQ Logging & Observability
Structured logging, metrics collection, and monitoring utilities.

Demonstrates:
- Structured logging with context
- Performance metrics
- Request tracing
- Error tracking
- Production logging patterns
"""
import logging
import time
import functools
import uuid
import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
from enum import Enum


# ============================================================================
# Log Levels & Configuration
# ============================================================================

class LogLevel(str, Enum):
    """Log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# ============================================================================
# Structured Logger
# ============================================================================

class StructuredLogger:
    """
    Structured JSON logger for production environments.
    
    Outputs logs in JSON format for easy parsing by log aggregators
    like ELK, Splunk, Datadog, etc.
    """
    
    def __init__(
        self,
        name: str,
        level: LogLevel = LogLevel.INFO,
        json_output: bool = True
    ):
        self.name = name
        self.json_output = json_output
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.value))
        
        # Clear existing handlers
        self.logger.handlers = []
        
        # Create handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, level.value))
        
        if json_output:
            handler.setFormatter(JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
            ))
        
        self.logger.addHandler(handler)
        
        # Context storage
        self._context: Dict[str, Any] = {}
    
    def set_context(self, **kwargs):
        """Set persistent context fields."""
        self._context.update(kwargs)
    
    def clear_context(self):
        """Clear context."""
        self._context = {}
    
    def _log(self, level: str, message: str, **kwargs):
        """Internal log method."""
        extra = {
            "timestamp": datetime.utcnow().isoformat(),
            "logger": self.name,
            **self._context,
            **kwargs
        }
        
        getattr(self.logger, level.lower())(
            message,
            extra={"structured": extra}
        )
    
    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self._log("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log("CRITICAL", message, **kwargs)
    
    def exception(self, message: str, exc: Exception, **kwargs):
        """Log exception with traceback."""
        import traceback
        self._log(
            "ERROR",
            message,
            exception_type=type(exc).__name__,
            exception_message=str(exc),
            traceback=traceback.format_exc(),
            **kwargs
        )


class JsonFormatter(logging.Formatter):
    """JSON log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add structured data if present
        if hasattr(record, 'structured'):
            log_data.update(record.structured)
        
        # Add exception info
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data)


# ============================================================================
# Metrics Collector
# ============================================================================

@dataclass
class MetricPoint:
    """Single metric data point."""
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """
    Simple metrics collector for tracking application metrics.
    
    In production, this would send to Prometheus, StatsD, Datadog, etc.
    """
    
    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, list] = {}
        self._timers: Dict[str, list] = {}
    
    def increment(self, name: str, value: float = 1, tags: Optional[Dict] = None):
        """Increment a counter."""
        key = self._make_key(name, tags)
        self._counters[key] = self._counters.get(key, 0) + value
    
    def gauge(self, name: str, value: float, tags: Optional[Dict] = None):
        """Set a gauge value."""
        key = self._make_key(name, tags)
        self._gauges[key] = value
    
    def histogram(self, name: str, value: float, tags: Optional[Dict] = None):
        """Record a histogram value."""
        key = self._make_key(name, tags)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)
    
    def timer(self, name: str, duration_ms: float, tags: Optional[Dict] = None):
        """Record a timer value."""
        key = self._make_key(name, tags)
        if key not in self._timers:
            self._timers[key] = []
        self._timers[key].append(duration_ms)
    
    @contextmanager
    def time(self, name: str, tags: Optional[Dict] = None):
        """Context manager for timing operations."""
        start = time.perf_counter()
        try:
            yield
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            self.timer(name, duration_ms, tags)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        stats = {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {},
            "timers": {}
        }
        
        # Calculate histogram stats
        for key, values in self._histograms.items():
            if values:
                stats["histograms"][key] = {
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values)
                }
        
        # Calculate timer stats
        for key, values in self._timers.items():
            if values:
                sorted_values = sorted(values)
                stats["timers"][key] = {
                    "count": len(values),
                    "min_ms": min(values),
                    "max_ms": max(values),
                    "avg_ms": sum(values) / len(values),
                    "p50_ms": sorted_values[len(values) // 2],
                    "p95_ms": sorted_values[int(len(values) * 0.95)] if len(values) >= 20 else None,
                    "p99_ms": sorted_values[int(len(values) * 0.99)] if len(values) >= 100 else None
                }
        
        return stats
    
    def reset(self):
        """Reset all metrics."""
        self._counters = {}
        self._gauges = {}
        self._histograms = {}
        self._timers = {}
    
    @staticmethod
    def _make_key(name: str, tags: Optional[Dict]) -> str:
        """Create metric key from name and tags."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"


# ============================================================================
# Request Tracing
# ============================================================================

@dataclass
class TraceContext:
    """Request trace context."""
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    span_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    parent_span_id: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.utcnow)
    
    def child_span(self) -> 'TraceContext':
        """Create a child span."""
        return TraceContext(
            trace_id=self.trace_id,
            parent_span_id=self.span_id
        )


class RequestTracer:
    """
    Distributed tracing for request tracking.
    
    In production, integrates with Jaeger, Zipkin, AWS X-Ray, etc.
    """
    
    def __init__(self, logger: StructuredLogger, metrics: MetricsCollector):
        self.logger = logger
        self.metrics = metrics
        self._active_traces: Dict[str, TraceContext] = {}
    
    @contextmanager
    def trace(self, operation: str, tags: Optional[Dict] = None):
        """
        Context manager for tracing an operation.
        
        Usage:
            with tracer.trace("process_company", {"ticker": "AAPL"}):
                # ... operation code
        """
        ctx = TraceContext()
        self._active_traces[ctx.trace_id] = ctx
        
        self.logger.info(
            f"Span started: {operation}",
            trace_id=ctx.trace_id,
            span_id=ctx.span_id,
            operation=operation,
            **(tags or {})
        )
        
        start = time.perf_counter()
        error = None
        
        try:
            yield ctx
        except Exception as e:
            error = e
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            
            self.metrics.timer(f"trace.{operation}", duration_ms, tags)
            
            if error:
                self.metrics.increment(f"trace.{operation}.error", tags=tags)
                self.logger.error(
                    f"Span failed: {operation}",
                    trace_id=ctx.trace_id,
                    span_id=ctx.span_id,
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    error=str(error),
                    **(tags or {})
                )
            else:
                self.metrics.increment(f"trace.{operation}.success", tags=tags)
                self.logger.info(
                    f"Span completed: {operation}",
                    trace_id=ctx.trace_id,
                    span_id=ctx.span_id,
                    operation=operation,
                    duration_ms=round(duration_ms, 2),
                    **(tags or {})
                )
            
            self._active_traces.pop(ctx.trace_id, None)


# ============================================================================
# Decorators
# ============================================================================

def log_execution(
    logger: Optional[StructuredLogger] = None,
    level: str = "INFO",
    include_args: bool = False,
    include_result: bool = False
):
    """
    Decorator to log function execution.
    
    Usage:
        @log_execution(logger, include_args=True)
        def my_function(x, y):
            return x + y
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or StructuredLogger(func.__module__)
            
            log_data = {
                "function": func.__name__,
                "module": func.__module__
            }
            
            if include_args:
                log_data["args"] = str(args)[:200]
                log_data["kwargs"] = str(kwargs)[:200]
            
            _logger.info(f"Executing {func.__name__}", **log_data)
            
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start) * 1000
                
                result_data = {"duration_ms": round(duration_ms, 2)}
                if include_result:
                    result_data["result"] = str(result)[:200]
                
                _logger.info(f"Completed {func.__name__}", **log_data, **result_data)
                return result
                
            except Exception as e:
                duration_ms = (time.perf_counter() - start) * 1000
                _logger.exception(
                    f"Failed {func.__name__}",
                    e,
                    duration_ms=round(duration_ms, 2),
                    **log_data
                )
                raise
        
        return wrapper
    return decorator


def track_metrics(
    metrics: MetricsCollector,
    operation: str,
    tags: Optional[Dict] = None
):
    """
    Decorator to track function metrics.
    
    Usage:
        @track_metrics(metrics, "api.search")
        async def search_companies(...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            metrics.increment(f"{operation}.calls", tags=tags)
            
            with metrics.time(operation, tags):
                try:
                    result = func(*args, **kwargs)
                    metrics.increment(f"{operation}.success", tags=tags)
                    return result
                except Exception:
                    metrics.increment(f"{operation}.error", tags=tags)
                    raise
        
        return wrapper
    return decorator


# ============================================================================
# Global Instances
# ============================================================================

# Default logger
default_logger = StructuredLogger("compiq", LogLevel.INFO, json_output=False)

# Global metrics collector
metrics = MetricsCollector()

# Global tracer
tracer = RequestTracer(default_logger, metrics)


# ============================================================================
# Convenience Functions
# ============================================================================

def get_logger(name: str, json_output: bool = False) -> StructuredLogger:
    """Get a named logger."""
    return StructuredLogger(name, json_output=json_output)


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    return metrics


def get_tracer() -> RequestTracer:
    """Get the global tracer."""
    return tracer


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Example: Structured logging
    logger = get_logger("example", json_output=True)
    logger.set_context(service="compiq", environment="development")
    
    logger.info("Application started", version="2.0.0")
    logger.info("Processing request", user_id="123", action="search")
    
    # Example: Metrics
    metrics = get_metrics()
    metrics.increment("api.requests", tags={"endpoint": "/search"})
    metrics.gauge("active_connections", 42)
    
    with metrics.time("database.query"):
        time.sleep(0.1)  # Simulate query
    
    # Example: Tracing
    tracer = get_tracer()
    with tracer.trace("process_batch", {"batch_size": "10"}):
        time.sleep(0.05)
    
    # Example: Decorated function
    @log_execution(logger, include_args=True, include_result=True)
    @track_metrics(metrics, "example.calculate")
    def calculate_sum(a: int, b: int) -> int:
        return a + b
    
    result = calculate_sum(5, 3)
    
    # Print metrics summary
    print("\n" + "=" * 50)
    print("Metrics Summary:")
    print("=" * 50)
    print(json.dumps(metrics.get_stats(), indent=2))
