from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

from etl.pipeline import FinancialETLPipeline

app = FastAPI(title="CompIQ API")

pipeline = FinancialETLPipeline()

class Company(BaseModel):
  name: str
  ticker: str
  exchange: str

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/etl/run")
def run_etl(companies: List[Company]):
    """
    Trigger a financial ETL run.
    """
    search_id = pipeline.run([c.dict() for c in companies])
    return {
        "status": "completed",
        "search_id": search_id,
        "records": len(companies),
    }
  
