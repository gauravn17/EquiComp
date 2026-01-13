from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import time
import os
import logging
import pandas as pd
from openai import OpenAI
import json
import numpy as np

# =========================================================
# Logging Configuration
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =========================================================
# Type Definitions
# =========================================================
TargetCompany = Dict[str, str]
ComparableCompany = Dict[str, Any]
# -------------------------
# Example target (treated as private)
# -------------------------
def get_example_target() -> TargetCompany:
    # HURON CONSULTING - Healthcare consulting (CURRENT TEST)
    # return {
    #     "name": "Huron Consulting Group Inc.",
    #     "description": (
    #         "Provides specialized management consulting and advisory services primarily "
    #         "to healthcare providers, including hospitals and health systems. Huron focuses "
    #         "on performance improvement, financial restructuring, revenue cycle optimization, "
    #         "care delivery transformation, regulatory compliance, and technology-enabled "
    #         "operational improvements within the healthcare industry."
    #     ),
    #     "homepage_url": "https://www.huronconsultinggroup.com",
    #     "primary_sic": "Research and Consulting Services"
    # }
    
    # =====================================================
    # ULTRA-NICHE / EMERGING SECTORS (few or no pure-play comps)
    # =====================================================
    
    # QUANTUM COMPUTING - Very few pure-play public comps
    # return {
    #     "name": "IonQ Competitor",
    #     "description": (
    #         "Develops trapped-ion quantum computers and quantum software "
    #         "for enterprise and research customers. Provides cloud-based "
    #         "quantum computing access and quantum algorithm development services."
    #     ),
    #     "homepage_url": "https://www.quantumstartup.com",
    #     "primary_sic": "Computer Equipment"
    # }
    
    # SPACE DEBRIS REMOVAL - Novel industry, almost no public comps
    # return {
    #     "name": "OrbitClear Technologies",
    #     "description": (
    #         "Develops spacecraft and robotic systems for active debris removal "
    #         "and satellite servicing in low-earth orbit. Provides on-orbit "
    #         "inspection, refueling, and decommissioning services to satellite operators."
    #     ),
    #     "homepage_url": "https://www.orbitclear.com",
    #     "primary_sic": "Aerospace Equipment"
    # }
    
    # NUCLEAR FUSION - Pre-revenue deep tech
    # return {
    #     "name": "HelionX Energy",
    #     "description": (
    #         "Develops compact fusion reactor technology using magneto-inertial "
    #         "confinement. Building pilot fusion power plant for grid-scale "
    #         "electricity generation with zero carbon emissions."
    #     ),
    #     "homepage_url": "https://www.helionx.com",
    #     "primary_sic": "Electric Services"
    # }
    
    # =====================================================
    # SPAC-HEAVY / HIGH M&A SECTORS (tests delisting detection)
    # =====================================================
    
    # VERTICAL FARMING - Many SPACs went bust
    # return {
    #     "name": "UrbanLeaf Systems",
    #     "description": (
    #         "Designs and operates indoor vertical farming facilities using "
    #         "hydroponics, LED lighting, and AI-controlled climate systems. "
    #         "Supplies fresh produce to grocery chains and food service companies "
    #         "in urban markets year-round."
    #     ),
    #     "homepage_url": "https://www.urbanleaf.com",
    #     "primary_sic": "Agricultural Production - Crops"
    # }
    
    # CARBON CAPTURE - SPAC-heavy, many acquisitions
    # return {
    #     "name": "CarbonVault Inc.",
    #     "description": (
    #         "Develops direct air capture technology and operates carbon "
    #         "sequestration facilities. Sells carbon removal credits to "
    #         "corporations and provides carbon accounting and verification services."
    #     ),
    #     "homepage_url": "https://www.carbonvault.com",
    #     "primary_sic": "Environmental Services"
    # }
    
    # PRECISION AGRICULTURE - Known delistings (Raven, AgJunction)
    # return {
    #     "name": "CropSense Analytics",
    #     "description": (
    #         "Precision agriculture platform combining satellite imagery, soil sensors, "
    #         "and machine learning to provide crop health monitoring, yield prediction, "
    #         "and irrigation optimization for large-scale farms and agricultural cooperatives."
    #     ),
    #     "homepage_url": "https://www.cropsenseanalytics.com",
    #     "primary_sic": "Agricultural Services"
    # }
    
    # =====================================================
    # AI / SEMICONDUCTORS (hot sector, complex landscape)
    # =====================================================
    
    # AI INFRASTRUCTURE - Tests against Nvidia, AMD, etc.
    # return {
    #     "name": "Groq",
    #     "description": (
    #         "Designs and manufactures specialized AI accelerator hardware focused on "
    #         "low-latency inference for large language models, using a deterministic "
    #         "tensor streaming architecture. Groq provides hardware systems and software "
    #         "tools optimized for real-time AI workloads in data centers and enterprise environments."
    #     ),
    #     "homepage_url": "https://www.groq.com",
    #     "primary_sic": "Computer Peripheral Equipment"
    # }
    
    # EDGE AI CHIPS - Semiconductor with specific focus
    # return {
    #     "name": "NeuralEdge Semiconductor",
    #     "description": (
    #         "Designs ultra-low-power AI inference chips for edge devices. "
    #         "Processors optimized for computer vision and NLP in IoT sensors, "
    #         "wearables, and autonomous vehicles with <1W power consumption."
    #     ),
    #     "homepage_url": "https://www.neuraledge.com",
    #     "primary_sic": "Semiconductors"
    # }
    
    # AI DATA SERVICES
    # return {
    #     "name": "Scale AI",
    #     "description": (
    #         "Provides data labeling, data curation, and AI training data services "
    #         "to support machine learning model development, including computer vision, "
    #         "natural language processing, and autonomous systems. Scale AI primarily "
    #         "serves enterprise and government customers building AI models."
    #     ),
    #     "homepage_url": "https://www.scale.com",
    #     "primary_sic": "Computer Integrated Systems Design"
    # }
    
    # FOUNDATION MODELS
    # return {
    #     "name": "OpenAI",
    #     "description": (
    #         "Develops large-scale artificial intelligence models and platforms, "
    #         "including large language models and generative AI systems, delivered "
    #         "via APIs and enterprise integrations. OpenAI focuses on model research, "
    #         "training, inference infrastructure, and deployment of general-purpose AI systems."
    #     ),
    #     "homepage_url": "https://www.openai.com",
    #     "primary_sic": "Computer Programming Services"
    # }
    
    # =====================================================
    # B2B SOFTWARE / VERTICAL SAAS
    # =====================================================
    
    # LEGAL TECH
    # return {
    #     "name": "LexiContract AI",
    #     "description": (
    #         "AI-powered contract analysis and management platform for corporate "
    #         "legal departments and law firms. Automates contract review, risk "
    #         "identification, obligation tracking, and compliance monitoring."
    #     ),
    #     "homepage_url": "https://www.lexicontract.com",
    #     "primary_sic": "Computer Programming Services"
    # }
    
    # COMPLIANCE SOFTWARE
    # return {
    #     "name": "ComplianceGuard AI",
    #     "description": (
    #         "AI-powered compliance monitoring and reporting software for financial institutions. "
    #         "Automates anti-money laundering (AML) transaction monitoring, Know Your Customer (KYC) "
    #         "verification, and regulatory reporting for banks and fintech companies."
    #     ),
    #     "homepage_url": "https://www.complianceguardai.com",
    #     "primary_sic": "Business Services - Software"
    # }
    
    # LOGISTICS SOFTWARE
    # return {
    #     "name": "FreightOptima",
    #     "description": (
    #         "Transportation management system (TMS) for mid-size logistics companies and "
    #         "third-party logistics providers. Cloud software optimizes route planning, "
    #         "carrier selection, and freight cost management."
    #     ),
    #     "homepage_url": "https://www.freightoptima.com",
    #     "primary_sic": "Business Services - Software"
    # }
    
    # =====================================================
    # FINTECH / FINANCIAL SERVICES
    # =====================================================
    
    # PAYMENTS INFRASTRUCTURE
    # return {
    #     "name": "ClearLedger Technologies",
    #     "description": (
    #         "Provides digital payments infrastructure and transaction processing "
    #         "solutions for banks and financial institutions."
    #     ),
    #     "homepage_url": "https://www.clearledgertech.com",
    #     "primary_sic": "Financial Technology Services"
    # }
    
    # INSURTECH
    # return {
    #     "name": "RiskLens Analytics",
    #     "description": (
    #         "Provides AI-driven underwriting and claims automation software for "
    #         "property and casualty insurers. Platform enables real-time risk "
    #         "scoring, dynamic pricing, and automated claims adjudication."
    #     ),
    #     "homepage_url": "https://www.risklens.com",
    #     "primary_sic": "Insurance Services"
    # }
    
    # =====================================================
    # HEALTHCARE / BIOTECH
    # =====================================================
    
    # DIGITAL PATHOLOGY
    return {
        "name": "PathAI Diagnostics",
        "description": (
            "Develops AI-powered digital pathology software for cancer diagnosis. "
            "Platform analyzes whole-slide images to detect tumors, grade cancers, "
            "and predict treatment response for pathology labs and hospitals."
        ),
        "homepage_url": "https://www.pathai-dx.com",
        "primary_sic": "Medical Laboratories"
    }
    
    # SYNTHETIC BIOLOGY
    # return {
    #     "name": "EnzymeWorks Bio",
    #     "description": (
    #         "Engineers microorganisms and enzymes for industrial manufacturing. "
    #         "Produces bio-based chemicals, sustainable materials, and specialty "
    #         "proteins for food, pharma, and materials customers using fermentation."
    #     ),
    #     "homepage_url": "https://www.enzymeworks.com",
    #     "primary_sic": "Biological Products"
    # }
    
    # PSYCHEDELIC THERAPEUTICS - Emerging/controversial pharma
    # return {
    #     "name": "MindPath Therapeutics",
    #     "description": (
    #         "Develops psilocybin and MDMA-based treatments for depression, PTSD, "
    #         "and addiction. Operates clinical trial programs and licensed treatment "
    #         "centers for psychedelic-assisted therapy."
    #     ),
    #     "homepage_url": "https://www.mindpath.com",
    #     "primary_sic": "Pharmaceutical Preparations"
    # }
    
    # =====================================================
    # HARDWARE / INDUSTRIAL
    # =====================================================
    
    # CONSTRUCTION ROBOTICS
    # return {
    #     "name": "BuildBot Robotics",
    #     "description": (
    #         "Manufactures autonomous robots for construction sites including "
    #         "bricklaying robots, rebar-tying machines, and 3D concrete printers. "
    #         "Sells equipment and provides robotics-as-a-service to general contractors."
    #     ),
    #     "homepage_url": "https://www.buildbot.com",
    #     "primary_sic": "Industrial Machinery"
    # }
    
    # BATTERY STORAGE
    # return {
    #     "name": "GridVault Energy",
    #     "description": (
    #         "Designs and manufactures utility-scale battery energy storage systems for "
    #         "renewable energy integration. Lithium-ion battery installations enable grid "
    #         "stabilization and peak demand management for electric utilities and solar farms."
    #     ),
    #     "homepage_url": "https://www.gridvaultenergy.com",
    #     "primary_sic": "Electrical Equipment"
    # }
    
    # RARE EARTH PROCESSING
    # return {
    #     "name": "RareEarth Refining Corp",
    #     "description": (
    #         "Operates rare earth element separation and refining facilities. "
    #         "Produces purified neodymium, dysprosium, and other critical minerals "
    #         "for EV motors, wind turbines, and electronics manufacturers."
    #     ),
    #     "homepage_url": "https://www.rareearthrefining.com",
    #     "primary_sic": "Nonferrous Metals"
    # }
    
    # =====================================================
    # AEROSPACE / DEFENSE
    # =====================================================
    
    # SATELLITE INFRASTRUCTURE
    # return {
    #     "name": "OrbitalGrid Technologies",
    #     "description": (
    #         "Designs and manufactures satellite power management and "
    #         "communications infrastructure for low-earth orbit constellations "
    #         "supporting defense and commercial missions."
    #     ),
    #     "homepage_url": "https://www.orbitalgridtech.com",
    #     "primary_sic": "Aerospace Manufacturing"
    # }
    
    # DEFENSE AI
    # return {
    #     "name": "ShieldMind Defense",
    #     "description": (
    #         "Provides AI-powered autonomous systems and decision support software "
    #         "for defense and intelligence agencies. Products include drone swarm "
    #         "coordination, threat detection, and mission planning systems."
    #     ),
    #     "homepage_url": "https://www.shieldmind.com",
    #     "primary_sic": "Computer Integrated Systems Design"
    # }
    
    # =====================================================
    # REAL PUBLIC COMPANIES (validation tests)
    # =====================================================
    
    # PALANTIR - Should find itself or close peers
    # return {
    #     "name": "Palantir Technologies",
    #     "description": (
    #         "Develops data integration and analytics software platforms for "
    #         "government and commercial customers. Gotham platform serves defense "
    #         "and intelligence; Foundry platform serves enterprise data operations."
    #     ),
    #     "homepage_url": "https://www.palantir.com",
    #     "primary_sic": "Computer Programming Services"
    # }

# =========================================================
# OpenAI Client
# =========================================================
def get_openai_client() -> OpenAI:
    """Initialize OpenAI client with API key from environment."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY environment variable not set")
    return OpenAI(api_key=api_key)


# =========================================================
# JSON Parsing Utilities
# =========================================================
def safe_parse_json(text: str) -> Any:
    """Parse JSON from LLM response, handling common formatting issues."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    text_clean = text.strip()
    
    # Remove markdown code fences
    if text_clean.startswith("```"):
        lines = text_clean.split("\n")
        text_clean = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    
    # Try array extraction
    start = text_clean.find("[")
    end = text_clean.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text_clean[start:end + 1])
        except json.JSONDecodeError:
            pass
    
    # Try object extraction
    start = text_clean.find("{")
    end = text_clean.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text_clean[start:end + 1])
        except json.JSONDecodeError:
            pass
    
    logger.warning(f"Could not parse JSON from response: {text[:200]}...")
    return [] if "[" in text else {}


# =========================================================
# Dynamic Public Status Validation
# =========================================================
class PublicStatusValidator:
    """
    Validates whether companies are currently publicly traded.
    
    This validator uses ONLY dynamic LLM-based verification with no
    hardcoded company names or industry-specific logic. It is designed
    to work across any industry or market.
    
    Validation checks:
    1. Is the company currently trading on the stated exchange?
    2. Has the company been acquired, merged, or taken private?
    3. Has the company been delisted for any reason?
    4. Has the company undergone material business changes affecting comparability?
    """
    
    def __init__(self, client: OpenAI):
        self.client = client
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def verify_single_company(
        self, 
        comp: ComparableCompany
    ) -> Dict[str, Any]:
        """
        Verify public trading status for a single company.
        
        Returns:
            Dict with verification results including:
            - is_publicly_traded: bool or None if uncertain
            - status: ACTIVE, ACQUIRED, MERGED, DELISTED, PRIVATE, UNCERTAIN
            - reason: Explanation if not active
            - acquirer: Acquirer info if applicable
            - date_changed: Date of status change if known
            - material_changes: Any significant business changes
        """
        ticker = comp.get("ticker", "").upper().strip()
        name = comp.get("name", "")
        exchange = comp.get("exchange", "")
        
        # Check cache first
        cache_key = f"{ticker}:{exchange}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        prompt = f"""
You are a financial data analyst verifying public company trading status.

Verify the current trading status of this company:
- Company Name: {name}
- Ticker Symbol: {ticker}
- Listed Exchange: {exchange}

VERIFICATION TASKS:
1. Is this stock CURRENTLY trading on {exchange} (as of today's date)?
2. Has this company been ACQUIRED by another company? If so, by whom and when?
3. Has this company MERGED with another company? If so, details?
4. Has this company been TAKEN PRIVATE? If so, by whom and when?
5. Has this company been DELISTED for any other reason?
6. Has this company undergone MATERIAL BUSINESS CHANGES that significantly 
   altered its core business (e.g., divested major segments, pivoted industries)?

IMPORTANT: Be thorough. Many companies suggested as comparables have been 
acquired in recent years. Check carefully for M&A activity.

Return ONLY a JSON object:
{{
    "ticker": "{ticker}",
    "name": "{name}",
    "exchange": "{exchange}",
    "is_publicly_traded": true/false/null,
    "status": "ACTIVE" | "ACQUIRED" | "MERGED" | "DELISTED" | "PRIVATE" | "UNCERTAIN",
    "confidence": "HIGH" | "MEDIUM" | "LOW",
    "reason": "Detailed explanation of current status",
    "acquirer": "Name and ticker of acquirer if applicable, null otherwise",
    "date_changed": "YYYY-MM-DD if status changed, null if still active",
    "material_changes": "Description of any major business changes, null if none"
}}
"""

        try:
            resp = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1
            )
            
            result = safe_parse_json(resp.choices[0].message.content)
            
            if isinstance(result, dict):
                # Ensure required fields
                result.setdefault("ticker", ticker)
                result.setdefault("name", name)
                result.setdefault("is_publicly_traded", None)
                result.setdefault("status", "UNCERTAIN")
                result.setdefault("confidence", "LOW")
                
                self._cache[cache_key] = result
                return result
            
        except Exception as e:
            logger.error(f"Error verifying {ticker}: {e}")
        
        # Return uncertain result on failure
        uncertain = {
            "ticker": ticker,
            "name": name,
            "exchange": exchange,
            "is_publicly_traded": None,
            "status": "UNCERTAIN",
            "confidence": "LOW",
            "reason": "Verification failed - manual check required"
        }
        self._cache[cache_key] = uncertain
        return uncertain
    
    def verify_batch(
        self, 
        companies: List[ComparableCompany],
        batch_size: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Verify public trading status for a batch of companies.
        Uses batched LLM calls for efficiency.
        """
        results = []
        
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            company_list = "\n".join([
                f"{j+1}. {c.get('name', 'Unknown')} (Ticker: {c.get('ticker', 'N/A')}, Exchange: {c.get('exchange', 'N/A')})"
                for j, c in enumerate(batch)
            ])
            
            prompt = f"""
You are a financial data analyst verifying public company trading status.

Verify the CURRENT trading status of each company below. For each one, determine:
1. Is it currently trading on the stated exchange?
2. Has it been acquired, merged, or taken private?
3. Has it been delisted?
4. Has it undergone material business changes?

Companies to verify:
{company_list}

IMPORTANT: Be thorough. Many companies have been acquired in recent years.
Check carefully for any M&A activity, going-private transactions, or delistings.

Return ONLY a JSON array with one object per company (in the same order):
[
  {{
    "ticker": "TICK",
    "name": "Company Name",
    "is_publicly_traded": true/false/null,
    "status": "ACTIVE" | "ACQUIRED" | "MERGED" | "DELISTED" | "PRIVATE" | "UNCERTAIN",
    "confidence": "HIGH" | "MEDIUM" | "LOW",
    "reason": "Explanation if not active or uncertain, null if active",
    "acquirer": "Acquirer name and ticker if acquired, null otherwise",
    "date_changed": "YYYY-MM-DD if known, null otherwise",
    "material_changes": "Description of major business changes, null if none"
  }}
]

Return ONLY the JSON array.
"""
            
            try:
                resp = self.client.chat.completions.create(
                    model="gpt-4o",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1
                )
                
                batch_results = safe_parse_json(resp.choices[0].message.content)
                
                if isinstance(batch_results, list) and len(batch_results) == len(batch):
                    # Cache results
                    for comp, result in zip(batch, batch_results):
                        cache_key = f"{comp.get('ticker', '').upper()}:{comp.get('exchange', '')}"
                        self._cache[cache_key] = result
                    results.extend(batch_results)
                else:
                    # Fallback to individual verification
                    logger.warning(f"Batch verification returned unexpected format, falling back to individual")
                    for comp in batch:
                        results.append(self.verify_single_company(comp))
                        
            except Exception as e:
                logger.error(f"Error in batch verification: {e}")
                # Fallback to individual verification
                for comp in batch:
                    results.append(self.verify_single_company(comp))
            
            # Rate limiting between batches
            if i + batch_size < len(companies):
                time.sleep(0.5)
        
        return results
    
    def validate_companies(
        self,
        companies: List[ComparableCompany]
    ) -> Tuple[List[ComparableCompany], List[Dict[str, Any]]]:
        """
        Validate a list of companies and separate valid from rejected.
        
        Returns:
            Tuple of (valid_companies, rejected_with_reasons)
        """
        if not companies:
            return [], []
        
        logger.info(f"  Verifying public status for {len(companies)} companies...")
        
        # Get verification results
        verifications = self.verify_batch(companies)
        
        valid = []
        rejected = []
        
        for comp, verification in zip(companies, verifications):
            ticker = comp.get("ticker", "N/A")
            status = verification.get("status", "UNCERTAIN")
            is_public = verification.get("is_publicly_traded")
            confidence = verification.get("confidence", "LOW")
            
            # Decision logic
            if is_public == True and status == "ACTIVE":
                # Confirmed active - check for material changes
                material_changes = verification.get("material_changes")
                if material_changes:
                    comp["_caveat"] = f"Material change: {material_changes}"
                    logger.info(f"  ⚠ {ticker}: Active with caveat - {material_changes[:50]}...")
                valid.append(comp)
                
            elif is_public == False or status in ["ACQUIRED", "MERGED", "DELISTED", "PRIVATE"]:
                # Confirmed not trading
                rejected.append({
                    "company": comp,
                    "status": status,
                    "reason": verification.get("reason", "No longer publicly traded"),
                    "acquirer": verification.get("acquirer"),
                    "date": verification.get("date_changed"),
                    "confidence": confidence
                })
                logger.info(f"  ✗ Rejected {ticker}: {status} - {verification.get('reason', 'N/A')[:50]}")
                
            elif status == "UNCERTAIN" and confidence == "LOW":
                # Uncertain - reject with flag for manual review
                rejected.append({
                    "company": comp,
                    "status": "UNCERTAIN",
                    "reason": "Could not confirm public trading status - manual verification required",
                    "confidence": "LOW"
                })
                logger.info(f"  ? {ticker}: Uncertain status - flagged for manual review")
                
            else:
                # Uncertain but medium/high confidence on being active - allow with caveat
                comp["_needs_verification"] = True
                comp["_verification_note"] = verification.get("reason", "Status uncertain")
                valid.append(comp)
                logger.info(f"  ~ {ticker}: Likely active but verify manually")
        
        logger.info(f"  Results: {len(valid)} valid, {len(rejected)} rejected")
        return valid, rejected


# =========================================================
# Target Company Analysis
# =========================================================
def analyze_target_company(target: TargetCompany, client: OpenAI) -> Dict[str, Any]:
    """Analyze target to extract specialization, focus areas, business model."""
    prompt = f"""
You are an expert investment analyst. Analyze this company deeply to guide comparable company selection.

COMPANY:
Name: {target['name']}
Description: {target['description']}
Primary SIC: {target.get('primary_sic', 'Not provided')}

ANALYSIS REQUIRED:

1. SPECIALIZATION LEVEL (0.0 to 1.0):
   - 1.0 = Highly specialized (serves one narrow niche)
   - 0.7 = Moderately specialized (serves one industry vertical)
   - 0.4 = Multi-segment (serves 2-3 industries)
   - 0.0 = Highly diversified (serves many industries)

2. CORE FOCUS AREAS (extract 3-7 key terms)

3. BUSINESS MODEL:
   Choose ONE: "consulting", "software_vendor", "managed_services", "hardware", "platform", "hybrid", "other"

4. KEY DIFFERENTIATORS (2-4 specific characteristics)

5. EXCLUSION CRITERIA:
   - Company types to avoid
   - Characteristics that would make a company non-comparable

Return ONLY valid JSON:
{{
  "specialization_level": 0.0-1.0,
  "core_focus_areas": ["term1", "term2", ...],
  "business_model": "...",
  "key_differentiators": ["diff1", "diff2", ...],
  "exclusion_criteria": {{
    "avoid_company_types": ["type1", "type2", ...],
    "avoid_characteristics": ["char1", "char2", ...]
  }},
  "ideal_comparable_profile": "One sentence describing the ideal comparable company"
}}
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        
        analysis = safe_parse_json(resp.choices[0].message.content)
        
        analysis.setdefault("specialization_level", 0.5)
        analysis.setdefault("core_focus_areas", [])
        analysis.setdefault("business_model", "other")
        analysis.setdefault("key_differentiators", [])
        analysis.setdefault("exclusion_criteria", {"avoid_company_types": [], "avoid_characteristics": []})
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing target: {e}")
        return {
            "specialization_level": 0.5,
            "core_focus_areas": [],
            "business_model": "other",
            "key_differentiators": [],
            "exclusion_criteria": {"avoid_company_types": [], "avoid_characteristics": []}
        }


# =========================================================
# Candidate Generation
# =========================================================
def generate_candidate_comparables(
    target: TargetCompany, 
    analysis: Dict[str, Any],
    client: OpenAI,
    max_candidates: int = 25,
    attempt: int = 1,
    use_broader_search: bool = False
) -> List[ComparableCompany]:
    """Generate comparable candidates using insights from target analysis."""
    specialization = analysis["specialization_level"]
    focus_areas = analysis["core_focus_areas"]
    business_model = analysis["business_model"]
    exclusions = analysis["exclusion_criteria"]
    
    if use_broader_search:
        search_strategy = f"""
BROADER SEARCH MODE (Initial search found insufficient matches):

RELAXED REQUIREMENTS:
1. Companies in RELATED industries: {', '.join(focus_areas[:5])}
2. Similar business model: {business_model}
3. Can include companies where target's focus is 20-40% of revenue
4. Include adjacent/upstream/downstream industries
"""
    elif specialization >= 0.7:
        search_strategy = f"""
HIGHLY SPECIALIZED TARGET (level: {specialization}):

STRICT REQUIREMENTS:
1. >50% revenue from: {', '.join(focus_areas[:5])}
2. Business model alignment: {business_model}
3. AVOID diversified conglomerates where focus is <20% of revenue

AVOID: {', '.join(exclusions.get('avoid_company_types', []))}
"""
    else:
        search_strategy = f"""
MODERATE/DIVERSIFIED TARGET (level: {specialization}):

REQUIREMENTS:
1. >30% exposure to: {', '.join(focus_areas[:5])}
2. Similar business model: {business_model}
"""

    prompt = f"""
You are an expert equity research analyst finding publicly-traded comparable companies.

TARGET COMPANY (Private):
Name: {target['name']}
Description: {target['description']}
Business Model: {business_model}

{search_strategy}

CRITICAL REQUIREMENTS:
1. Only suggest companies that are CURRENTLY publicly traded
2. DO NOT include the target company itself ({target['name']}) in your suggestions
3. DO NOT include companies that have been:
   - Acquired by another company (even recently)
   - Taken private by PE firms or other buyers
   - Merged out of existence
   - Delisted from their exchange
4. Verify each company is still independently trading before including it
5. If a company has undergone major business changes (e.g., divested key segments),
   note this in the description

INSTRUCTIONS:
1. Identify {max_candidates} CURRENTLY PUBLICLY TRADED companies
2. Double-check each is still trading independently
3. Prioritize quality matches over quantity
4. Include exchange and ticker for verification

Return ONLY a valid JSON array:
[
  {{
    "name": "Exact legal company name",
    "url": "https://company-website.com",
    "exchange": "NYSE/NASDAQ/TSE/LSE/etc",
    "ticker": "TICK",
    "business_activity": "Detailed description of main products/services",
    "customer_segment": "Specific industries and customer types served",
    "SIC_industry": "Primary SIC classification",
    "revenue_focus_explanation": "How this matches target's focus",
    "trading_status_note": "Confirm currently trading, note any recent changes"
  }}
]
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=4000
        )
        
        candidates = safe_parse_json(resp.choices[0].message.content)
        
        if not isinstance(candidates, list):
            logger.warning(f"Expected list, got {type(candidates)}")
            return []
        
        # Filter out the target company itself (safety net)
        target_name_lower = target['name'].lower()
        candidates = [
            c for c in candidates 
            if target_name_lower not in c.get("name", "").lower()
        ]
        
        return candidates
        
    except Exception as e:
        logger.error(f"Error generating candidates (attempt {attempt}): {e}")
        if attempt < 3:
            time.sleep(2 ** attempt)
            return generate_candidate_comparables(
                target, analysis, client, max_candidates, attempt + 1, use_broader_search
            )
        return []

# =========================================================
# Description Normalization
# =========================================================
def normalize_for_comparability(description: str, analysis: Dict[str, Any], client: OpenAI) -> str:
    """Rewrite description into standardized format for comparison."""
    focus = ", ".join(analysis.get("core_focus_areas", [])[:5])
    business_model = analysis.get("business_model", "other")
    specialization = analysis.get("specialization_level", 0.5)
    
    prompt = f"""
Rewrite this company description into a factual comparable profile.

RULES:
1. Focus ONLY on PRIMARY revenue-generating activities
2. State industry concentration explicitly
3. Remove marketing language
4. If multi-industry, estimate revenue weighting

Context: focus areas={focus}, model={business_model}, specialization={specialization:.2f}

Description: {description}

Return ONE paragraph (3-5 sentences) describing actual revenue activities.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Error normalizing: {e}")
        return description


# =========================================================
# Embeddings and Similarity
# =========================================================
def embed_texts(texts: List[str], client: OpenAI) -> np.ndarray:
    """Generate embeddings for semantic comparison."""
    try:
        resp = client.embeddings.create(model="text-embedding-3-small", input=texts)
        return np.array([d.embedding for d in resp.data])
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        return np.zeros((len(texts), 1536))


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Compute cosine similarity between two vectors."""
    norm_product = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / norm_product) if norm_product > 0 else 0.0


# =========================================================
# Basic Validation Filters
# =========================================================
def is_valid_company_data(comp: ComparableCompany) -> bool:
    """Validate that company data is complete."""
    required = ["name", "url", "exchange", "ticker", "business_activity"]
    for field in required:
        val = comp.get(field, "")
        if not isinstance(val, str) or not val.strip() or val.lower() in {"na", "n/a", "none", "unknown"}:
            return False
    return True


def is_operating_company_basic(comp: ComparableCompany) -> Tuple[bool, Optional[str]]:
    """
    Quick heuristic check for obviously non-operating entities.
    Returns (is_likely_operating, reason_if_not).
    
    Note: This checks for structural entity types (SPACs, holding companies, etc.)
    which are universally non-operating regardless of industry. This is not 
    industry-specific hardcoding - these are legal/structural classifications.
    """
    text = f"{comp.get('business_activity', '')} {comp.get('name', '')}".lower()
    
    # These are structural entity types, not industry-specific
    structural_non_operating = [
        ("holding company", "Holding company structure"),
        ("investment vehicle", "Investment vehicle"),
        ("spac", "Special Purpose Acquisition Company"),
        ("shell company", "Shell company"),
        ("investment trust", "Investment trust structure"),
        ("blank check", "Blank check company"),
        ("special purpose acquisition", "SPAC structure"),
    ]
    
    for signal, reason in structural_non_operating:
        if signal in text:
            return False, reason
    
    return True, None


def verify_operating_status_llm(
    comp: ComparableCompany,
    client: OpenAI
) -> Dict[str, Any]:
    """
    Use LLM to verify if a company is an operating company vs investment vehicle.
    Fully dynamic - no industry-specific logic.
    """
    prompt = f"""
Analyze whether this is an OPERATING company or a non-operating entity.

Company: {comp.get('name', 'Unknown')}
Business Description: {comp.get('business_activity', 'N/A')}

OPERATING COMPANY = Produces goods or provides services as its primary business
NON-OPERATING ENTITY = Primarily exists to hold investments, assets, or as a financial structure

Examples of NON-OPERATING entities (regardless of industry):
- Holding companies that own subsidiaries but don't operate directly
- Investment trusts or funds
- SPACs (Special Purpose Acquisition Companies)
- Shell companies
- Real estate investment trusts (REITs) that only hold property
- Closed-end funds

Return ONLY a JSON object:
{{
    "is_operating": true/false,
    "entity_type": "operating_company" | "holding_company" | "investment_vehicle" | "spac" | "other_non_operating",
    "confidence": "HIGH" | "MEDIUM" | "LOW",
    "explanation": "Brief explanation"
}}
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        return safe_parse_json(resp.choices[0].message.content)
    except Exception as e:
        logger.error(f"Error verifying operating status: {e}")
        return {"is_operating": True, "confidence": "LOW", "explanation": "Verification failed"}


# =========================================================
# Dynamic Business Model Matching
# =========================================================
def assess_business_model_match(
    target_model: str,
    target_description: str,
    comp_description: str,
    client: OpenAI
) -> Dict[str, Any]:
    """
    Dynamically assess whether two companies have similar business models.
    No hardcoded keywords - fully LLM-based assessment.
    
    Returns:
        Dict with match_score (0.0-1.0) and explanation
    """
    prompt = f"""
You are an investment analyst comparing business models.

TARGET COMPANY BUSINESS MODEL: {target_model}
TARGET DESCRIPTION: {target_description[:500]}

COMPARABLE COMPANY DESCRIPTION: {comp_description[:500]}

TASK: Assess how well the comparable company's business model aligns with the target.

Consider:
1. Revenue generation mechanism (subscription, transaction, licensing, services, product sales, etc.)
2. Customer relationship model (B2B, B2C, B2B2C, marketplace, etc.)
3. Value delivery method (software, hardware, services, platform, hybrid, etc.)
4. Operational model (asset-light, capital-intensive, recurring vs one-time, etc.)

Return ONLY a JSON object:
{{
    "match_score": 0.0-1.0,
    "target_model_type": "Brief description of target's business model",
    "comp_model_type": "Brief description of comparable's business model", 
    "explanation": "One sentence explaining the match/mismatch"
}}

Score guide:
- 1.0 = Nearly identical business models
- 0.7-0.9 = Same general category with minor differences
- 0.4-0.6 = Related but meaningfully different models
- 0.1-0.3 = Different business models
- 0.0 = Completely unrelated models
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        
        result = safe_parse_json(resp.choices[0].message.content)
        
        if isinstance(result, dict) and "match_score" in result:
            return result
            
    except Exception as e:
        logger.error(f"Error assessing business model match: {e}")
    
    # Default to moderate match on failure
    return {"match_score": 0.5, "explanation": "Could not assess - defaulting to neutral"}


# =========================================================
# Scoring
# =========================================================
def score_comparable(
    comp: ComparableCompany,
    analysis: Dict[str, Any],
    target_embedding: np.ndarray,
    target_description: str,
    client: OpenAI
) -> Dict[str, Any]:
    """Score how well a comparable matches the target."""
    score = 1.0  # Base for valid public company
    breakdown = {"valid_public_operating": 1.0}
    
    comp_normalized = comp.get("normalized_description", comp.get("business_activity", ""))
    
    # Semantic similarity
    try:
        comp_embedding = embed_texts([comp_normalized], client)[0]
        semantic_sim = cosine_similarity(target_embedding, comp_embedding)
        specialization = analysis.get("specialization_level", 0.5)
        weight = 3.0 + (specialization * 2.0)
        score += semantic_sim * weight
        breakdown["semantic_similarity"] = f"{semantic_sim:.3f} (weighted {weight:.1f}x)"
    except Exception as e:
        logger.error(f"Semantic similarity error: {e}")
        breakdown["semantic_similarity"] = "error"
    
    # Focus area overlap (dynamic - uses focus areas extracted from target analysis)
    focus_areas = analysis.get("core_focus_areas", [])
    if focus_areas:
        text_lower = comp_normalized.lower()
        matches = sum(1 for a in focus_areas if a.lower() in text_lower)
        focus_score = matches / len(focus_areas)
        score += focus_score * 1.5
        breakdown["focus_overlap"] = f"{focus_score:.2f}"
    
    # Business model match (fully dynamic via LLM)
    target_model = analysis.get("business_model", "unknown")
    model_assessment = assess_business_model_match(
        target_model, 
        target_description,
        comp_normalized, 
        client
    )
    
    model_match_score = model_assessment.get("match_score", 0.5)
    score += model_match_score * 0.75  # Scale contribution
    breakdown["business_model"] = f"{model_match_score:.2f} ({model_assessment.get('explanation', 'N/A')[:50]})"
    
    # Caveat penalty
    if comp.get("_caveat"):
        score -= 0.5
        breakdown["caveat"] = comp["_caveat"]
    
    # Needs verification penalty
    if comp.get("_needs_verification"):
        score -= 0.25
        breakdown["needs_verification"] = True
    
    return {"score": round(score, 3), "breakdown": breakdown}


# =========================================================
# Main Validation and Ranking
# =========================================================
def validate_and_rank_comparables(
    candidates: List[ComparableCompany],
    analysis: Dict[str, Any],
    target_embedding: np.ndarray,
    target_description: str,
    client: OpenAI,
    validator: PublicStatusValidator,
    min_required: int = 3,
    max_allowed: int = 10
) -> Tuple[List[ComparableCompany], List[Dict[str, Any]]]:
    """Validate candidates and return top-ranked comparables."""
    all_rejected = []
    
    # Step 1: Data validation
    logger.info("  Step 4a: Basic data validation...")
    data_valid = []
    for comp in candidates:
        if not is_valid_company_data(comp):
            all_rejected.append({"company": comp, "reason": "Incomplete data", "status": "DATA_INVALID"})
        else:
            # Quick heuristic check for obviously non-operating
            is_operating, reason = is_operating_company_basic(comp)
            if not is_operating:
                all_rejected.append({"company": comp, "reason": reason, "status": "NON_OPERATING"})
            else:
                data_valid.append(comp)
    logger.info(f"    {len(data_valid)} passed, {len(candidates) - len(data_valid)} rejected")
    
    # Step 2: PUBLIC STATUS VALIDATION (fully dynamic, no hardcoding)
    logger.info("  Step 4b: Dynamic public status validation...")
    public_valid, public_rejected = validator.validate_companies(data_valid)
    all_rejected.extend(public_rejected)
    logger.info(f"    {len(public_valid)} confirmed public, {len(public_rejected)} rejected")
    
    # Step 3: Normalize descriptions
    logger.info("  Step 4c: Normalizing descriptions...")
    for comp in public_valid:
        if "normalized_description" not in comp:
            text = f"{comp.get('business_activity', '')} {comp.get('customer_segment', '')}"
            comp["normalized_description"] = normalize_for_comparability(text, analysis, client)
    
    # Step 4: Score
    logger.info("  Step 4d: Scoring comparables...")
    for comp in public_valid:
        result = score_comparable(comp, analysis, target_embedding, target_description, client)
        comp["validation_score"] = result["score"]
        comp["score_breakdown"] = result["breakdown"]
    
    scored = sorted(public_valid, key=lambda x: x["validation_score"], reverse=True)
    
    # Apply thresholds
    thresholds = [5.0, 4.0, 3.0] if analysis.get("specialization_level", 0.5) >= 0.7 else [4.0, 3.0, 2.0]
    for t in thresholds:
        filtered = [c for c in scored if c["validation_score"] >= t]
        if len(filtered) >= min_required:
            return filtered[:max_allowed], all_rejected
    
    return scored[:max_allowed], all_rejected


# =========================================================
# Main Pipeline
# =========================================================
def find_comparables(
    target: TargetCompany,
    min_required: int = 3,
    max_attempts: int = 3
) -> Tuple[List[ComparableCompany], Dict[str, Any]]:
    """Main pipeline to find comparable companies."""
    print(f"\n{'='*60}")
    print(f"Finding comparables for: {target['name']}")
    print(f"{'='*60}\n")
    
    client = get_openai_client()
    validator = PublicStatusValidator(client)
    metadata = {
        "target": target["name"], 
        "timestamp": datetime.now().isoformat(), 
        "rejected_companies": [],
        "validation_method": "dynamic_llm"  # Indicates no hardcoding
    }
    
    # Step 1: Analyze
    print("Step 1: Analyzing target company...")
    analysis = analyze_target_company(target, client)
    print(f"  Specialization: {analysis['specialization_level']:.2f}")
    print(f"  Focus areas: {', '.join(analysis.get('core_focus_areas', [])[:5])}")
    print(f"  Business model: {analysis.get('business_model')}\n")
    metadata["analysis"] = analysis
    
    # Step 2: Embed target
    print("Step 2: Normalizing target and creating embedding...")
    target_norm = normalize_for_comparability(target["description"], analysis, client)
    target_embedding = embed_texts([target_norm], client)[0]
    print(f"  Done.\n")
    
    # Step 3: Generate and validate
    best_comps, best_rejected = [], []
    
    for attempt in range(1, max_attempts + 1):
        use_broader = attempt > 1 and len(best_comps) < min_required
        if use_broader:
            print("  → Switching to broader search...")
        
        print(f"Step 3: Generating candidates (attempt {attempt}/{max_attempts})...")
        candidates = generate_candidate_comparables(target, analysis, client, 25, attempt, use_broader)
        
        if not candidates:
            print("  WARNING: No candidates generated")
            time.sleep(2)
            continue
        
        print(f"  Generated {len(candidates)} candidates")
        print("Step 4: Validating and scoring...")
        
        comps, rejected = validate_and_rank_comparables(
            candidates, analysis, target_embedding, target["description"], client, validator, min_required, 10
        )
        
        print(f"  Final: {len(comps)} valid comparables")
        
        if len(comps) >= min_required:
            print(f"\n✓ Found {len(comps)} comparables!\n")
            metadata["rejected_companies"] = rejected
            return comps, metadata
        
        if len(comps) > len(best_comps):
            best_comps, best_rejected = comps, rejected
        
        if attempt < max_attempts:
            print(f"  Only {len(comps)}, retrying...\n")
            time.sleep(2)
    
    metadata["rejected_companies"] = best_rejected
    return best_comps, metadata

# =========================================================
# Output
# =========================================================
def format_results(comps: List[ComparableCompany], metadata: Dict[str, Any]) -> str:
    lines = [f"\n{'='*60}", f"FINAL RESULTS: {len(comps)} Comparable Companies", f"{'='*60}\n"]
    
    for i, c in enumerate(comps, 1):
        lines.append(f"{i}. {c['name']} ({c['ticker']})")
        lines.append(f"   Exchange: {c['exchange']}")
        lines.append(f"   Score: {c['validation_score']:.2f}")
        lines.append(f"   Business: {c.get('business_activity', '')[:100]}...")
        lines.append(f"   Breakdown: {c.get('score_breakdown', {})}")
        if c.get('_caveat'):
            lines.append(f"   ⚠ CAVEAT: {c['_caveat']}")
        if c.get('_needs_verification'):
            lines.append(f"   ⚠ NEEDS MANUAL VERIFICATION")
        lines.append("")
    
    rejected = metadata.get("rejected_companies", [])
    if rejected:
        lines.extend([f"\n{'='*60}", f"REJECTED: {len(rejected)} companies", f"{'='*60}\n"])
        for r in rejected[:15]:
            comp = r.get("company", {})
            lines.append(f"  ✗ {comp.get('name', 'Unknown')} ({comp.get('ticker', 'N/A')})")
            lines.append(f"    Status: {r.get('status', 'UNKNOWN')}")
            lines.append(f"    Reason: {r.get('reason', 'N/A')}")
            if r.get('acquirer'):
                lines.append(f"    Acquirer: {r.get('acquirer')}")
            if r.get('date'):
                lines.append(f"    Date: {r.get('date')}")
            lines.append("")
    
    return "\n".join(lines)


# =========================================================
# Main
# =========================================================
def main():
    target = get_example_target()
    comps, metadata = find_comparables(target, min_required=3, max_attempts=3)
    
    print(format_results(comps, metadata))
    
    if comps:
        export = [{k: v for k, v in c.items() if not k.startswith('_')} for c in comps]
        pd.DataFrame(export).to_csv("comparables.csv", index=False)
        print(f"Saved {len(comps)} comparables to comparables.csv")
        
        rejected = metadata.get("rejected_companies", [])
        if rejected:
            rej_data = [{
                "name": r.get("company", {}).get("name"),
                "ticker": r.get("company", {}).get("ticker"),
                "exchange": r.get("company", {}).get("exchange"),
                "status": r.get("status"),
                "reason": r.get("reason"),
                "acquirer": r.get("acquirer"),
                "date": r.get("date"),
                "confidence": r.get("confidence")
            } for r in rejected]
            pd.DataFrame(rej_data).to_csv("comparables_rejected.csv", index=False)
            print(f"Saved {len(rejected)} rejected to comparables_rejected.csv")
    
    return comps, metadata


if __name__ == "__main__":
    main()
