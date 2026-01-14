"""
Enhanced scoring module with advanced AI-powered analysis.
Provides more nuanced, intelligent comparable matching.
"""
from typing import Dict, Any, List, Tuple
from openai import OpenAI
import numpy as np
import logging

logger = logging.getLogger(__name__)


class AdvancedScoringEngine:
    """
    Enhanced scoring with multi-dimensional analysis.
    """
    
    def __init__(self, client: OpenAI):
        self.client = client
    
    def calculate_advanced_score(
        self,
        comparable: Dict[str, Any],
        target: Dict[str, Any],
        analysis: Dict[str, Any],
        target_embedding: np.ndarray
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive score with multiple dimensions.
        
        Dimensions:
        1. Semantic similarity (AI-based)
        2. Business model alignment (LLM-based)
        3. Customer overlap (LLM-based)
        4. Scale/maturity matching
        5. Financial profile similarity (if available)
        """
        scores = {}
        weights = {}
        
        # Base score
        scores['base'] = 1.0
        weights['base'] = 1.0
        
        # 1. Semantic Similarity (enhanced)
        semantic_result = self._calculate_semantic_similarity(
            comparable, target_embedding
        )
        scores['semantic'] = semantic_result['score']
        weights['semantic'] = 3.5  # Increased weight
        
        # 2. Business Model Alignment (deep analysis)
        business_model_result = self._analyze_business_model_depth(
            comparable, target, analysis
        )
        scores['business_model'] = business_model_result['score']
        weights['business_model'] = 2.0
        
        # 3. Customer/Market Overlap
        customer_result = self._analyze_customer_overlap(
            comparable, target
        )
        scores['customer_overlap'] = customer_result['score']
        weights['customer_overlap'] = 1.5
        
        # 4. Scale Matching (if financial data available)
        if comparable.get('financials', {}).get('market_cap'):
            scale_result = self._analyze_scale_similarity(comparable, target, analysis)
            scores['scale'] = scale_result['score']
            weights['scale'] = 1.0
        
        # 5. Focus Area Precision
        focus_result = self._analyze_focus_precision(
            comparable, analysis.get('core_focus_areas', [])
        )
        scores['focus_precision'] = focus_result['score']
        weights['focus_precision'] = 1.8
        
        # Calculate weighted total
        total_score = sum(scores[k] * weights.get(k, 1.0) for k in scores)
        max_possible = sum(weights.values())
        normalized_score = (total_score / max_possible) * 10  # Scale to 10
        
        # Penalties
        if comparable.get('_caveat'):
            normalized_score -= 0.5
        if comparable.get('_needs_verification'):
            normalized_score -= 0.25
        
        return {
            'score': round(normalized_score, 2),
            'components': scores,
            'weights': weights,
            'breakdown_detailed': {
                'semantic': semantic_result,
                'business_model': business_model_result,
                'customer': customer_result,
                'focus': focus_result
            }
        }
    
    def _calculate_semantic_similarity(
        self,
        comparable: Dict[str, Any],
        target_embedding: np.ndarray
    ) -> Dict[str, Any]:
        """Enhanced semantic similarity with context awareness."""
        comp_desc = comparable.get('normalized_description', 
                                   comparable.get('business_activity', ''))
        
        try:
            # Get embedding
            resp = self.client.embeddings.create(
                model="text-embedding-3-small",
                input=[comp_desc]
            )
            comp_embedding = np.array(resp.data[0].embedding)
            
            # Calculate cosine similarity
            similarity = float(np.dot(target_embedding, comp_embedding) / 
                             (np.linalg.norm(target_embedding) * np.linalg.norm(comp_embedding)))
            
            # Enhanced scoring with non-linear scaling
            # Rewards very high similarity, penalizes low similarity more
            if similarity > 0.85:
                score = 1.0
            elif similarity > 0.75:
                score = 0.85 + (similarity - 0.75) * 1.5
            elif similarity > 0.60:
                score = 0.65 + (similarity - 0.60) * 1.3
            else:
                score = similarity
            
            return {
                'score': min(score, 1.0),
                'raw_similarity': similarity,
                'confidence': 'HIGH' if similarity > 0.75 else 'MEDIUM' if similarity > 0.60 else 'LOW'
            }
            
        except Exception as e:
            logger.error(f"Semantic similarity error: {e}")
            return {'score': 0.5, 'raw_similarity': 0.5, 'confidence': 'ERROR'}
    
    def _analyze_business_model_depth(
        self,
        comparable: Dict[str, Any],
        target: Dict[str, Any],
        analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Deep business model analysis using LLM.
        Goes beyond simple categorization.
        """
        target_model = analysis.get('business_model', 'unknown')
        target_desc = target.get('description', '')[:500]
        comp_desc = comparable.get('business_activity', '')[:500]
        
        prompt = f"""Analyze business model similarity between target and comparable.

TARGET: {target_desc}
Target Business Model: {target_model}

COMPARABLE: {comparable.get('name')}
Description: {comp_desc}

Evaluate similarity across:
1. Revenue model (subscription, transaction, licensing, services, etc.)
2. Customer acquisition approach
3. Value delivery method
4. Operational model (asset-light vs capital-intensive)
5. Competitive dynamics

Return ONLY JSON:
{{
    "overall_score": 0.0-1.0,
    "revenue_model_match": 0.0-1.0,
    "customer_model_match": 0.0-1.0,
    "delivery_match": 0.0-1.0,
    "key_similarity": "one sentence",
    "key_difference": "one sentence"
}}
"""
        
        try:
            resp = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            import json
            result = json.loads(resp.choices[0].message.content)
            
            return {
                'score': result.get('overall_score', 0.5),
                'details': result
            }
            
        except Exception as e:
            logger.error(f"Business model analysis error: {e}")
            return {'score': 0.5, 'details': {}}
    
    def _analyze_customer_overlap(
        self,
        comparable: Dict[str, Any],
        target: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze how much customer base overlaps."""
        comp_customers = comparable.get('customer_segment', '').lower()
        target_desc = target.get('description', '').lower()
        
        # Simple keyword matching for now
        # TODO: Could enhance with LLM analysis
        
        overlap_keywords = []
        customer_types = [
            'enterprise', 'government', 'healthcare', 'financial', 
            'retail', 'manufacturing', 'education', 'startups',
            'smb', 'mid-market', 'consumers', 'b2b', 'b2c'
        ]
        
        for keyword in customer_types:
            if keyword in comp_customers and keyword in target_desc:
                overlap_keywords.append(keyword)
        
        score = min(len(overlap_keywords) * 0.25, 1.0)
        
        return {
            'score': score,
            'matched_segments': overlap_keywords,
            'confidence': 'HIGH' if score > 0.6 else 'MEDIUM'
        }
    
    def _analyze_scale_similarity(
        self,
        comparable: Dict[str, Any],
        target: Dict[str, Any],
        analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze if companies are at similar scale/maturity.
        """
        fin = comparable.get('financials', {})
        market_cap = fin.get('market_cap', 0)
        
        # Estimate target stage from description
        # This is heuristic-based; could be improved with LLM
        target_desc = target.get('description', '').lower()
        
        # Rough categorization
        if market_cap > 50_000_000_000:  # >$50B
            comp_stage = 'mega_cap'
        elif market_cap > 10_000_000_000:  # >$10B
            comp_stage = 'large_cap'
        elif market_cap > 2_000_000_000:  # >$2B
            comp_stage = 'mid_cap'
        elif market_cap > 300_000_000:  # >$300M
            comp_stage = 'small_cap'
        else:
            comp_stage = 'micro_cap'
        
        # For now, don't penalize too much on scale
        # In future, could use LLM to estimate target's likely stage
        return {
            'score': 0.7,  # Neutral score
            'comparable_stage': comp_stage,
            'note': 'Scale matching is approximate'
        }
    
    def _analyze_focus_precision(
        self,
        comparable: Dict[str, Any],
        focus_areas: List[str]
    ) -> Dict[str, Any]:
        """
        Measure how precisely the comparable matches focus areas.
        """
        comp_desc = comparable.get('normalized_description', '').lower()
        
        matches = []
        partial_matches = []
        
        for area in focus_areas:
            area_lower = area.lower()
            
            # Exact match
            if area_lower in comp_desc:
                matches.append(area)
            # Partial match (check for word stems)
            elif any(word in comp_desc for word in area_lower.split()):
                partial_matches.append(area)
        
        # Calculate score
        exact_score = len(matches) / len(focus_areas) if focus_areas else 0
        partial_score = len(partial_matches) / len(focus_areas) if focus_areas else 0
        
        total_score = exact_score + (partial_score * 0.5)
        
        return {
            'score': min(total_score, 1.0),
            'exact_matches': matches,
            'partial_matches': partial_matches,
            'precision': f"{exact_score:.0%}"
        }


def rescore_comparables_advanced(
    comparables: List[Dict[str, Any]],
    target: Dict[str, Any],
    analysis: Dict[str, Any],
    target_embedding: np.ndarray,
    client: OpenAI
) -> List[Dict[str, Any]]:
    """
    Re-score comparables with advanced engine.
    """
    engine = AdvancedScoringEngine(client)
    
    for comp in comparables:
        result = engine.calculate_advanced_score(
            comp, target, analysis, target_embedding
        )
        
        comp['advanced_score'] = result['score']
        comp['advanced_breakdown'] = result['breakdown_detailed']
        comp['score_components'] = result['components']
    
    # Re-sort by advanced score
    comparables.sort(key=lambda x: x.get('advanced_score', 0), reverse=True)
    
    return comparables
