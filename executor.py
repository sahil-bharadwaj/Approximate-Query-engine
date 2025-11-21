"""
Query executor for executing plans
"""
import sqlite3
from typing import List, Dict, Tuple
import math


def execute_query(conn: sqlite3.Connection, plan: Dict) -> Tuple[List[Dict], Dict]:
    """
    Execute a query plan
    
    Args:
        conn: Database connection
        plan: Execution plan from planner
    
    Returns:
        Tuple of (result_rows, metadata)
    """
    cursor = conn.cursor()
    cursor.execute(plan["sql"])
    
    columns = [desc[0] for desc in cursor.description]
    results = []
    sample_data = {}
    
    # Collect results
    if plan["type"] == "sample":
        for col in columns:
            sample_data[col] = []
    
    for row in cursor.fetchall():
        row_dict = {}
        for i, col in enumerate(columns):
            value = row[i]
            row_dict[col] = value
            
            if plan["type"] == "sample":
                if isinstance(value, (int, float)):
                    sample_data[col].append(float(value))
        
        results.append(row_dict)
    
    meta = {
        "plan_type": plan["type"],
        "reason": plan.get("reason", ""),
        "rows": len(results),
        "sql_executed": plan["sql"]
    }
    
    # Handle sampling
    if plan["type"] == "sample" and results:
        meta["sample_fraction"] = plan.get("sample_fraction", 0.01)
        meta["sample_table"] = plan.get("sample_table", "")
        
        # Scale results
        _scale_sample_results(results, plan.get("sample_fraction", 0.01), columns)
        
        # Add confidence intervals
        _enrich_with_bootstrap_cis(results, sample_data, plan.get("sample_fraction", 0.01), columns)
    
    return results, meta


def _scale_sample_results(results: List[Dict], sample_fraction: float, columns: List[str]):
    """Scale sample results to population estimates"""
    if sample_fraction <= 0 or not results:
        return
    
    scale = 1.0 / sample_fraction
    
    for row in results:
        for col in columns:
            if col not in row:
                continue
            
            col_upper = col.upper()
            needs_scaling = any(keyword in col_upper for keyword in 
                              ['COUNT', 'SUM', 'TOTAL', 'REVENUE', 'ORDERS'])
            
            if needs_scaling:
                value = row[col]
                if isinstance(value, (int, float)):
                    row[col] = value * scale


def _enrich_with_bootstrap_cis(results: List[Dict], sample_data: Dict[str, List[float]], 
                               sample_fraction: float, columns: List[str]):
    """Add bootstrap confidence intervals to results"""
    if not results or not sample_data:
        return
    
    scale = 1.0 / sample_fraction
    
    for col in columns:
        values = sample_data.get(col, [])
        if not values or len(values) < 2:
            continue
        
        # Simple confidence interval calculation
        mean_val = sum(values) / len(values)
        std_dev = math.sqrt(sum((x - mean_val) ** 2 for x in values) / len(values))
        se = std_dev / math.sqrt(len(values))
        
        # 95% confidence interval
        ci_low = (mean_val - 1.96 * se) * scale
        ci_high = (mean_val + 1.96 * se) * scale
        rel_error = (ci_high - ci_low) / (2 * mean_val * scale) if mean_val > 0 else 0
        
        # Add to first result row
        if col in results[0]:
            results[0][f"{col}_ci_low"] = ci_low
            results[0][f"{col}_ci_high"] = ci_high
            results[0][f"{col}_rel_error"] = rel_error
