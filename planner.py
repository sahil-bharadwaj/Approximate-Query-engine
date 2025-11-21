"""
Query planner for choosing execution strategies
"""
import sqlite3
import re
import math
from typing import Dict, List, Optional, Tuple


class Planner:
    """Query planner for approximate query execution"""
    
    def __init__(self):
        self.cost_model = {
            "scan_cost_per_row": 1.0,
            "hash_cost_per_group": 2.0,
            "sketch_query_cost": 10.0,
            "sample_setup_cost": 5.0
        }
    
    def plan(self, conn: sqlite3.Connection, sql: str, max_rel_error: float, prefer_exact: bool) -> Dict:
        """
        Create an execution plan for the query
        
        Returns:
            Plan dictionary with type, sql, metadata
        """
        features = self._parse_query_features(sql)
        table = self._extract_table_name(sql)
        
        if not table:
            return {
                "type": "exact",
                "sql": sql,
                "original_sql": sql,
                "reason": "no table found"
            }
        
        # Check if already querying a sample
        original_table, fraction, is_sample = self._parse_sample_table_name(table)
        if is_sample:
            return {
                "type": "sample",
                "sql": sql,
                "original_sql": sql,
                "table": original_table,
                "sample_table": table,
                "sample_fraction": fraction,
                "reason": f"direct query on sample table (fraction: {fraction:.4f})"
            }
        
        if prefer_exact:
            return {
                "type": "exact",
                "sql": sql,
                "original_sql": sql,
                "table": table,
                "reason": "user prefers exact"
            }
        
        # Get table stats
        table_stats = self._get_table_stats(conn, table)
        if not table_stats:
            return {
                "type": "exact",
                "sql": sql,
                "original_sql": sql,
                "table": table,
                "reason": "no table stats available"
            }
        
        # Evaluate strategies
        strategies = self._evaluate_strategies(conn, sql, table, features, table_stats, max_rel_error)
        
        # Choose best strategy
        best_strategy = self._choose_best_strategy(strategies, max_rel_error)
        
        return best_strategy
    
    def _parse_query_features(self, sql: str) -> Dict:
        """Parse SQL to extract query features"""
        sql_upper = sql.upper()
        
        features = {
            "has_distinct": bool(re.search(r'\bDISTINCT\b', sql_upper)),
            "has_group_by": bool(re.search(r'\bGROUP\s+BY\b', sql_upper)),
            "aggregate_types": [],
            "group_by_columns": [],
            "where_columns": [],
            "is_heavy_hitter": False
        }
        
        # Find aggregates
        agg_pattern = re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', re.IGNORECASE)
        for match in agg_pattern.finditer(sql):
            features["aggregate_types"].append(match.group(1).upper())
        
        # Find GROUP BY columns
        group_by_match = re.search(r'GROUP\s+BY\s+([^HAVING^ORDER^LIMIT]+)', sql, re.IGNORECASE)
        if group_by_match:
            cols = [c.strip() for c in group_by_match.group(1).split(',')]
            features["group_by_columns"] = cols
            features["is_heavy_hitter"] = len(cols) <= 2
        
        return features
    
    def _extract_table_name(self, sql: str) -> Optional[str]:
        """Extract table name from SQL"""
        match = re.search(r'FROM\s+([a-zA-Z0-9_]+)', sql, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    
    def _parse_sample_table_name(self, table_name: str) -> Tuple[str, float, bool]:
        """Check if table name is a sample table"""
        if '__sample_' in table_name:
            idx = table_name.index('__sample_')
            original = table_name[:idx]
            fraction_part = table_name[idx+9:]
            fraction_str = fraction_part.replace('_', '.')
            try:
                fraction = float(fraction_str)
                return original, fraction, True
            except:
                pass
        
        if '__strat_sample_' in table_name:
            idx = table_name.index('__strat_sample_')
            original = table_name[:idx]
            # Extract fraction from end
            parts = table_name[idx+15:].split('_')
            if len(parts) >= 2:
                fraction_str = '_'.join(parts[-2:]).replace('_', '.')
                try:
                    fraction = float(fraction_str)
                    return original, fraction, True
                except:
                    pass
        
        return table_name, 0, False
    
    def _get_table_stats(self, conn: sqlite3.Connection, table: str) -> Optional[Dict]:
        """Get table statistics"""
        cursor = conn.cursor()
        stats = {
            "row_count": 0,
            "distinct_value_counts": {},
            "has_sketches": {},
            "best_sample_fraction": 0.0
        }
        
        # Get row count
        try:
            cursor.execute("SELECT row_count FROM aqe_table_stats WHERE table_name = ?", (table,))
            row = cursor.fetchone()
            if row:
                stats["row_count"] = row[0]
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats["row_count"] = cursor.fetchone()[0]
        except:
            return None
        
        # Check for sketches
        try:
            cursor.execute("SELECT column_name, sketch_type FROM aqe_sketches WHERE table_name = ?", (table,))
            for row in cursor.fetchall():
                stats["has_sketches"][row[0]] = True
        except:
            pass
        
        # Find best sample
        try:
            cursor.execute("""
                SELECT sample_fraction FROM aqe_samples 
                WHERE table_name = ? 
                ORDER BY sample_fraction ASC LIMIT 1
            """, (table,))
            row = cursor.fetchone()
            if row:
                stats["best_sample_fraction"] = row[0]
        except:
            pass
        
        return stats
    
    def _evaluate_strategies(self, conn: sqlite3.Connection, sql: str, table: str, 
                           features: Dict, stats: Dict, max_rel_error: float) -> List[Dict]:
        """Evaluate different execution strategies"""
        strategies = []
        
        # Strategy 1: Exact execution
        exact_plan = {
            "type": "exact",
            "sql": sql,
            "original_sql": sql,
            "table": table,
            "estimated_cost": self._estimate_exact_cost(features, stats),
            "estimated_error": 0.0,
            "reason": "exact execution"
        }
        strategies.append(exact_plan)
        
        # Strategy 2: Sample-based
        if stats["best_sample_fraction"] > 0:
            sample_plan = self._evaluate_sample_strategy(conn, sql, table, features, stats)
            if sample_plan:
                strategies.append(sample_plan)
        
        return strategies
    
    def _estimate_exact_cost(self, features: Dict, stats: Dict) -> float:
        """Estimate cost of exact execution"""
        cost = stats["row_count"] * self.cost_model["scan_cost_per_row"]
        
        if features["has_group_by"]:
            estimated_groups = min(stats["row_count"], 10000)
            cost += estimated_groups * self.cost_model["hash_cost_per_group"]
        
        return cost
    
    def _evaluate_sample_strategy(self, conn: sqlite3.Connection, sql: str, table: str, 
                                  features: Dict, stats: Dict) -> Optional[Dict]:
        """Evaluate sample-based execution"""
        fraction = stats["best_sample_fraction"]
        sample_table = f"{table}__sample_{self._fraction_name(fraction)}"
        
        # Check if sample exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (sample_table,))
        
        if cursor.fetchone()[0] == 0:
            return None
        
        # Estimate error
        estimated_error = math.sqrt(1.0 / (fraction * stats["row_count"]))
        
        # Rewrite SQL
        rewritten_sql = sql.replace(table, sample_table)
        
        sample_cost = (stats["row_count"] * fraction * self.cost_model["scan_cost_per_row"] + 
                      self.cost_model["sample_setup_cost"])
        
        return {
            "type": "sample",
            "sql": rewritten_sql,
            "original_sql": sql,
            "table": table,
            "sample_table": sample_table,
            "sample_fraction": fraction,
            "estimated_cost": sample_cost,
            "estimated_error": estimated_error,
            "reason": f"using {fraction*100:.1f}% sample"
        }
    
    def _choose_best_strategy(self, strategies: List[Dict], max_rel_error: float) -> Dict:
        """Choose the best execution strategy"""
        if not strategies:
            return {"type": "exact", "reason": "no strategies available"}
        
        # Filter by error requirement
        valid = [s for s in strategies if s.get("estimated_error", 0) <= max_rel_error]
        
        if not valid:
            return strategies[0]  # Return exact
        
        # Choose lowest cost
        best = min(valid, key=lambda s: s.get("estimated_cost", float('inf')))
        return best
    
    def _fraction_name(self, f: float) -> str:
        """Convert fraction to string"""
        if f <= 0:
            return "0_000"
        s = f"{f:.3f}".replace(".", "_").rstrip("0")
        if s.endswith("_"):
            s += "0"
        return s
