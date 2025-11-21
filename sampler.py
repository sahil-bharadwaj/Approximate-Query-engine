"""
Sampling strategies for approximate query processing
"""
import sqlite3
import math
from typing import List, Dict, Tuple, Optional


def create_uniform_sample(conn: sqlite3.Connection, table: str, fraction: float) -> Tuple[str, int]:
    """
    Create a uniform random sample of a table
    
    Args:
        conn: Database connection
        table: Source table name
        fraction: Sampling fraction (0 < fraction < 1)
    
    Returns:
        Tuple of (sample_table_name, row_count)
    """
    if fraction <= 0 or fraction >= 1:
        raise ValueError("Invalid fraction")
    
    name = f"{table}__sample_{_fraction_name(fraction)}"
    cursor = conn.cursor()
    
    # Drop existing sample
    cursor.execute(f"DROP TABLE IF EXISTS {name}")
    
    # Create sample table
    query = f"""
        CREATE TABLE {name} AS 
        SELECT * FROM {table} 
        WHERE (abs(random())/9223372036854775807.0) < {fraction}
    """
    cursor.execute(query)
    
    # Count rows
    cursor.execute(f"SELECT COUNT(*) FROM {name}")
    count = cursor.fetchone()[0]
    
    conn.commit()
    
    # Record metadata
    _record_sample_meta(conn, table, name, fraction)
    
    return name, count


def create_stratified_sample(conn: sqlite3.Connection, table: str, strata_col: str, 
                            total_fraction: float, variance_col: Optional[str] = None) -> Tuple[str, List[Dict]]:
    """
    Create a stratified sample of a table
    
    Args:
        conn: Database connection
        table: Source table name
        strata_col: Column to stratify on
        total_fraction: Total sampling fraction
        variance_col: Optional column for variance-based allocation (Neyman allocation)
    
    Returns:
        Tuple of (sample_table_name, strata_info_list)
    """
    if total_fraction <= 0 or total_fraction >= 1:
        raise ValueError(f"Invalid total fraction: {total_fraction}")
    
    # Analyze strata
    strata = _analyze_strata(conn, table, strata_col, variance_col)
    
    # Allocate samples
    if variance_col:
        _allocate_neyman_optimal(strata, total_fraction)
    else:
        _allocate_proportional(strata, total_fraction)
    
    sample_name = f"{table}__strat_sample_{strata_col}_{_fraction_name(total_fraction)}"
    
    cursor = conn.cursor()
    
    # Drop existing sample
    cursor.execute(f"DROP TABLE IF EXISTS {sample_name}")
    
    # Build stratified sample query
    query = _build_stratified_sample_query(table, sample_name, strata_col, strata)
    cursor.execute(query)
    
    # Update actual sample sizes
    _update_actual_sample_sizes(conn, sample_name, strata_col, strata)
    
    # Record metadata
    _record_stratified_sample_meta(conn, table, sample_name, strata_col, total_fraction, strata)
    
    conn.commit()
    
    return sample_name, strata


def _fraction_name(f: float) -> str:
    """Convert fraction to string for table naming"""
    if f <= 0:
        return "0_000"
    
    if f < 0.001:
        s = f"{f:.6f}"
    else:
        s = f"{f:.3f}"
    
    s = s.replace(".", "_")
    s = s.rstrip("0")
    if s.endswith("_"):
        s += "0"
    
    if not s.startswith("0_"):
        s = "0_" + s
    
    return s


def _record_sample_meta(conn: sqlite3.Connection, table: str, sample: str, fraction: float):
    """Record sample metadata"""
    cursor = conn.cursor()
    
    # Get base table count
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    base_count = cursor.fetchone()[0]
    
    # Update table stats
    cursor.execute("""
        INSERT INTO aqe_table_stats(table_name, row_count, updated_at)
        VALUES(?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(table_name) DO UPDATE SET 
            row_count=excluded.row_count, 
            updated_at=CURRENT_TIMESTAMP
    """, (table, base_count))
    
    # Record sample
    cursor.execute("""
        INSERT INTO aqe_samples(table_name, sample_table, sample_fraction, created_at)
        VALUES(?, ?, ?, CURRENT_TIMESTAMP)
    """, (table, sample, fraction))
    
    conn.commit()


def _analyze_strata(conn: sqlite3.Connection, table: str, strata_col: str, 
                   variance_col: Optional[str]) -> List[Dict]:
    """Analyze strata characteristics"""
    cursor = conn.cursor()
    
    if variance_col:
        query = f"""
            SELECT {strata_col} as strata_value, 
                   COUNT(*) as pop_size,
                   AVG({variance_col}) as mean_val,
                   CASE WHEN COUNT(*) > 1 THEN 
                       (SUM(({variance_col} - (SELECT AVG({variance_col}) FROM {table} WHERE {strata_col} = t.{strata_col})) * 
                            ({variance_col} - (SELECT AVG({variance_col}) FROM {table} WHERE {strata_col} = t.{strata_col}))) / (COUNT(*) - 1))
                   ELSE 0 END as variance
            FROM {table} t
            WHERE {strata_col} IS NOT NULL AND {variance_col} IS NOT NULL
            GROUP BY {strata_col}
            ORDER BY pop_size DESC
        """
    else:
        query = f"""
            SELECT {strata_col} as strata_value, 
                   COUNT(*) as pop_size,
                   0.0 as mean_val,
                   0.0 as variance
            FROM {table}
            WHERE {strata_col} IS NOT NULL
            GROUP BY {strata_col}
            ORDER BY pop_size DESC
        """
    
    cursor.execute(query)
    strata = []
    for row in cursor.fetchall():
        strata.append({
            "strata_key": strata_col,
            "strata_value": str(row[0]),
            "pop_size": row[1],
            "mean_val": row[2],
            "variance": row[3],
            "sample_size": 0,
            "fraction": 0.0,
            "weight": 0.0
        })
    
    return strata


def _allocate_neyman_optimal(strata: List[Dict], total_fraction: float):
    """Neyman allocation for optimal variance reduction"""
    total_pop = sum(s["pop_size"] for s in strata)
    total_weight = 0.0
    
    for s in strata:
        std_dev = math.sqrt(s["variance"])
        s["weight"] = s["pop_size"] * std_dev
        total_weight += s["weight"]
    
    total_sample_size = total_pop * total_fraction
    
    for s in strata:
        if total_weight > 0:
            s["sample_size"] = int(total_sample_size * s["weight"] / total_weight)
            s["fraction"] = s["sample_size"] / s["pop_size"]
        else:
            s["fraction"] = total_fraction
            s["sample_size"] = int(s["pop_size"] * total_fraction)
        
        # Don't sample more than population
        if s["fraction"] > 1.0:
            s["fraction"] = 1.0
            s["sample_size"] = s["pop_size"]


def _allocate_proportional(strata: List[Dict], total_fraction: float):
    """Proportional allocation"""
    for s in strata:
        s["fraction"] = total_fraction
        s["sample_size"] = int(s["pop_size"] * total_fraction)
        s["weight"] = float(s["pop_size"])


def _build_stratified_sample_query(table: str, sample_name: str, strata_col: str, 
                                   strata: List[Dict]) -> str:
    """Build SQL query for stratified sampling"""
    union_parts = []
    
    for stratum in strata:
        if stratum["sample_size"] > 0:
            part = f"""
                SELECT * FROM {table} 
                WHERE {strata_col} = '{stratum["strata_value"]}' 
                  AND (abs(random())/9223372036854775807.0) < {stratum["fraction"]}
            """
            union_parts.append(part)
    
    if not union_parts:
        return f"CREATE TABLE {sample_name} AS SELECT * FROM {table} WHERE 1=0"
    
    query = f"CREATE TABLE {sample_name} AS {' UNION ALL '.join(union_parts)}"
    return query


def _update_actual_sample_sizes(conn: sqlite3.Connection, sample_name: str, 
                                strata_col: str, strata: List[Dict]):
    """Update strata with actual achieved sample sizes"""
    cursor = conn.cursor()
    query = f"""
        SELECT {strata_col} as strata_value, COUNT(*) as actual_count
        FROM {sample_name}
        GROUP BY {strata_col}
    """
    cursor.execute(query)
    
    actual_counts = {str(row[0]): row[1] for row in cursor.fetchall()}
    
    for s in strata:
        if s["strata_value"] in actual_counts:
            actual_count = actual_counts[s["strata_value"]]
            s["sample_size"] = actual_count
            s["fraction"] = actual_count / s["pop_size"]


def _record_stratified_sample_meta(conn: sqlite3.Connection, table: str, sample_name: str, 
                                   strata_col: str, total_fraction: float, strata: List[Dict]):
    """Record stratified sample metadata"""
    cursor = conn.cursor()
    
    # Record in main samples table
    cursor.execute("""
        INSERT INTO aqe_samples(table_name, sample_table, sample_fraction, strata_column, created_at)
        VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, (table, sample_name, total_fraction, strata_col))
    
    # Record each stratum
    for s in strata:
        cursor.execute("""
            INSERT INTO aqe_strata_info(sample_table, strata_key, strata_value, pop_size, 
                                       sample_size, fraction, weight, variance)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """, (sample_name, s["strata_key"], s["strata_value"], s["pop_size"],
              s["sample_size"], s["fraction"], s["weight"], s["variance"]))
    
    conn.commit()
