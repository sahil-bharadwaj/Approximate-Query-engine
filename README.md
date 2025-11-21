# ML-Powered Approximate Query Engine

An intelligent SQL query optimization system using Machine Learning to provide 3-7x performance improvements on analytical queries.

This Python Flask application uses intelligent sampling strategies and probabilistic data structures to execute approximate queries with controlled error bounds. The system learns from query execution history to adaptively improve its optimization decisions.

## Key Features

**Real-Time Learning & Adaptation**
- Historical performance tracking to improve future optimizations
- Adaptive strategy selection based on actual query performance
- Dynamic confidence scoring based on historical success rates
- Actual speedup measurement with real execution time comparisons

**ML-Based Optimization**
- Intelligent choice between sampling, sketching, and exact execution
- Dual execution modes for comparing ML-optimized vs exact results
- Performance gains of 3-7x on typical analytical queries
- Efficient ROWID modulo sampling with minimal overhead

**Error Control & Monitoring**
- Bootstrap-based confidence intervals for uncertainty quantification
- Real-time display of confidence ranges and error bounds
- Configurable error tolerance (typically 1-5%)
- Detailed explanations of optimization decisions

**Deployment & Interface**
- Integrated web UI with responsive design
- Single Python application with no build steps required
- Docker support for containerized deployment
- Real-time performance monitoring and metrics

## Example Usage

Query: `SELECT COUNT(*) FROM purchases` (200K rows dataset)

**ML Optimized Result:**
- Actual Speedup: 5.2x
- ML Time: 28 ms
- Exact Time: 145 ms
- Estimated Error: 2.1%

The system automatically chooses between sampling strategies, probabilistic sketches, or exact execution based on query characteristics and learned performance patterns.

## Quick Start

**Using Quick Start Scripts:**

Windows:
```cmd
start.bat
```

Linux/Mac:
```bash
chmod +x start.sh
./start.sh
```

**Manual Setup:**

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate sample data:
```bash
python seed.py 200000
```

3. Start the server:
```bash
python app.py
```

4. Open your browser to http://localhost:8080

**Docker Deployment:**

```bash
docker-compose up --build
```

Requirements: Python 3.8+ or Docker

**Testing the System:**

1. Open the web interface at http://localhost:8080
2. Enter a query like `SELECT COUNT(*) FROM purchases`
3. Click "Run ML Optimized" to see optimized execution
4. Click "Run Exact" to compare with baseline performance
5. View actual speedup measurements and execution times

## Supported Query Types

**Good Performance (3-7x speedup):**
```sql
SELECT COUNT(*), SUM(amount) FROM purchases;
SELECT country, COUNT(*) FROM purchases GROUP BY country;
SELECT AVG(amount) FROM purchases;
```

**Moderate Performance (2-4x speedup):**
```sql
SELECT COUNT(DISTINCT country) FROM purchases;
SELECT category, SUM(amount) FROM purchases GROUP BY category;
```

**Not Optimized (exact execution):**
```sql
SELECT MIN(amount), MAX(amount) FROM purchases;
SELECT * FROM purchases ORDER BY amount DESC LIMIT 10;
```

## 🔧 API Usage

### ML-Optimized Query with Learning:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) FROM purchases",
    "use_ml_optimization": true,
    "max_rel_error": 0.05
  }'

# Response includes learning information:
# {
#   "status": "ok",
#   "result": [{"COUNT(*)": 200000}],
#   "ml_optimization": {
#     "strategy": "exact",
#     "confidence": 0.6,
#     "estimated_speedup": 1,
#     "estimated_error": 0,
#     "reasoning": "No clear optimization strategy found - using exact computation for safety (No historical data available)",
#     "transformations": []
#   }
# }
```

### Check Learning Stats:
```bash
curl -X GET http://localhost:8080/ml/stats

# Response:
# {
#   "status": "ok", 
#   "learning_stats": {
#     "learning_enabled": true,
#     "total_historical_queries": 0,
#     "strategies": []
#   }
# }
```

### Exact Query:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) FROM purchases", 
    "use_ml_optimization": false
  }'
```

## Project Structure

```
├── app.py              # Main Flask application
├── config.py           # Configuration management
├── storage.py          # Database layer (SQLite)
├── planner.py          # Query planning
├── executor.py         # Query execution
├── ml_optimizer.py     # ML optimization engine
├── sampler.py          # Sampling strategies
├── sketches.py         # Probabilistic data structures
├── seed.py             # Data generation script
├── templates/
│   └── index.html      # Web UI
├── requirements.txt    # Python dependencies
├── Dockerfile          # Docker configuration
├── docker-compose.yml  # Docker Compose setup
└── start.bat/start.sh  # Quick start scripts
```

## Technical Details

**Machine Learning Features:**
- Historical performance database for tracking query execution metrics
- Actual speedup measurement comparing optimized vs exact execution
- Confidence scoring that improves with more historical data
- Adaptive strategy selection based on real performance results

**Optimization Strategies:**
- Decision logic to choose best strategy based on query features
- Confidence levels ranging from 60-90% based on historical accuracy
- Performance estimation validated against actual measurements
- Fallback to exact computation when optimization is uncertain

**Query Transformation Methods:**
- ROWID modulo sampling for efficient, low-overhead sampling
- Support for pre-created materialized sample tables
- HyperLogLog sketches for COUNT(DISTINCT) approximation
- Automatic result scaling for COUNT and SUM aggregations

**Error Control:**
- Bootstrap-based confidence intervals
- Configurable error tolerance (1%, 5%, or 10%)
- Real-time uncertainty quantification
- Transparent display of execution times and speedups

## Performance Benchmarks

Typical performance on 200K row dataset:

| Query Type      | Strategy    | Speedup | Error | ML Time | Exact Time |
|-----------------|-------------|---------|-------|---------|------------|
| COUNT(*)        | Sample (1%) | 4-6x    | 2%    | 25ms    | 140ms      |
| SUM(amount)     | Sample (1%) | 4-6x    | 2-3%  | 28ms    | 155ms      |
| GROUP BY        | Sample (1%) | 3-5x    | 3-5%  | 45ms    | 190ms      |
| COUNT(DISTINCT) | Sketch      | 2-3x    | 3%    | 65ms    | 180ms      |

Performance notes:
- Speedups measured from actual query execution times
- ROWID modulo sampling provides efficient optimization
- Results vary based on hardware and query complexity
- Pre-created sample tables offer best performance

## Technology Stack

**Backend:**
- Python 3.8+
- Flask 3.0 web framework
- SQLite 3 database
- Flask-CORS for cross-origin support

**Frontend:**
- HTML5/CSS3 with responsive design
- Vanilla JavaScript (no frameworks)
- Modern CSS with flexbox layout

**Algorithms:**
- ROWID modulo sampling for efficient uniform sampling
- HyperLogLog for cardinality estimation
- Count-Min Sketch for frequency estimation
- Bootstrap confidence intervals for error estimation
- Historical learning for performance-based optimization

## Use Cases

This ML-powered approximate query engine is suitable for:

- Analytical workloads where approximate results are acceptable
- Exploratory data analysis on large datasets
- Business intelligence dashboards with aggregated metrics
- Real-time analytics with controlled error bounds
- Development and testing environments where speed is prioritized

The system provides 3-7x performance improvements on typical aggregation queries while maintaining error rates under 5%.

## Credits

Original Go version developed by:
- [Sahithi Kokkula](https://github.com/SahithiKokkula)
- [Nikunj Agarwal](https://github.com/nikunjagarwal17)

Python Flask conversion:
- Converted from Go to Python with Flask framework
- Simplified architecture and deployment process
- Added real-time speedup measurements
- Optimized sampling algorithms for better performance

