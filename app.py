"""
ML-Powered Approximate Query Engine - Flask Application
Main entry point for the AQE server
"""
import os
import sqlite3
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from config import config
from storage import Storage
import traceback

# Initialize Flask app
app = Flask(__name__, 
           template_folder='templates',
           static_folder='static')

# Load configuration
env = os.environ.get('FLASK_ENV', 'development')
app.config.from_object(config[env])

# Enable CORS
CORS(app, origins=app.config['CORS_ORIGINS'])

# Initialize storage
storage = Storage(app.config['DB_PATH'])

# Ensure database is set up
with app.app_context():
    storage.ensure_meta_tables()


@app.route('/')
def index():
    """Render the main UI"""
    return render_template('index.html')


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})


@app.route('/tables', methods=['GET'])
def list_tables():
    """List all tables in the database"""
    try:
        conn = storage.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%' 
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        return jsonify({"tables": tables})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/query', methods=['POST'])
def post_query():
    """
    Execute a query with optional ML optimization
    
    Request body:
    {
        "sql": "SELECT COUNT(*) FROM purchases",
        "max_rel_error": 0.05,
        "prefer_exact": false,
        "use_ml_optimization": true,
        "explain": false
    }
    """
    try:
        data = request.get_json()
        sql = data.get('sql', '').strip()
        max_rel_error = data.get('max_rel_error', 0.05)
        prefer_exact = data.get('prefer_exact', False)
        use_ml_optimization = data.get('use_ml_optimization', False)
        explain = data.get('explain', False)
        
        if not sql:
            return jsonify({"error": "sql required"}), 400
        
        from ml_optimizer import MLOptimizer
        from planner import Planner
        from executor import execute_query
        
        conn = storage.get_connection()
        ml_optimization = None
        final_sql = sql
        
        # ML optimization
        if use_ml_optimization and not prefer_exact:
            optimizer = MLOptimizer(conn)
            ml_optimization = optimizer.optimize_query(sql, max_rel_error)
            if ml_optimization:
                final_sql = ml_optimization.get('modified_sql', sql)
        
        # Planning
        planner = Planner()
        plan = planner.plan(conn, final_sql, max_rel_error, prefer_exact)
        
        if explain:
            return jsonify({
                "status": "ok",
                "plan": plan,
                "ml_optimization": ml_optimization
            })
        
        # Execution with timing
        import time
        execution_start = time.time()
        results, meta = execute_query(conn, plan)
        execution_time = time.time() - execution_start
        
        # Calculate actual speedup by running exact query for comparison
        actual_speedup = 1.0
        if use_ml_optimization and not prefer_exact and ml_optimization:
            try:
                # Run exact query to get baseline
                exact_start = time.time()
                cursor = conn.cursor()
                cursor.execute(sql)
                cursor.fetchall()  # Force execution
                exact_time = time.time() - exact_start
                
                # Calculate actual speedup
                if execution_time > 0:
                    actual_speedup = exact_time / execution_time
                    ml_optimization['actual_speedup'] = actual_speedup
                    ml_optimization['execution_time_ms'] = execution_time * 1000
                    ml_optimization['exact_time_ms'] = exact_time * 1000
            except:
                # If exact query fails, use estimated speedup
                actual_speedup = ml_optimization.get('estimated_speedup', 1.0)
        
        # Apply ML result scaling if needed
        if ml_optimization and ml_optimization.get('strategy') == 'sample':
            from ml_optimizer import scale_ml_optimized_results
            scale_ml_optimized_results(results, ml_optimization)
        
        # Add timing to meta
        meta['execution_time_ms'] = execution_time * 1000
        if use_ml_optimization and not prefer_exact:
            meta['actual_speedup'] = actual_speedup
        
        return jsonify({
            "status": "ok",
            "plan": plan,
            "result": results,
            "meta": meta,
            "ml_optimization": ml_optimization
        })
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "status": "error",
            "error": str(e)
        }), 500


@app.route('/samples/create', methods=['POST'])
def create_sample():
    """Create a uniform sample"""
    try:
        data = request.get_json()
        table = data.get('table', '')
        sample_fraction = data.get('sample_fraction', 0)
        
        if not table or sample_fraction <= 0 or sample_fraction >= 1:
            return jsonify({"error": "table and 0<sample_fraction<1 required"}), 400
        
        from sampler import create_uniform_sample
        
        conn = storage.get_connection()
        name, count = create_uniform_sample(conn, table, sample_fraction)
        
        return jsonify({
            "status": "ok",
            "sample_table": name,
            "rows": count
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/samples/stratified', methods=['POST'])
def create_stratified_sample():
    """Create a stratified sample"""
    try:
        data = request.get_json()
        table = data.get('table', '')
        strata_column = data.get('strata_column', '')
        total_fraction = data.get('total_fraction', 0)
        variance_column = data.get('variance_column')
        
        if not table or not strata_column or total_fraction <= 0 or total_fraction >= 1:
            return jsonify({"error": "table, strata_column and 0<total_fraction<1 required"}), 400
        
        from sampler import create_stratified_sample
        
        conn = storage.get_connection()
        sample_name, strata = create_stratified_sample(conn, table, strata_column, 
                                                       total_fraction, variance_column)
        
        return jsonify({
            "status": "ok",
            "sample_table": sample_name,
            "strata": strata,
            "allocation_type": "neyman" if variance_column else "proportional"
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/sketches/create', methods=['POST'])
def create_sketch():
    """Create a probabilistic sketch"""
    try:
        data = request.get_json()
        table = data.get('table', '')
        column = data.get('column')
        sketch_type = data.get('sketch_type', '')
        parameters = data.get('parameters', {})
        
        if not table or not sketch_type:
            return jsonify({"error": "table and sketch_type required"}), 400
        
        from sketches import HyperLogLog, CountMinSketch
        import json
        
        conn = storage.get_connection()
        cursor = conn.cursor()
        
        sketch_data = None
        
        if sketch_type == 'hyperloglog':
            if not column:
                return jsonify({"error": "column required for HyperLogLog"}), 400
            
            hll = HyperLogLog(b=12)
            query = f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
            cursor.execute(query)
            
            count = 0
            for row in cursor.fetchall():
                hll.add_string(str(row[0]))
                count += 1
                if count > 1000000:
                    break
            
            sketch_data = hll.serialize()
        
        elif sketch_type == 'countmin':
            epsilon = parameters.get('epsilon', 0.01)
            delta = parameters.get('delta', 0.01)
            cms = CountMinSketch(epsilon, delta)
            
            if column:
                query = f"SELECT {column}, COUNT(*) FROM {table} WHERE {column} IS NOT NULL GROUP BY {column}"
            else:
                query = f"SELECT 'total', COUNT(*) FROM {table}"
            
            cursor.execute(query)
            for row in cursor.fetchall():
                key, count = row
                cms.add_string(str(key), int(count))
            
            sketch_data = cms.serialize()
        
        else:
            return jsonify({"error": "unsupported sketch type"}), 400
        
        # Store sketch
        storage.upsert_sketch(table, column, sketch_type, sketch_data, json.dumps(parameters))
        
        return jsonify({
            "status": "ok",
            "sketch_type": sketch_type,
            "size_bytes": len(sketch_data)
        })
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/sketches', methods=['GET'])
def get_sketches():
    """List all sketches for a table"""
    try:
        table = request.args.get('table', '')
        if not table:
            return jsonify({"error": "table parameter required"}), 400
        
        sketches = storage.list_sketches(table)
        return jsonify({"sketches": sketches})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/ml/stats', methods=['GET'])
def get_learning_stats():
    """Get ML learning statistics"""
    try:
        from ml_optimizer import get_learning_stats
        
        conn = storage.get_connection()
        stats = get_learning_stats(conn)
        
        return jsonify({
            "status": "ok",
            "learning_stats": stats
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    port = app.config['PORT']
    host = app.config['HOST']
    debug = app.config['DEBUG']
    
    print(f"🧠 ML-Powered Approximate Query Engine")
    print(f"📊 Server listening on http://{host}:{port}")
    print(f"🗄️  Database: {app.config['DB_PATH']}")
    
    app.run(host=host, port=port, debug=debug)
