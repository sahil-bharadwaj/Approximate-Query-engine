from src.engine import ApproximateQueryEngine

def test_exact_and_approximate():
    engine = ApproximateQueryEngine('data/data_file_1.csv')
    
    exact_result = engine.run_query('COUNT', 'column_name', mode='exact')
    approx_result = engine.run_query('COUNT', 'column_name', mode='approximate', accuracy=0.1)
    
    assert exact_result['result'] >= approx_result['result']  # Sample size makes sense
