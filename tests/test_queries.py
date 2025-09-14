def test_sum_query():
    from src.engine import ApproximateQueryEngine
    engine = ApproximateQueryEngine('data/data_file_1.csv')

    exact_sum = engine.run_query('SUM', 'column_name', mode='exact')
    approx_sum = engine.run_query('SUM', 'column_name', mode='approximate', accuracy=0.1)

    assert isinstance(exact_sum['result'], (int, float))
    assert isinstance(approx_sum['result'], (int, float))
