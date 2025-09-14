from src.data_loader import load_databases

class ApproximateQueryEngine:
    def __init__(self, data_paths):
        """
        Initialize the engine with one or more CSV file paths.
        Combines all CSVs into a single DataFrame.
        """
        self.df = load_databases(data_paths)

    def run_query(self, query_func, *args, **kwargs):
        """
        Run a query function on the combined DataFrame.
        query_func: a function that takes a DataFrame and returns a result.
        """
        return query_func(self.df, *args, **kwargs)