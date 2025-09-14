from .data_loader import load_database
from .query_processor import QueryProcessor

class ApproximateQueryEngine:
    def __init__(self, data_path=None, db_uri=None):
        if db_uri:
            self.df = load_database(db_uri=db_uri)
        elif data_path:
            self.df = load_database(data_path=data_path)
        else:
            raise ValueError("Provide either data_path or db_uri")
        self.processor = QueryProcessor(self.df)

    def run_query(self, query_type, column, mode='approximate', accuracy=0.1):
        if mode == 'approximate':
            return self.processor.approximate_query(query_type, column, accuracy)
        else:
            return self.processor.exact_query(query_type, column)