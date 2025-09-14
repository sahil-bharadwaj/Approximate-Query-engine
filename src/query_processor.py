import time

class QueryProcessor:
    def __init__(self, data):
        # data should be a list of dicts, e.g. [{'col1': val1, 'col2': val2}, ...]
        self.data = data

    def approximate_query(self, query_type, column, accuracy=0.1):
        """
        Approximates queries by sampling every Nth row, where N = int(1/accuracy).
        For SUM, AVG, and GROUP_BY, the result is scaled to estimate the full dataset.
        """
        start_time = time.time()
        step = max(1, int(1 / accuracy))
        sampled_rows = self.data[::step]
        result = None

        if query_type == 'COUNT':
            sampled_count = len(sampled_rows)
            result = sampled_count * step

        elif query_type == 'SUM':
            sampled_sum = sum(float(row.get(column, 0) or 0) for row in sampled_rows)
            result = sampled_sum * step

        elif query_type == 'AVG':
            values = [float(row.get(column, 0) or 0) for row in sampled_rows]
            result = sum(values) / len(values) if values else 0

        elif query_type == 'GROUP_BY':
            group_counts = {}
            for row in sampled_rows:
                key = row.get(column)
                group_counts[key] = group_counts.get(key, 0) + 1
            result = {k: v * step for k, v in group_counts.items()}

        time_taken = time.time() - start_time
        return {'result': result, 'time_taken': time_taken}

    def exact_query(self, query_type, column):
        """
        Runs exact queries on the full dataset.
        """
        start_time = time.time()
        result = None

        if query_type == 'COUNT':
            result = len(self.data)

        elif query_type == 'SUM':
            result = sum(float(row.get(column, 0) or 0) for row in self.data)

        elif query_type == 'AVG':
            values = [float(row.get(column, 0) or 0) for row in self.data]
            result = sum(values) / len(values) if values else 0

        elif query_type == 'GROUP_BY':
            group_counts = {}
            for row in self.data:
                key = row.get(column)
                group_counts[key] = group_counts.get(key, 0) + 1
            result = group_counts

        time_taken = time.time() - start_time
        return {'result': result, 'time_taken': time_taken}