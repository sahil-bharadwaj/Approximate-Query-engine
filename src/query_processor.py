import time
import random
import numpy as np
from collections import defaultdict

class QueryProcessor:
    def __init__(self, data):
        # data should be a list of dicts, e.g. [{'col1': val1, 'col2': val2}, ...]
        self.data = data
        self.total_rows = len(data)
        # Pre-calculate random indices for different sample sizes
        self.sample_indices = {}
    
    def _get_sample_size(self, accuracy):
        """
        Calculate appropriate sample size based on accuracy.
        Using statistical sampling formula for finite population.
        """
        confidence_level = 0.95  # 95% confidence level
        z_score = 1.96  # z-score for 95% confidence
        margin_of_error = 1.0 - accuracy
        
        # Sample size calculation formula
        numerator = (z_score**2 * 0.25 * self.total_rows)
        denominator = ((margin_of_error**2) * (self.total_rows - 1)) + (z_score**2 * 0.25)
        sample_size = int(numerator / denominator)
        
        return min(sample_size, self.total_rows)
    
    def _get_random_sample(self, accuracy):
        """
        Get or create a random sample based on accuracy.
        Cache the sample indices for reuse.
        """
        sample_size = self._get_sample_size(accuracy)
        
        if sample_size not in self.sample_indices:
            # Generate random indices without replacement
            self.sample_indices[sample_size] = random.sample(range(self.total_rows), sample_size)
        
        return [self.data[i] for i in self.sample_indices[sample_size]]

    def approximate_query(self, query_type, column, accuracy=0.1):
        """
        Performs approximate queries using statistically valid random sampling.
        Optimized for better performance and accuracy.
        """
        start_time = time.time()
        sampled_rows = self._get_random_sample(accuracy)
        sample_size = len(sampled_rows)
        scaling_factor = self.total_rows / sample_size
        result = None

        if query_type == 'COUNT':
            # For COUNT, we can directly scale the sample size
            result = int(sample_size * scaling_factor)

        elif query_type == 'SUM':
            # Using numpy for faster sum calculation
            try:
                values = np.array([float(row.get(column, 0) or 0) for row in sampled_rows])
                sample_sum = np.sum(values)
                result = sample_sum * scaling_factor
            except ValueError:
                # Fallback for non-numeric values
                sample_sum = sum(float(row.get(column, 0) or 0) for row in sampled_rows)
                result = sample_sum * scaling_factor

        elif query_type == 'AVG':
            # For average, no need to scale as it's independent of sample size
            try:
                values = np.array([float(row.get(column, 0) or 0) for row in sampled_rows])
                result = np.mean(values) if len(values) > 0 else 0
            except ValueError:
                values = [float(row.get(column, 0) or 0) for row in sampled_rows]
                result = sum(values) / len(values) if values else 0

        elif query_type == 'GROUP_BY':
            # Using defaultdict for more efficient group counting
            group_counts = defaultdict(int)
            for row in sampled_rows:
                key = row.get(column)
                group_counts[key] += 1
            
            # Scale the counts and convert to regular dict
            result = {k: int(v * scaling_factor) for k, v in group_counts.items()}

        time_taken = time.time() - start_time
        return {
            'result': result, 
            'time_taken': time_taken,
            'sample_size': sample_size,
            'total_rows': self.total_rows,
            'sampling_ratio': sample_size / self.total_rows
        }

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