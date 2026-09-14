from time import perf_counter
from typing import Any


class TimerUtil:
    def __enter__(self):
        self.start_time = perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = perf_counter()
        self.elapsed_time = self.end_time - self.start_time


class MetricUtil:
    @classmethod
    def combine_run_metrics(cls, run_metrics: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Aggregate numeric metrics together to across a list of run metrics.
        Non-numeric metrics are dropped
        """
        combined = {}
        all_available_metrics = [
            metric for dictionary in run_metrics for metric in dictionary
        ]
        for metric in all_available_metrics:
            running_total = None
            for dictionary in run_metrics:
                new_value = dictionary.get(metric, None)
                if isinstance(new_value, (int, float)):
                    if running_total is None:
                        running_total = new_value
                    else:
                        running_total += new_value
            if running_total is not None:
                combined[metric] = running_total
        return combined
