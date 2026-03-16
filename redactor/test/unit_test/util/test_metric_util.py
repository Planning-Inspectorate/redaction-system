from core.util.metric_util import MetricUtil


def test__metric_util__combine_metrics():
    mock_results = [
        {"metricA": 1, "metricB": 2, "metricC": 3},
        {"metricA": 2, "metricB": 3, "metricC": 4},
        {"metricD": 10, "metricA": 1},
        {"metricE": "bah"},
        {"metricA": 5, "metricB": 6, "metricC": 7},
    ]
    expected_combined_metrics = {
        "metricA": 9,
        "metricB": 11,
        "metricC": 14,
        "metricD": 10,
    }
    actual_combined_metrics = MetricUtil.combine_run_metrics(mock_results)
    assert expected_combined_metrics == actual_combined_metrics
