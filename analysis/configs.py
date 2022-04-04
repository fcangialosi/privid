# TODO set path prefix to root directory containing all of the pre-parsed video data
# path_prefix = "/home/ubuntu/data"

query_results_dir = f"{path_prefix}/query_results/"
persistence_distributions_dir = f"{path_prefix}/persistence_distributions/"
masked_features_dir = f"{path_prefix}/masked_features/"
features_dir = f"{path_prefix}/features/"
heat_squares_dir = f"{path_prefix}/heat_squares/"
deepsort_tracking_results_dir = f"{path_prefix}/deep_sort_tracking_results/"
sort_tracking_results_dir = f"{path_prefix}/sort_tracking_results/"

ANALYSIS_FPS = 8
FPS = 30

hours = {
    "auburn": range(7, 19),
    "hampton": range(1, 13),
    "shibuya": range(1, 13)
}

from enum import Enum

class DataSource(Enum):
    ORIGINAL = 0
    SPATIAL_HYBRID = 1
    JUST_HYBRID = 2
    ORIGINAL_WITH_HYBRID = 3

class Label(Enum):
    PERSON = 0.0
    CAR = 2.0

class PrividAnalysisConfig:
    def __init__(self, lower_bounds=None, upper_bounds=None, epsilon=1.0, split_agg_fn=None, num_splits=None):
        self.epsilon = epsilon
        self.lower_bounds = lower_bounds # unit: unit of query result
        self.upper_bounds = upper_bounds # unit: unit of query result

        self.split_agg_fn =split_agg_fn
        self.num_splits = num_splits
