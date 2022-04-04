# %%

import numpy as np

from configs import DataSource
from Query import Query
from configs import ANALYSIS_FPS, PrividAnalysisConfig
from per_chunk_functions import NullFunction, DeepSortTrackerUpwardTrajectory

def get_accuracy(gt, privid):
    results = 1 - np.abs(privid-gt)/gt
    return (np.mean(results), np.std(results))

def shibuya_tree_helper():
    raw_result = float(4/6)
    gt_source = DataSource.JUST_HYBRID
    return "shibuya", raw_result, gt_source

def auburn_tree_helper():
    raw_result = float(15/15)
    gt_source = DataSource.JUST_HYBRID
    return "auburn", raw_result, gt_source

def hampton_tree_helper():
    raw_result = float(3/7)
    gt_source = DataSource.JUST_HYBRID
    return "hampton", raw_result, gt_source

# case 3 | Q7, Q8, Q9 | fraction of trees with leaves (%)
# 1 chunk per frame
# window size: 12 hours = 8 * 60 * 60 * 12
def fraction_of_trees_with_leaves(helper_fn):
    name, raw_result, gt_data_source = helper_fn()
    privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=1)
    query = Query(name, gt_data_source, NullFunction(), frames_per_chunk=1, per_window_fn=np.mean, chunks_per_window=ANALYSIS_FPS*60*60*12, privid_params=privid_params, smart_adjust=False)
    ns = query.get_noise_scale()
    noise = query.get_noise_multiple(ns)
    mean_acc, std_acc = get_accuracy(raw_result, noise+raw_result)
    return mean_acc, std_acc, query.persistence

# print(fraction_of_trees_with_leaves(shibuya_tree_helper))
# (0.9939092546577122, 0.006580218732517105, 1604)
# print(fraction_of_trees_with_leaves(auburn_tree_helper))
# (0.9990082773939037, 0.0010714208694275014, 391)
# print(fraction_of_trees_with_leaves(hampton_tree_helper))
# (0.9823733112987882, 0.019043230453038795, 2985)

# case 4 | Q10, Q11, Q12 | duration of red lights (seconds)
# persistence is 0; chunk size = 10 min
def red_light_duration_helper(video_info):
    name, raw_result = video_info
    privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=300)
    query = Query(name, None, NullFunction(), frames_per_chunk=ANALYSIS_FPS*60*10, per_window_fn=np.mean, chunks_per_window=6*12, privid_params=privid_params, smart_adjust=False)
    ns = query.get_noise_scale(persistence=0)
    noise = query.get_noise_multiple(ns)
    mean_acc, std_acc = get_accuracy(raw_result, noise+raw_result)
    return (1-mean_acc), std_acc

hampton_red_light_info = ("hampton", ANALYSIS_FPS*50)
auburn_red_light_info = ("auburn", ANALYSIS_FPS*75)
shibuya_red_light_info = ("shibuya", ANALYSIS_FPS*100)

# print(red_light_duration_helper(shibuya_red_light_info))
# (0.9999990512857724, 1.0249561888631251e-06)
# print(red_light_duration_helper(auburn_red_light_info))
# (0.9999987350476964, 1.3666082518245698e-06)
# print(red_light_duration_helper(hampton_red_light_info))
# (0.9999981025715446, 2.049912377726795e-06)

# case 5 | Q13 | number of people walking upward (toward campus)
# def auburn_upward_count():
#     truth = 0 + 3 + 9 + 38 + 30 + 63 + 80 + 84 + 133 + 33 + 47 + 56
#     privid_prenoise = 0 + 3 + 9 + 21 + 23 + 49 + 70 + 74 + 105 + 26 + 34 + 42

#     privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=25)
#     query = Query("auburn", DataSource.JUST_HYBRID, NullFunction(), frames_per_chunk=ANALYSIS_FPS*60*10, per_window_fn=np.sum, chunks_per_window=6*12, privid_params=privid_params, smart_adjust=False)
#     ns = query.get_noise_scale()
#     noise = query.get_noise_multiple(ns, num_iter=1000)
#     mean_acc, std_acc = get_accuracy(truth, noise+privid_prenoise)
#     return mean_acc, std_acc

# print(auburn_upward_count())
# diff num from paper but produces same as code in previous repository
# (0.7821132651145426, 0.10560901996020419) 

# case 5 | Q13 | number of people walking upward (toward campus)
def auburn_upward_count():

    # query_fn = DeepSortTrackerUpwardTrajectory(cf_thresh=0.8, max_age=96, max_cos=0.5, max_iou=0.7, n_init=9, nms=1.0, min_dist=10)
    # gt_query = Query("auburn", DataSource.ORIGINAL, query_fn, frames_per_chunk=ANALYSIS_FPS*60*60, per_window_fn=np.sum, chunks_per_window=1, smart_adjust=False)
    # truth = np.sum(gt_query.generate_per_window_results())

    # privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=25)

    # query = Query("auburn", DataSource.JUST_HYBRID, query_fn, frames_per_chunk=ANALYSIS_FPS*60*10, per_window_fn=np.sum, chunks_per_window=6*12, privid_params=privid_params, smart_adjust=False)

    # privid_prenoise = np.sum(query.generate_per_window_results())
    # ns = query.get_noise_scale()
    # noise = query.get_noise_multiple(ns, num_iter=1000)
    # mean_acc, std_acc = get_accuracy(truth, noise+privid_prenoise)
    # return mean_acc, std_acc

    query_fn = DeepSortTrackerUpwardTrajectory(cf_thresh=0.8, max_age=96, max_cos=0.5, max_iou=0.7, n_init=9, nms=1.0, min_dist=10)
    gt_query = Query("auburn", DataSource.ORIGINAL, query_fn, frames_per_chunk=ANALYSIS_FPS*60*60, per_window_fn=np.sum, chunks_per_window=1, smart_adjust=False)
    truth = np.sum(gt_query.generate_per_window_results())

    for seconds_per_chunk in [3600, 1200, 600, 60, 50, 40, 30, 20, 10, 5]:

        for ub in [2, 3, 5, 10, 20, 25, 30, 40, 60, 100, 120]:

            frames_per_chunk = int(seconds_per_chunk * ANALYSIS_FPS)
            chunks_per_window = int(12 * 60 * 60 * ANALYSIS_FPS / frames_per_chunk)

            privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=ub)

            query = Query("auburn", DataSource.JUST_HYBRID, query_fn, frames_per_chunk=frames_per_chunk, per_window_fn=np.sum, chunks_per_window=chunks_per_window, privid_params=privid_params, smart_adjust=False)

            privid_prenoise = np.sum(query.generate_per_window_results())
            ns = query.get_noise_scale()
            noise = query.get_noise_multiple(ns, num_iter=1000)
            mean_acc, std_acc = get_accuracy(truth, noise+privid_prenoise)

            print(f"{seconds_per_chunk:>4} | {ub:>3} | {round(mean_acc, 2):>5.2f} | {int(ns):>4}")

print(auburn_upward_count())

# seconds per chunk | upper bound | acc | noise scale

# 3600 |   2 |  0.04 |    4
# 3600 |   3 |  0.06 |    6
# 3600 |   5 |  0.09 |   10
# 3600 |  10 |  0.18 |   20
# 3600 |  20 |  0.33 |   40
# 3600 |  25 |  0.40 |   50
# 3600 |  30 |  0.45 |   60
# 3600 |  40 |  0.54 |   80
# 3600 |  60 |  0.62 |  120
# 3600 | 100 |  0.59 |  200
# 3600 | 120 |  0.54 |  240
# 1200 |   2 |  0.10 |    4
# 1200 |   3 |  0.15 |    6
# 1200 |   5 |  0.25 |   10
# 1200 |  10 |  0.43 |   20
# 1200 |  20 |  0.63 |   40
# 1200 |  25 |  0.68 |   50
# 1200 |  30 |  0.72 |   60
# 1200 |  40 |  0.74 |   80
# 1200 |  60 |  0.71 |  120
# 1200 | 100 |  0.60 |  200
# 1200 | 120 |  0.54 |  240
#  600 |   2 |  0.19 |    4
#  600 |   3 |  0.27 |    6
#  600 |   5 |  0.41 |   10
#  600 |  10 |  0.61 |   20
#  600 |  20 |  0.75 |   40
#  600 |  25 |  0.76 |   50
#  600 |  30 |  0.76 |   60
#  600 |  40 |  0.76 |   80
#  600 |  60 |  0.71 |  120
#  600 | 100 |  0.60 |  200
#  600 | 120 |  0.54 |  240
#   60 |   2 |  0.54 |    4
#   60 |   3 |  0.65 |    6
#   60 |   5 |  0.76 |   10
#   60 |  10 |  0.80 |   20
#   60 |  20 |  0.79 |   40
#   60 |  25 |  0.79 |   50
#   60 |  30 |  0.78 |   60
#   60 |  40 |  0.76 |   80
#   60 |  60 |  0.72 |  120
#   60 | 100 |  0.60 |  200
#   60 | 120 |  0.54 |  240
#   50 |   2 |  0.57 |    4
#   50 |   3 |  0.69 |    6
#   50 |   5 |  0.79 |   10
#   50 |  10 |  0.83 |   20
#   50 |  20 |  0.82 |   40
#   50 |  25 |  0.81 |   50
#   50 |  30 |  0.80 |   60
#   50 |  40 |  0.78 |   80
#   50 |  60 |  0.73 |  120
#   50 | 100 |  0.62 |  200
#   50 | 120 |  0.55 |  240
#   40 |   2 |  0.58 |    6
#   40 |   3 |  0.70 |    9
#   40 |   5 |  0.79 |   15
#   40 |  10 |  0.82 |   30
#   40 |  20 |  0.81 |   60
#   40 |  25 |  0.79 |   75
#   40 |  30 |  0.77 |   90
#   40 |  40 |  0.73 |  120
#   40 |  60 |  0.65 |  180
#   40 | 100 |  0.46 |  300
#   40 | 120 |  0.36 |  360
#   30 |   2 |  0.58 |    6
#   30 |   3 |  0.69 |    9
#   30 |   5 |  0.80 |   15
#   30 |  10 |  0.83 |   30
#   30 |  20 |  0.81 |   60
#   30 |  25 |  0.79 |   75
#   30 |  30 |  0.77 |   90
#   30 |  40 |  0.73 |  120
#   30 |  60 |  0.65 |  180
#   30 | 100 |  0.46 |  300
#   30 | 120 |  0.36 |  360
#   20 |   2 |  0.62 |    8
#   20 |   3 |  0.73 |   12
#   20 |   5 |  0.83 |   20
#   20 |  10 |  0.84 |   40
#   20 |  20 |  0.80 |   80
#   20 |  25 |  0.78 |  100
#   20 |  30 |  0.75 |  120
#   20 |  40 |  0.69 |  160
#   20 |  60 |  0.56 |  240
#   20 | 100 |  0.30 |  400
#   20 | 120 |  0.17 |  480
#   10 |   2 |  0.71 |   12
#   10 |   3 |  0.81 |   18
#   10 |   5 |  0.88 |   30
#   10 |  10 |  0.86 |   60
#   10 |  20 |  0.77 |  120
#   10 |  25 |  0.73 |  150
#   10 |  30 |  0.68 |  180
#   10 |  40 |  0.58 |  240
#   10 |  60 |  0.38 |  360
#   10 | 100 | -0.02 |  600
#   10 | 120 | -0.22 |  720
#    5 |   2 |  0.85 |   22
#    5 |   3 |  0.92 |   33
#    5 |   5 |  0.91 |   55
#    5 |  10 |  0.82 |  110
#    5 |  20 |  0.63 |  220
#    5 |  25 |  0.54 |  275
#    5 |  30 |  0.45 |  330
#    5 |  40 |  0.26 |  440
#    5 |  60 | -0.10 |  660
#    5 | 100 | -0.84 | 1100
#    5 | 120 | -1.21 | 1320