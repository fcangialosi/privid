# %%

from Query import Query
from per_chunk_functions import SortTracker, DeepSortTracker
from configs import DataSource, Label, PrividAnalysisConfig, ANALYSIS_FPS
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import interp1d
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import seaborn as sns

sns.set()
sns.set(font_scale=1.5, palette="GnBu_d")
sns.set_style("ticks")

color_list = sns.color_palette()

def get_shibuya_queries():

    # ground truth
    _shibuya_person_deep_sort_tracker = DeepSortTracker(cf_thresh=0.6, max_age=96, max_cos=0.1, max_iou=0.5, n_init=5, nms=1.0)
    gt_query = Query("shibuya", DataSource.JUST_HYBRID, _shibuya_person_deep_sort_tracker, frames_per_chunk=ANALYSIS_FPS*60*60, per_window_fn=np.sum, chunks_per_window=1, smart_adjust=False)

    # privid
    privid_params = PrividAnalysisConfig(lower_bounds=[0, 0, 0, 0], upper_bounds=[23, 19, 23, 19], epsilon=1.0, split_agg_fn=np.sum, num_splits=4)
    privid_query = Query("shibuya", DataSource.SPATIAL_HYBRID, _shibuya_person_deep_sort_tracker, frames_per_chunk=240, per_window_fn=np.sum, chunks_per_window=120, privid_params=privid_params, smart_adjust=True)

    return gt_query, privid_query, "# Unique People \nIn Crosswalks"

def get_hampton_queries():

    # ground truth
    # frames per chunk used here is number of frames in an hour
    _hampton_car_sort_tracker = SortTracker(cf_thresh=0.2, label=Label.CAR, max_age=720, min_hits=9, iou_dist=0.1)
    gt_query = Query("hampton", DataSource.ORIGINAL, _hampton_car_sort_tracker, frames_per_chunk=ANALYSIS_FPS*60*60, per_window_fn=np.sum, chunks_per_window=1, smart_adjust=False)

    # privid
    # PORTNOTE: `frames per chunk` (FPC), `upper_bounds` (UPPER), `chunks_per_window` (CPW) based on HAMPTON_BESTS directory in "final_plot_queries.py" of old repo
    privid_params = PrividAnalysisConfig(lower_bounds=[0, 0], upper_bounds=[8,8], epsilon=1.0, split_agg_fn=np.sum, num_splits=2)
    privid_query = Query("hampton", DataSource.SPATIAL_HYBRID, _hampton_car_sort_tracker, frames_per_chunk=80, per_window_fn=np.sum, chunks_per_window=360, privid_params=privid_params, smart_adjust=True)

    return gt_query, privid_query, "# Unique Cars"

def get_auburn_queries():
    _auburn_person_deep_sort_tracker = DeepSortTracker(cf_thresh=0.8, max_age=96, max_cos=0.5, max_iou=0.7, n_init=9, nms=1.0)
    gt_query = Query("auburn", DataSource.ORIGINAL, _auburn_person_deep_sort_tracker, frames_per_chunk=ANALYSIS_FPS*60*60, per_window_fn=np.sum, chunks_per_window=1, smart_adjust=False)

    privid_params = PrividAnalysisConfig(lower_bounds=0, upper_bounds=6, epsilon=1.0)
    privid_query = Query("auburn", DataSource.JUST_HYBRID, _auburn_person_deep_sort_tracker, frames_per_chunk=240, per_window_fn=np.sum, chunks_per_window=240, privid_params=privid_params, smart_adjust=True)

    return gt_query, privid_query, "# Unique People"

def plot_subgraph(gt_query, privid_query, ylabel, ax_ptr):

    # plotting ground truth
    gt_y = gt_query.generate_per_window_results()
    gt_x = range(len(gt_y))
    if gt_query.vid == "auburn":
        gt_y = [sum(gt_y[i:i+2]) for i in range(0, 12, 2)]
        gt_x = [i for i in range(0, 12, 2)]
    ax_ptr.plot(gt_x, gt_y, 'o-', color=color_list[0], label="Original", ms=3)

    # plotting privid results
    y = privid_query.generate_per_window_results()
    if gt_query.vid == "hampton":
        ns = privid_query.get_noise_scale(persistence=2 * 60 * 8)
        print("Using custom persistence for hampton...")
    else:
        ns = privid_query.get_noise_scale()
    bound = privid_query.get_noise_ribbon(ns)
    m = interp1d([0, len(y)], [0, 12])
    x = list(map(m, range(len(y))))
    ax_ptr.plot(x, y, 'o-', color=color_list[1], label="Privid", ms=3)
    ax_ptr.fill_between(x, y + bound, y - bound, alpha=0.2, color=color_list[1])

    # add labels, axes, limits...
    time_str = ['6a', '8a', '10a', '12p', '2p', '4p']
    ax_ptr.set_xticks(range(0, 12, 2))
    ax_ptr.set_xticklabels(time_str)
    ax_ptr.set_xlim(x[0], x[-1])
    ax_ptr.set_ylabel(ylabel)
    ax_ptr.set_title(gt_query.vid)
    ax_ptr.set_ylim(bottom=0)
    
def draw_figs():

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20,4))
    fig.tight_layout(pad=3)

    plot_subgraph(*get_auburn_queries(), ax1)
    plot_subgraph(*get_hampton_queries(), ax2)
    plot_subgraph(*get_shibuya_queries(), ax3)

    ax2.set_xlabel("Time (Hours)")

    legend_elements = [Line2D([0], [0], color=color_list[0], ls='-', marker='o', ms=3, label='Original'),
                        Line2D([0], [0], color=color_list[1], ls='-', marker='o', ms=3, label='Privid (No Noise)'),
                        Patch(facecolor=color_list[1], alpha=0.2, label="Privid")]

    ax2.legend(handles=legend_elements, loc='lower center', labelspacing=0.1, borderpad=0.19, columnspacing=.4)
    plt.tight_layout()
    plt.subplots_adjust(wspace=0.32)
    plt.savefig("graphs/fig5.pdf", bbox_inches='tight', pad_inches=0)
    plt.show()
    
if __name__ == "__main__":
    draw_figs()
