import pandas as pd

from configs import deepsort_tracking_results_dir, sort_tracking_results_dir

class BaselineDeepSortTracker:
    def __init__(self, vid, max_cos, max_iou, max_age, nms, n_init, conf):
        self.vid = vid
        self.max_cos = max_cos
        self.max_iou = max_iou
        self.max_age = max_age
        self.nms = nms
        self.n_init = n_init
        self.conf = conf

    def get_results_fname(self):
        return f"{deepsort_tracking_results_dir}{self.vid}/{self.vid}{{hour}}_{self.max_cos:0.2f}_{self.max_iou:0.2f}_{self.max_age:d}_{self.nms:0.2f}_{self.n_init:d}_{self.conf:0.2f}.csv"

    def get_results(self, hour):
        return pd.read_csv(self.get_results_fname().format(hour=hour), header=None)

class BaselineSortTracker:
    def __init__(self, vid, max_age, min_hits, iou_dist, conf):
        self.vid = vid
        self.max_age = max_age
        self.min_hits = min_hits
        self.iou_dist = iou_dist
        self.conf = conf

    def get_results_fname(self):
        return f"{sort_tracking_results_dir}{self.vid}/{self.vid}{{hour}}_{self.max_age:d}_{self.min_hits:d}_{self.iou_dist:0.2f}_{self.conf:0.2f}.csv"

    def get_results(self, hour):
        return pd.read_csv(self.get_results_fname().format(hour=hour), header=None, names=["frame", "id", "x1", "y1", "x2", "y2"])

    
