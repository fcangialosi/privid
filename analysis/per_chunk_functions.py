from functools import reduce
from operator import iconcat

import numpy as np

from deep_sort.deep_sort.nn_matching import NearestNeighborDistanceMetric
from deep_sort.deep_sort.tracker import Tracker
from deep_sort.deep_sort.detection import Detection
from deep_sort.application_util.preprocessing import non_max_suppression

# get applied to each chunk!
# actually, not sure if this base class is necessary...
class ChunkFunction:
    def __init__(self):
        self.use_deep_sort_features = False

class DeepSortTracker(ChunkFunction):

    def __init__(self, cf_thresh, max_cos, max_iou, max_age, nms, n_init):
        self.cf_thresh = cf_thresh
        self.max_cos = max_cos
        self.max_iou = max_iou
        self.max_age = max_age
        self.nms = nms
        self.n_init = n_init
        self.use_deep_sort_features = True

    def get_descriptor(self):
        return f"DEEPSORT_CF:{self.cf_thresh:.2f}_CO:{self.max_cos:.2f}_IO:{self.max_iou:.2f}_MA:{self.max_age:d}_NMS:{self.nms:.2f}_NI:{self.n_init:.2f}"

    def __call__(self, frames):
        metric = NearestNeighborDistanceMetric("cosine", self.max_cos, budget=None)
        tracker = Tracker(metric, max_iou_distance=self.max_iou, max_age=self.max_age, n_init=self.n_init)
        chunk_results = []
        smart_adjust_dets_per_frame = []
        for rows in frames:
            # shouldn't this be r[6:], not r[5:]? why would we add the confidence to the features?
            # keeping current version to stay equivalent to previous implementation
            detections = [Detection(r[1:5], r[5], r[5:]) for r in rows if r[5] >= self.cf_thresh]
            boxes_tlwh = np.array([d.tlwh for d in detections])
            scores = [d.confidence for d in detections]
            indices = non_max_suppression(boxes_tlwh, self.nms, scores)
            detections_out = [detections[i] for i in indices]
            smart_adjust_dets_per_frame.append(len(detections_out))
            tracker.predict()
            tracker.update(detections_out)
            results = []
            for track in tracker.tracks:
                if not track.is_confirmed() or track.time_since_update > 0:
                    continue
                results.append([track.track_id])
            chunk_results.extend(results)
        values = set(reduce(iconcat, chunk_results, []))
        final_count = len(values)
        smart_adjust = 0
        if len(smart_adjust_dets_per_frame) != 0:
            smart_adjust = (smart_adjust_dets_per_frame[0] + smart_adjust_dets_per_frame[-1]) * 0.5
        return final_count, smart_adjust

class DeepSortTrackerUpwardTrajectory(ChunkFunction):

    def __init__(self, cf_thresh, max_cos, max_iou, max_age, nms, n_init, min_dist):
        self.cf_thresh = cf_thresh
        self.max_cos = max_cos
        self.max_iou = max_iou
        self.max_age = max_age
        self.nms = nms
        self.n_init = n_init
        self.use_deep_sort_features = True
        self.min_dist = min_dist

    def get_descriptor(self):
        min_dist = ""
        if self.min_dist != 10:
            min_dist = f"_MD:{self.min_dist}"
        return f"DEEPSORT_CF_UPWARD_TRAJ:{self.cf_thresh:.2f}_CO:{self.max_cos:.2f}_IO:{self.max_iou:.2f}_MA:{self.max_age:d}_NMS:{self.nms:.2f}_NI:{self.n_init:.2f}{min_dist}"

    def __call__(self, frames):
        metric = NearestNeighborDistanceMetric("cosine", self.max_cos, budget=None)
        tracker = Tracker(metric, max_iou_distance=self.max_iou, max_age=self.max_age, n_init=self.n_init)
        chunk_results = {}
        for rows in frames:
            # shouldn't this be r[6:], not r[5:]? why would we add the confidence to the features?
            # keeping current version to stay equivalent to previous implementation
            detections = [Detection(r[1:5], r[5], r[5:]) for r in rows if r[5] >= self.cf_thresh]
            boxes_tlwh = np.array([d.tlwh for d in detections])
            scores = [d.confidence for d in detections]
            indices = non_max_suppression(boxes_tlwh, self.nms, scores)
            detections_out = [detections[i] for i in indices]
            tracker.predict()
            tracker.update(detections_out)
            for track in tracker.tracks:
                if not track.is_confirmed() or track.time_since_update > 0:
                    continue
                if track.track_id not in chunk_results:
                    chunk_results[track.track_id] = [track.to_tlbr()[1]]  # gets me min y
                else:
                    chunk_results[track.track_id].append(track.to_tlbr()[1])

        upward = 0.0
        downward = 0.0

        for traj in chunk_results.values():
            # compares first appearance to last appearance
            val = traj[0] - traj[-1]
            if val < self.min_dist * -1:
                downward += 1
            elif val > self.min_dist:
                upward += 1
            else:
                pass
        
        return upward

class SortTracker(ChunkFunction):
    def __init__(self, cf_thresh, label, max_age, min_hits, iou_dist):
        self.cf_thresh = cf_thresh
        self.label = label
        self.max_age = max_age
        self.min_hits = min_hits
        self.iou_dist = iou_dist

    def get_descriptor(self):
         return f"SORT_CF:{self.cf_thresh:.2f}_{self.label}_IO:{self.iou_dist:.2f}_MA:{self.max_age:d}_MH:{self.min_hits:d}"

class NullFunction(ChunkFunction):

    def __init__(self):
        pass

    def get_descriptor(self):
        return ""