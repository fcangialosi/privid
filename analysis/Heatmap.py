import os
import pandas as pd
import json
from tqdm import tqdm

from BaselineTracker import BaselineDeepSortTracker, BaselineSortTracker

from configs import heat_squares_dir, persistence_distributions_dir, deepsort_tracking_results_dir, sort_tracking_results_dir

class Heatmap:

    def __init__(self, vid, width=1920, height=1080, box_side=10):
        self.vid = vid
        # self.hybrid_thresh = hybrid_conf[vid]

        self.box_side = box_side

        self.num_boxes_per_row = int(width/box_side)
        self.num_boxes_per_column = int(height/box_side)
        self.num_vals_in_a_box = float(box_side * box_side)

        self.heat_squares_dir = f"{heat_squares_dir}{vid}"
        os.makedirs(self.heat_squares_dir, exist_ok=True)

        self.heat_squares_fname = f"{self.heat_squares_dir}/{vid}{{hour}}_{box_side}_v2.json"
        # self.heat_squares_normalized_fname = f"{self.heat_squares_dir}/{vid}_{box_side}_normalized_heat_squares.pkl"

        # self.persistence_video_dir = f"{persistence_distributions_dir}{vid}"
        # os.makedirs(self.persistence_video_dir, exist_ok=True)


    # def get_persistence_pkl_fname(self, is_hybrid, hr=None):
    #     nm = "hybrid" if is_hybrid else "orig"
    #     hr = f"{hr}_" if hr is not None else ""
    #     return f"{persistence_video_dir}/{self.vid}{self.box_side}_{hr}_{nm}.pkl"

    def generate_heat_square_per_hour(self, trajectories: pd.DataFrame, hour: int):

        if os.path.exists(self.heat_squares_fname.format(hour=hour)):
            print(f"Heat Squares already generated for {self.vid} hour {hour}")
            return

        heat_squares = [dict() for i in range(self.num_boxes_per_row*self.num_boxes_per_column)]

        # todo: optimize
        for _, (frame, id, x1, y1, x2, y2) in tqdm(trajectories.iterrows(), total=len(trajectories)):
            for j in range(self.num_boxes_per_column):
                for i in range(self.num_boxes_per_row):
                    idx = j * self.num_boxes_per_row + i
                    sq_x1 = i * self.box_side
                    sq_x2 = sq_x1 + self.box_side
                    sq_y1 = j * self.box_side
                    sq_y2 = sq_y1 + self.box_side
                    if (not (sq_x1 > x2 or x1 > sq_x2)) and (not (sq_y1 > y2 or y1 > sq_y2)):
                        if id not in heat_squares[idx]:
                            heat_squares[idx][id] = [frame]
                        else:
                            heat_squares[idx][id].append(frame)

        with open(self.heat_squares_fname.format(hour=hour), "w") as f:
            json.dump({"data": heat_squares}, f)

        print("Completed Heat Square Generation for %s Hour %d." % (self.vid, hour))

hm = Heatmap("auburn")
auburn_baseline_tracker = BaselineDeepSortTracker("auburn", 0.5, 0.7, 96, 1.0, 9, 0.8)
for hour in range(7, 19):
    trajectories = auburn_baseline_tracker.get_results(hour)
    hm.generate_heat_square_per_hour(trajectories, hour)

hm = Heatmap("shibuya")
shibuya_baseline_tracker = BaselineDeepSortTracker("shibuya", 0.1, 0.5, 96, 1.0, 5, 0.6)
for hour in range(5, 17):
    trajectories = shibuya_baseline_tracker.get_results(hour)
    hm.generate_heat_square_per_hour(trajectories, hour)

hm = Heatmap("hampton")
hampton_baseline_tracker = BaselineSortTracker("hampton", 720, 9, 0.1, 0.2)
for hour in range(7, 19):
    trajectories = hampton_baseline_tracker.get_results(hour)
    hm.generate_heat_square_per_hour(trajectories, hour)