from copy import deepcopy
from functools import partial
import os

import numpy as np 
from tqdm import tqdm

from configs import query_results_dir, DataSource, ANALYSIS_FPS, persistence_distributions_dir, masked_features_dir, features_dir, hours
from utils import frame_iterator

class Query:
    
    def __init__(self, vid, data_source, per_chunk_fn, frames_per_chunk, per_window_fn, chunks_per_window, privid_params=None, smart_adjust=False):
        self.vid = vid

        self.per_chunk_fn = per_chunk_fn
        self.frames_per_chunk = frames_per_chunk
        self.data_source = data_source

        self.smart_adjust = smart_adjust

        # assert self.data_source in [DataSource.ORIGINAL, DataSource.SPATIAL_HYBRID, DataSource.JUST_HYBRID]
        # at the moment, not sure what other data sources are for... so let's support these only for now 

        self.per_window_fn = per_window_fn
        self.chunks_per_window = chunks_per_window

        self.privid_params = privid_params

        # if self.data_source is not DataSource.ORIGINAL:
        #     assert self.privid_params is not None
        #     #todo: could check if DataSource.SPATIAL_HYBRID and if spatial params are not None

        # total frames = ANALYSIS_FPS frames/s * 60 s/min * 60 min/hr * 12 hr
        num_chunks = int(ANALYSIS_FPS * 60 * 60 * 12 / self.frames_per_chunk)
        assert num_chunks % self.chunks_per_window == 0

        self.num_windows = num_chunks//self.chunks_per_window 

        self.per_chunk_results = None # per-chunk data after applying per-chunk function

        if self.data_source is not None:
            self.per_chunk_results_fname = self._get_per_chunk_results_fname()
            # print(self.per_chunk_results_fname)

        self.persistence = None

    def _get_per_chunk_results_fname(self):

        per_chunk_fn_descriptor = self.per_chunk_fn.get_descriptor()

        import json

        # TODO: clean up repeated text; also datasource fnames are awful; also prob dont need keys in fnames...
        if self.data_source is DataSource.SPATIAL_HYBRID:
            n_splits = self.privid_params.num_splits
            return f"{query_results_dir}{self.vid}_{self.data_source}_FPS:{self.frames_per_chunk:d}_{per_chunk_fn_descriptor}_NSPLIT:{n_splits}_SA.json"
        elif self.data_source is DataSource.JUST_HYBRID:
            return f"{query_results_dir}{self.vid}_{self.data_source}_FPS:{self.frames_per_chunk:d}_{per_chunk_fn_descriptor}_SA.json"
        elif self.data_source is DataSource.ORIGINAL:
            return f"{query_results_dir}{self.vid}_{self.data_source}_FPS:{self.frames_per_chunk:d}_{per_chunk_fn_descriptor}.json"
        else:
            assert False

    # loads per-chunk results 
    def _generate_per_chunk_results(self):
        import json

        if os.path.exists(self.per_chunk_results_fname):
            with open(self.per_chunk_results_fname, "r") as f:
                self.per_chunk_results = json.load(f)['results']
        else:
            self.per_chunk_results = list(self._get_query_results())

            with open(self.per_chunk_results_fname, "w") as f:
                json.dump({"results": self.per_chunk_results}, f)

    # generates per-window results
    def generate_per_window_results(self, apply_bounds_after_split=False):
        
        if self.per_chunk_results is None:
            self._generate_per_chunk_results()

        # print(min(self.per_chunk_results), max(self.per_chunk_results)) #, self.per_chunk_results)

        windowed_results = []

        if self.privid_params is not None:
            lower_bounds = self.privid_params.lower_bounds
            upper_bounds = self.privid_params.upper_bounds
            n_splits = self.privid_params.num_splits
            split_agg_fn = self.privid_params.split_agg_fn

        for i in range(self.num_windows):
            temp = deepcopy(self.per_chunk_results[i * self.chunks_per_window: (i + 1) * self.chunks_per_window])

            # aggregate across spatial splits!
            if self.data_source is DataSource.SPATIAL_HYBRID:

                split_results = [list() for _ in range(n_splits)]

                for split_idx in range(n_splits):

                    # TODO: come back to.. not handling case if type(temp[0][0]) == list but not smart adjust... also smart adjust may just always be used so could remove check
                    if self.smart_adjust:
                        split_results[split_idx] = list(map(lambda elem: max(elem[split_idx][0] - elem[split_idx][1], 0), temp))
                    else:
                        split_results[split_idx] = list(map(lambda elem: elem[split_idx], temp))
                    
                    if not apply_bounds_after_split:
                        split_results[split_idx] = list(map(lambda elem: max(lower_bounds[split_idx], elem), split_results[split_idx]))
                        split_results[split_idx] = list(map(lambda elem: min(upper_bounds[split_idx], elem), split_results[split_idx]))
                temp = list(map(split_agg_fn, zip(*split_results)))
                if apply_bounds_after_split:
                    temp = list(map(lambda elem: max(lower_bounds, elem), temp))
                    temp = list(map(lambda elem: min(upper_bounds, elem), temp))

            elif self.data_source is DataSource.JUST_HYBRID:
                if self.smart_adjust:
                    temp = list(map(lambda elem: max(elem[0] - elem[1], 0), temp))
                elif type(temp[0]) == list:
                    temp = list(map(lambda elem: elem[0], temp))                    

                if self.privid_params is not None:
                    temp = list(map(lambda elem: max(lower_bounds, elem), temp))
                    temp = list(map(lambda elem: min(upper_bounds, elem), temp))

            else:
                if type(temp[0]) == list:
                    temp = list(map(lambda elem: elem[0], temp))
    
            windowed_results.append(self.per_window_fn(temp))
        return windowed_results

    # unit of persistence: frames
    def _load_persistence_value(self):

        # 10 because that's the box size we're using in the heat maps! could parameterize the value here...
        persistence_pkl_template = f"{persistence_distributions_dir}{self.vid}/{self.vid}10_{{ds_type}}.pkl"

        ds_type = "hybrid" if self.data_source in [DataSource.JUST_HYBRID, DataSource.SPATIAL_HYBRID] else "orig"
        
        persistence_pkl_template = persistence_pkl_template.format(ds_type=ds_type)

        import pickle
        with open(persistence_pkl_template, 'rb') as f:
            persistence_distribution = pickle.load(f)
        self.persistence = max(persistence_distribution)

    # use_frames_as_unit: multiply noise scale by frames per chunk
    # persistence (unit=frames)
    def get_noise_scale(self, use_frames_as_unit=False, persistence=None):
        if persistence is None:
            if self.persistence is None:
                self._load_persistence_value()
            persistence = self.persistence

        epsilon = self.privid_params.epsilon

        l = self.privid_params.lower_bounds
        u = self.privid_params.upper_bounds

        # convert list to int
        if type(l) == list:
            l = min(l)
            u = max(u)

        import math
        persistence_chunks = math.ceil(float(persistence) / float(self.frames_per_chunk)) + 1 # unit is now chunks

        if self.per_window_fn == np.sum:
            sensitivity = float((u - l) * persistence_chunks)
        elif self.per_window_fn == np.mean:
             sensitivity = float((u - l) * persistence_chunks) / float(self.frames_per_chunk * self.chunks_per_window)
        else:
            assert False, "Sensitivity not implemented for the used window agg fn."
        
        noise_scale = sensitivity / epsilon

        if use_frames_as_unit:
            return noise_scale * self.frames_per_chunk
        return noise_scale

    def get_noise_ribbon(self, noise_scale):
        return linvcdf(.99, 0, noise_scale)

    def get_noise(self, num_noise_samples, seed, noise_scale):
        np.random.seed(seed)
        return np.random.laplace(loc=0, scale=noise_scale, size=num_noise_samples)

    def get_noise_multiple(self, noise_scale, num_iter=100, num_samples=1, seed=2):
        np.random.seed(seed)
        return np.array([np.random.laplace(loc=0, scale=noise_scale, size=num_samples)for i in range(num_iter)])

    def _get_raw_data_fname_template(self, split_id=None):
        # only handling features, not bboxes... but see line 47 in QueryAnalyzer.py in old repo
        assert self.per_chunk_fn.use_deep_sort_features

        if self.per_chunk_fn.use_deep_sort_features:
            template = f"{{dir}}{self.vid}/{self.vid}{{hour}}{{split_info}}.csv"
            dir = masked_features_dir
            split_info = ""
            
            if self.data_source == DataSource.ORIGINAL:
                dir = features_dir
            
            if self.data_source == DataSource.SPATIAL_HYBRID:
                assert split_id is not None
                split_info = f"_m{split_id}"

            return partial(template.format, dir=dir, split_info=split_info)

        else:
            pass
            # not handling yet

    def _load_raw_data(self, split_id=None):
        import pandas as pd
        fname_template = self._get_raw_data_fname_template(split_id)
        for hr in tqdm(hours[self.vid]):
            fname = fname_template(hour=hr)
            df = pd.read_csv(fname, header=None)
            for frame_no in frame_iterator():
                yield df[df[0] == frame_no].values

    def _load_chunked_data(self, split_id=None):
        raw_data_loader = self._load_raw_data(split_id)
        chunk_counter = 1
        chunks = []
        for frame_counter, frame in enumerate(raw_data_loader):
            while frame_counter >= chunk_counter * self.frames_per_chunk:
                chunk_counter += 1
                yield chunks
                chunks = []
            chunks.append(frame)
        yield chunks

    def _get_query_results(self):
        if self.data_source == DataSource.SPATIAL_HYBRID:
            split_gens = map(self._load_chunked_data, range(self.privid_params.num_splits))
            for split_chunks in zip(*split_gens):
                yield list(map(self.per_chunk_fn, split_chunks))
        else:
            for chunk in self._load_chunked_data():
                yield self.per_chunk_fn(chunk)

def linvcdf(p, u, b):
    return u - (b * np.sign(p - 0.5) * np.log(1 - (2 * np.abs(p - 0.5))))
