# Privid (USENIX NSDI '22)

This repository contains the code needed to reproduce the results in our [NSDI '22 paper](https://www.usenix.org/system/files/nsdi22-paper-cangialosi.pdf).

Components:
* `pql` (Privid Query Language): parses Privid queries and computes the sensitivity needed to satisfy (p,K,e)-event-duration privacy given a privacy policy.
* `analysis`: code for reproducing plots in our paper.

Data:
* For Q1-Q3 and Q7-Q13, we collected video from 3 cameras (12 hours each) livestreamed to YouTube. We can provide the original source videos upon request. For now, we provide only the parsed data which we use in our analysis [here](https://www.dropbox.com/sh/ovfgy0u4ll39ebu/AAAx-0mNBlr_1tuJgMIZlC5Da?dl=0).
* For Q4-Q6, we generated a synthetic dataset of appearances of taxis in front of 127 cameras over 1.5 years in Porto, Portugal, based on the [ECML PKDD 2015 dataset challenge](https://archive.ics.uci.edu/ml/datasets/Taxi+Service+Trajectory+-+Prediction+Challenge,+ECML+PKDD+2015). Details can be found in `analysis/porto`.

