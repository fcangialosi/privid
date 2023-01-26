import cv2
import sys
import imutils
from matplotlib import pyplot as plt
import numpy as np
import glob
from tqdm import tqdm

def get_frames(video_name):
    seen = 0
    nframes = int(sys.argv[2])
    nskip = int(sys.argv[3])
    skipped = nskip
    if not video_name:
        cap = cv2.VideoCapture(0)
        # warmup
        for i in range(5):
            cap.read()
        while True:
            ret, frame = cap.read()
            if ret:
                yield frame
            else:
                break
    elif video_name.endswith('avi') or \
        video_name.endswith('mp4'):
        cap = cv2.VideoCapture(sys.argv[1])
        while True:
            ret, frame = cap.read()
            if skipped < nskip:
                skipped += 1
                continue
            skipped = 0
            if ret and seen < nframes:
                seen += 1
                yield frame
            else:
                break
    else:
        images = glob(os.path.join(video_name, '*.jp*'))
        images = sorted(images,
                        key=lambda x: int(x.split('/')[-1].split('.')[0]))
        for img in images:
            frame = cv2.imread(img)
            yield frame


out = cv2.VideoWriter('out.mp4', cv2.VideoWriter_fourcc(*'mp4v'), 15.0, (2560, 1440))
prev_im = None
for im in tqdm(get_frames(sys.argv[1])):

    #small_im = cv2.resize(im, dsize=(0,0), fx=0.5, fy=0.5)
    #im = cv2.imread(im_name)
    if prev_im is None:
        prev_im = im
        continue

    delta = cv2.absdiff(im, prev_im)
    prev_im = im

    #kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5,5))
    #opening = cv2.morphologyEx(delta, cv2.MORPH_OPEN, kernel)

    gray = cv2.cvtColor(delta.copy(), cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5,5), 0)
    thresh = cv2.threshold(gray, 25, 255, cv2.THRESH_BINARY)[1]
    thresh = cv2.dilate(thresh, None, iterations=2)

    boxim = delta.copy()
    cnts = cv2.findContours(thresh.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = imutils.grab_contours(cnts)
    for c in cnts:
        if cv2.contourArea(c) < 50:
            continue
        (x,y,w,h) = cv2. boundingRect(c)
        cv2.rectangle(boxim, (x,y), (x+w,y+h), (0,255,0), 2)

    threshc = cv2.cvtColor(thresh, cv2.COLOR_GRAY2BGR)
    #boximc = cv2.cvtColor(boxim, cv2.COLOR_GRAY2BGR)
    row1 = cv2.hconcat([im, delta])
    row2 = cv2.hconcat([threshc, boxim])
    out_im = cv2.vconcat([row1, row2])
    small_out_im = cv2.resize(out_im, dsize=(0,0), fx=0.5, fy=0.5)

    out.write(small_out_im)
out.release()
