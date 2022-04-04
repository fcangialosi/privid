from configs import ANALYSIS_FPS, FPS

def frame_iterator():
    jump_amt = FPS/ANALYSIS_FPS
    jump_remainder = 0
    frame_idx = 0

    while frame_idx < FPS * 60 * 60:
        if frame_idx >= FPS * 60 * 60:
            break
        yield frame_idx
        jump = round(jump_amt + jump_remainder)
        jump_remainder += (jump_amt - jump)
        frame_idx += jump
