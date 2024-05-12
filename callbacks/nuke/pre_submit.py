def check_cdl_entries():
    raise NotImplementedError
    ### TODO: Check if CDL nodes are looking for the correct shot_code

def check_env():
    ### TODO: Check if the environment sent matches the submission info
    raise NotImplementedError
    import os
    os.environ["OPENCV_IO_ENABLE_OPENEXR"] = "1"

def mute_viewer():
    ### TODO: Disable or delete viewer node vs "Bad Viewer" error.
    raise NotImplementedError

def replace_outdated_rawpred():
    ### TODO: If we find an old version of rawpred, replace it with a new one.
    raise NotImplementedError
