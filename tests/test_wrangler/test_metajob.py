import pytest
from MetaWrangler import MetaWrangler
from MetaWrangler import MetaJob

# Example-based tests for calculate_task_duration method
def test_metajob_init():
    wrangler = MetaWrangler()
    metajob = wrangler.get_metajob_from_deadline_job(wrangler.con.Jobs.GetJob("6639a543ee72d7dfc75d8178"))
    assert metajob.info["SceneFile"] == "/mnt/x/PROJECTS/romulus/sequences/wro/wro_1860/comp/work/nuke/Comp-WIP/wro_1860_debug.v003.nk"