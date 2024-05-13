import subprocess
import os
import ast

class NukeManager():
    def __init__(self, script_path=None):
        self.script_path = script_path
        self.tmp_dir = os.getcwd()+"/callbacks/nuke/tmp"

    def get_write_dependencies(self):
        tmp_path = os.path.join(self.tmp_dir, self.script_path.split(os.sep)[-1])
        subprocess.check_output(["cp", f"{self.script_path}", tmp_path]) ### backup script
        out = subprocess.check_output(["/opt/Nuke/Nuke14.0v2/python3",
                                       "./callbacks/nuke/get_dependency_graph.py"])
        parsed_str = out.decode("utf-8").split("\n\n")[1].strip()
        out = ast.literal_eval(parsed_str)
        out[self.script_path] = out[tmp_path]
        del out[tmp_path]
        os.remove(tmp_path) ### remove backup after its been read.
        return out