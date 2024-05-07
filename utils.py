from datetime import datetime

class DeadlineUtility():
    def __init__(self, con):
        self.con = con

    # (self, name="metawrangler_default.nk", batch_size=10, dept="TD", frame_min=1000, frame_max=1010,
    # write_node="ShotGridWrite1", output_dir="/mnt/x/temp/4renderserver/_metawrangler/images",
    # output_file_name="metawrangler_v001.####.exr", plugin="Nuke", pool="", group="", prio=33,
    # user="sadmin")

    def get_less_stupid_dictionary_keys(self, in_dict, reverse=False):
        key_mapping = {
            "BatchName": "Batch",
            "ChunkSize": 'Chunk',
            "Department": 'Dept',
            "Group": 'Grp',
            "MachineName": 'Mach',
            "OutputDirectory0": 'OutDir',
            "OutputFilename0": 'OutFile',
            "Plugin": 'Plug',
            "Pool": 'Pool',
            "Priority": 'Pri',
            "UserName": 'User',
            "MinRenderTimeSeconds": 'MinTime',
            "TaskTimeoutSeconds": 'MaxTime',
        }
        if not reverse:
            return {key_mapping.get(k, k): v for k, v in in_dict.items()}
        else:
            reverse_mapping = {v: k for k, v in key_mapping.items()}
            return {reverse_mapping.get(k, k): v for k, v in in_dict.items()}

    def get_job_plug_info(self, deadline_job_info):

        batch = deadline_job_info['Batch']
        name = deadline_job_info['Name']
        timecode = str(datetime.now().strftime('%y%m%d_%H%M%S'))

        allow_list = []
        deny_list = []

        if 'ListedSlaves' in deadline_job_info:
            if deadline_job_info['White']:
                allow_list = deadline_job_info['ListedSlaves']
            else:
                deny_list = deadline_job_info['ListedSlaves']

        submit_job_info = {
            "BatchName": timecode+"_"+batch if "metawrangler" in batch else batch,
            "ChunkSize": deadline_job_info['Chunk'],
            "Allowlist": allow_list,
            "Denylist": deny_list,
            "Department": deadline_job_info['Dept'],
            "Frames": deadline_job_info['Frames'],
            "Group": deadline_job_info['Grp'],
            "MachineName": deadline_job_info['Mach'],
            "Name": timecode+"_"+name if "metawrangler" in name else name,
            "OutputDirectory0": deadline_job_info['OutDir'],
            "OutputFilename0": deadline_job_info['OutFile'],
            "Plugin": deadline_job_info['Plug'],
            "Pool": deadline_job_info['Pool'],
            "Priority": deadline_job_info['Pri'],
            "UserName": deadline_job_info['User'],
            "MinRenderTimeSeconds": deadline_job_info['MinTime'],
            "TaskTimeoutSeconds": deadline_job_info['MaxTime']
        }

        submit_plugin_info = {
            "BatchMode": deadline_job_info['BatchMode'],
            "BatchModeIsMovie": deadline_job_info['BatchModeIsMovie'],
            "ContinueOnError": deadline_job_info['ContinueOnError'],
            "EnforceRenderOrder": deadline_job_info['EnforceRenderOrder'],
            "GpuOverride": deadline_job_info['GpuOverride'],
            "NukeX": deadline_job_info['NukeX'],
            "PerformanceProfiler": deadline_job_info['PerformanceProfiler'],
            "PerformanceProfilerDir": deadline_job_info['PerformanceProfilerDir'],
            "RamUse": deadline_job_info['RamUse'],
            "RenderMode": deadline_job_info['RenderMode'],
            "SceneFile": deadline_job_info['SceneFile'],
            "StackSize": deadline_job_info['StackSize'],
            "Threads": deadline_job_info['Threads'],
            "UseGpu": deadline_job_info['UseGpu'],
            "UseSpecificGpu": deadline_job_info['UseSpecificGpu'],
            "Version": deadline_job_info['Version'],
            "WriteNode": deadline_job_info['WriteNode']
        }
        return {"JobInfo": submit_job_info, "PluginInfo": submit_plugin_info}

class NukeUtility():
    def __init__(self):
        pass