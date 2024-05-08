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

    def get_job_plug_info(self, deadline_job_info, overrideDict):

        batch = deadline_job_info['Batch'] if 'Batch' in deadline_job_info else "metawrangler_default"
        name = deadline_job_info['Name'] if 'Name' in deadline_job_info else "metawrangler_default - ShotGridWrite1"
        timecode = str(datetime.now().strftime('%y%m%d_%H%M%S'))

        allow_list = ""
        deny_list = ""

        if 'ListedSlaves' in deadline_job_info:
            if deadline_job_info['White']:
                allow_list = ",".join(deadline_job_info['ListedSlaves'])
            else:
                deny_list = ",".join(deadline_job_info['ListedSlaves'])

        submit_job_info = {
            "BatchName": timecode+"_"+batch if "metawrangler" in batch else batch,
            "ChunkSize": deadline_job_info['Chunk'] if 'Chunk' in deadline_job_info else 10,
            "Allowlist": allow_list,
            "Denylist": deny_list,
            "Department": deadline_job_info['Dept'] if 'Dept' in deadline_job_info else "TD",
            "Frames": deadline_job_info['Frames'] if 'Frames' in deadline_job_info else "1000-1010",
            "Group": deadline_job_info['Grp'] if 'Grp' in deadline_job_info else "",
            "MachineName": deadline_job_info['Mach'] if 'Mach' in deadline_job_info else "",
            "Name": timecode+"_"+name if "metawrangler" in name else name,
            "OutputDirectory0": deadline_job_info['OutDir'] if 'OutDir' in deadline_job_info else "/mnt/x/PROJECTS/pipeline/sequences/ABC/ABC_0000/comp/work/images/final/comp_output/v001/4096x2304/",
            "OutputFilename0": deadline_job_info['OutFile'] if 'OutFile' in deadline_job_info else "ABC_0000_comp_output_mtp_v001.%04d.exr",
            "Plugin": deadline_job_info['Plug'] if 'Plug' in deadline_job_info else 'Nuke',
            "Pool": deadline_job_info['Pool'] if 'Pool' in deadline_job_info else 'meta',
            "Priority": deadline_job_info['Pri'] if 'Pri' in deadline_job_info else 33,
            "UserName": deadline_job_info['User'] if 'User' in deadline_job_info else "sadmin",
            "MinRenderTimeSeconds": deadline_job_info['MinTime'] if 'MinTime' in deadline_job_info else 0,
            "TaskTimeoutSeconds": deadline_job_info['MaxTime'] if 'MaxTime' in deadline_job_info else 3000
        }

        submit_plugin_info = {
            "BatchMode": deadline_job_info['BatchMode'] if 'BatchMode' in deadline_job_info else True,
            "BatchModeIsMovie": deadline_job_info['BatchModeIsMovie'] if 'BatchModeIsMovie' in deadline_job_info else False,
            "ContinueOnError": deadline_job_info['ContinueOnError'] if 'ContinueOnError' in deadline_job_info else True,
            "EnforceRenderOrder": deadline_job_info['EnforceRenderOrder'] if 'EnforceRenderOrder' in deadline_job_info else False,
            "GpuOverride": deadline_job_info['GpuOverride'] if 'GpuOverride' in deadline_job_info else 0,
            "NukeX": deadline_job_info['NukeX'] if 'GpuOverride' in deadline_job_info else True,
            "PerformanceProfiler": deadline_job_info['PerformanceProfiler'] if 'PerformanceProfiler' in deadline_job_info else False,
            "PerformanceProfilerDir": deadline_job_info['PerformanceProfilerDir'] if 'PerformanceProfilerDir' in deadline_job_info else "",
            "RamUse": deadline_job_info['RamUse'] if 'RamUse' in deadline_job_info else 8192,
            "RenderMode": deadline_job_info['RenderMode'] if 'RenderMode' in deadline_job_info else "Use Scene Settings",
            "SceneFile": deadline_job_info['SceneFile'] if 'SceneFile' in deadline_job_info else "/mnt/x/PROJECTS/pipeline/sequences/ABC/ABC_0000/comp/work/nuke/Comp/ABC_0000_metawranglerDefault.v002.nk",
            "StackSize": deadline_job_info['StackSize'] if 'StackSize' in deadline_job_info else 0,
            "Threads": deadline_job_info['Threads'] if 'Threads' in deadline_job_info else 0,
            "UseGpu": deadline_job_info['UseGpu'] if 'UseGpu' in deadline_job_info else False,
            "UseSpecificGpu": deadline_job_info['UseSpecificGpu'] if 'UseSpecificGpu' in deadline_job_info else False,
            "Version": deadline_job_info['Version'] if 'Version' in deadline_job_info else 14.0,
            "WriteNode": deadline_job_info['WriteNode'] if 'WriteNode' in deadline_job_info else "ShotGridWrite1",
        }
        for key, value in overrideDict.items():
            # Update the input dictionary with the new value for each matching key
            if key in submit_job_info:
                submit_job_info[key] = value
            if key in submit_plugin_info:
                submit_plugin_info[key] = value
        return {"JobInfo": submit_job_info, "PluginInfo": submit_plugin_info}

class NukeUtility():
    def __init__(self):
        pass