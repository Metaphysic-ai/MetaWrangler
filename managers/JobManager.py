import os
import subprocess
from datetime import datetime

class JobManager:
    def __init__(self, wrangler):
        self.wrangler = wrangler

    def simulateJobs(self):
        creation_time = datetime.now().strftime("%y%m%d_%H%M%S")
        script_path = "/mnt/x/PROJECTS/romulus/sequences/wro/wro_6300/comp/work/nuke/Comp-CA/wro_6300_metaSim.v001.nk"
        self.submit_job_from_path(script_path)
        profile = self.wrangler.get_job_profile(script_path)
        self.wrangler.create_task_event(id=profile.id,
                                        mem=profile.required_mem,
                                        cpus=profile.required_cpus,
                                        gpu=profile.required_gpu,
                                        batch_size=profile.batch_size,
                                        timeout=profile.timeout,
                                        creation_time=creation_time)

    def submit_job_from_path(self, script_path, gpu=False, batch_size=10, timeout=10):
        if not os.path.isdir("tmp"):
            os.mkdir("tmp")
        job_name = script_path.split(os.sep)[-1]
        stripped_job_name = job_name.replace(".nk", "").replace(".", "_")
        job_file_path = f"tmp/{stripped_job_name}_job.job"
        plugin_file_path = f"tmp/{stripped_job_name}_plugin.job"

        job_info_str = f"""
        Plugin=Nuke
        Name={job_name}
        Comment=
        Department=Comp
        Pool=comp
        SecondaryPool=
        Group=nuke
        Priority=10
        MachineLimit=0
        TaskTimeoutMinutes={timeout}
        EnableAutoTimeout=False
        ConcurrentTasks=1
        LimitConcurrentTasksToNumberOfCpus=True
        LimitGroups=
        JobDependencies=
        OnJobComplete=Nothing
        ForceReloadPlugin=False
        Frames=1001-1036
        ChunkSize={batch_size}
        Whitelist=
        OutputFilename0=/mnt/x/temp/4renderserver/out/{job_name}/{stripped_job_name}.####.exr
        ExtraInfo0=
        ExtraInfo1=
        ExtraInfo2=
        ExtraInfo3=
        ExtraInfo4=
        ExtraInfo5=
        ExtraInfo6=
        ExtraInfo7=
        ExtraInfo8=
        ExtraInfo9=
        """

        plugin_info_str = f"""
        SceneFile={script_path}
        Version=14.0
        Threads=0
        RamUse=8192
        BatchMode=True
        BatchModeIsMovie=False
        WriteNode=ShotGridWrite1
        NukeX=True
        UseGpu={gpu}
        UseSpecificGpu=False
        GpuOverride=0
        RenderMode=Use Scene Settings
        EnforceRenderOrder=False
        ContinueOnError=True
        PerformanceProfiler=False
        PerformanceProfilerDir=
        Views=
        StackSize=0
        """

        with open(job_file_path, "a+") as f:
            f.write(job_info_str.strip())

        with open(plugin_file_path, "a+") as f:
            f.write(plugin_info_str.strip())

        subprocess.run(["/opt/Thinkbox/Deadline10/bin/deadlinecommand",
                        "--SubmitJob",
                        job_file_path,
                        plugin_file_path])


