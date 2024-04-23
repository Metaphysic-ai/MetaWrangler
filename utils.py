
class DeadlineUtility():
    def __init__(self, con):
        self.con = con

    def submit_nuke_job(self):
        # Create a new job
        job = self.con.Jobs.CreateNewJob("Nuke Dummy Job")
        job.JobType = "Normal"
        job.Plugin = "Nuke"
        job.JobPriority = 50
        job.MachineLimit = 1
        job.JobGroup = "None"
        job.Frames = "1-10"
        job.ChunkSize = 1

        # Set the path to the Nuke script (.nk file)
        nuke_script_path = "/path/to/your/script.nk"  # Update this path

        # Set plugin info
        plugin_info = {
            "SceneFile": nuke_script_path,
            "Version": "12.0",  # Specify the Nuke version
            "Threads": 0,
            "RamUse": 0,
            "NukeX": "False",  # Use NukeX features if True
        }

        # Set auxiliary files (if any)
        auxiliary_files = [
            nuke_script_path  # Add additional files if needed
        ]

        # Submit the job to Deadline
        job_info = self.con.Jobs.SubmitJob(job, plugin_info, auxiliary_files)
        if job_info.Success:
            print(f"Job submitted successfully! Job ID: {job_info.JobId}")
        else:
            print(f"Failed to submit job: {job_info.ErrorMessage}")