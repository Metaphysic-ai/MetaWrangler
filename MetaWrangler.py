# https://docs.thinkboxsoftware.com/products/deadline/10.3/2_Scripting%20Reference/index.html

from deadline_api.Deadline.DeadlineConnect import DeadlineCon as Connect
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from managers.ContainerManager import ContainerManager
from managers.JobManager import JobManager
import logging
from utils import DeadlineUtility
import json
import socket

class TaskProfile():
    def __init__(self, id, info, mem, cpus, gpu, creation_time, batch_size, min_time, max_time, pcomp_flag=False):
        self.info = info ###
        self.id = id
        self.required_mem = mem
        self.required_cpus = cpus
        self.required_gpu = gpu
        self.creation_time = creation_time
        self.batch_size = batch_size
        self.min_time = min_time
        self.max_time = max_time
        self.pcomp_flag = pcomp_flag

    def mutate(self):
        ### TODO: Incrementally change the profile to adapt over time and find the right one for the job if the initial guess wasn't good enough.
        pass

class MetaWrite():
    def __init__(self, metajob, in_nodes=[], profile=None):
        self.metajob = metajob
        self.info = {}
        self.in_nodes = in_nodes
        self.profile = profile
        self.history = []
        self.holdover_frames = []
        self.active = False
        self.tasks = []
        self.assigned_workers = []
    def submit(self, override={}):
        return wrangler.con.Jobs.SubmitJobs([wrangler.deadline_utility.get_job_plug_info(self.info, override)])

class MetaJob():
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.nodes = []
        self.active_write_nodes = []

class MetaTask():
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.info = {} # 'JobId', 'TaskId', etc.
        self.assigned_workers = []
        self.history = []

    def requeue(self):
        return wrangler.con.Tasks.RequeueJobTasks(self.info['JobID'], self.info['TaskID'])

class MetaWrangler():
    def __init__(self):
        self.SUPPORTED_REQUEST_TYPES = [
            "PreCalc",
            "NewJobSubmission",
            "HandShake"
        ]
        self.con = Connect(self.get_local_ip(), 8081)
        self.task_event_stack = []
        self.active_jobs = []

        logging.basicConfig(filename='/mnt/x/temp/4renderserver/MetaWrangler3.logs',
                            format='%(asctime)s %(message)s',
                            filemode='w')
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.DEBUG)
        self.con_mng = ContainerManager(self)
        self.job_mng = JobManager(self)
        self.manual_mode = False
        self.deadline_utility = DeadlineUtility(self.con)
        self.NUM_ATTEMPTS_TO_TRY = 5

    def get_local_ip(self):
        try:
            # Create a dummy socket and connect to a well-known address (Google DNS)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
        except Exception:
            ip = "Could not determine local IP"
        return ip

    def get_running_jobs(self):
        jobs = self.con.Jobs.GetJobs()
        return [job for job in jobs if job["QueuedChunks"] or job["RenderingChunks"] ]

    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, k, sep=sep).items())
            else:
                items.append((k, v))
        return dict(items)

    def calculate_task_duration(self, task):

        if not ("StartRen" in task and "Comp" in task):
            return None

        start_time = task['StartRen']
        completion_time = task['Comp']

        # Use the new parse function
        start_dt = self.parse_datetime(start_time)
        completion_dt = self.parse_datetime(completion_time)

        if (start_dt is None or completion_dt is None):
            return None

        # Calculate the duration in seconds
        duration_seconds = (completion_dt - start_dt).total_seconds()
        return int(duration_seconds)

    def convert_dict_to_df(self, data, date_keys, percent_keys, factorize_keys):
        # Convert dictionary to DataFrame
        df = pd.DataFrame(data)

        # Parse date columns
        try:
            for key in date_keys:
                df[key] = pd.to_datetime(df[key], errors='coerce', utc=True)

            # Convert percentage fields to integers
            for key in percent_keys:
                df[key] = df[key].str.rstrip('%').astype(float)

            # Factorize specified keys
            for key in factorize_keys:
                df[key], _ = pd.factorize(df[key])
        except KeyError as e:
            print(e)
            print(df)
            print(data)

        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].apply(lambda x: int(x) if isinstance(x, str) and x.isdigit() else x)
                df[column] = df[column].apply(lambda x: True if isinstance(x, str) and x.lower() == 'true' else (
                    False if isinstance(x, str) and x.lower() == 'false' else x))
            elif df[column].dtype in ['float64', 'int64']:  # Handle float and int explicitly if needed
                df[column] = df[column].apply(lambda x: int(x) if x == int(x) else x)

        return df
    def parse_datetime(self, date_str):
        # Define a list of datetime formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",  # Full microsecond precision with timezone
            "%Y-%m-%dT%H:%M:%S%z",  # No fractional seconds with timezone
            "%Y-%m-%dT%H:%M:%S.%f",  # Full microsecond precision without timezone
            "%Y-%m-%dT%H:%M:%S"  # No fractional seconds and no timezone
        ]

        # Attempt to parse the datetime string using each format
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except ValueError:
                return None

    def is_worker_idle(self, worker, delta_min=5):
        time_in_idle = datetime.strptime(worker.creation_time, "%y%m%d_%H%M%S")
        time_difference = datetime.now() - time_in_idle
        seconds_difference = time_difference.total_seconds()
        minutes_difference = seconds_difference / 60

        worker_report = wrangler.get_worker_report(worker.name)
        last_render_time_str = None
        self.logger.debug(str(worker.name))
        self.logger.debug(worker_report)
        self.logger.debug(f"######## MINUTES DIFFERENCE: {minutes_difference} with delta {delta_min}")
        if minutes_difference < delta_min:
            return False

        if worker_report["info"]:
            if len(worker_report["info"]):
                last_render_time_str = worker_report["info"]["StatDate"]

        if last_render_time_str is None:
            return False

        last_render_time = self.parse_datetime(last_render_time_str)

        current_time = datetime.now(timezone.utc)

        difference = current_time - last_render_time
        return difference > timedelta(minutes=delta_min)

    def get_discarded_keys(self):
        ### TODO: Replace manual dredgework with stuff founded on data.

        discarded_keys = ["Region", "Cmmt", "Grp", "Pool", "SecPool", "ReqAss", "ScrDep",
                          "AuxSync", "Int", "IntPer", "RemTmT", "Seq", "Reload", "NoEvnt",
                          "OnComp", "Protect", "PathMap", "AutoTime", "TimeScrpt",
                          "StartTime", "InitializePluginTime", "Dep", "DepFrame", "DepFrameStart",
                          "DepFrameEnd", "DepComp", "DepDel", "DepFail", "DepPer", "NoBad",
                          "OverAutoClean", "OverClean", "OverCleanDays", "OverCleanType",
                          "JobFailOvr", "TskFailOvr", "SndWarn", "NotOvr", "SndEmail",
                          "SndPopup", "NotEmail", "NotNote", "White", "MachLmtProg", "PrJobScrp",
                          "PoJobScrp", "PrTskScrp", "PoTskScrp", "Schd", "SchdDays", "SchdDays",
                          "SchdDate", "SchdStop", "MonStart", "MonStop", "TueStart", "TueStop",
                          "WedStart", "WedStop", "ThuStart", "ThuStop", "FriStart", "FriStop",
                          "SatStart", "SatStop", "SunStart", "SunStop", "PerformanceProfiler",
                          "PerformanceProfilerDir", "Views", "StackSize", "PlugDir", "EventDir",
                          "EventOI", "AWSPortalAssets", "AWSPortalAssetFileWhiteList",
                          "OvrTaskEINames", "IsSub", "Purged", "TileFile", "Main", "MainStart",
                          "MainEnd", "Tile", "TileFrame", "TileCount", "TileX", "TileY", "Aux",
                          "Bad", "DataSize", "ConcurrencyToken", "ExtraElements", "WtgStrt",
                          "NormMult", "Frames"]
        return discarded_keys

    def add_total_frames_field(self, task):
        frames = task["JobFrames"]
        if len(frames.split(
                ",")) > 1:  ### Sometimes it returns the frames as a list of individual, comma-separated frames.
            task["TotalJobFrames"] = len(frames.split(","))
        elif len(frames.split("-")) < 2:
            task["TotalJobFrames"] = 1
        else:
            min_frame, max_frame = (int(frames.split("-")[0]), int(frames.split("-")[1]))
            total_frames = max_frame - min_frame
            task["TotalJobFrames"] = total_frames
        return task

    def combine_job_task_dict(self, deadline_job):
        job = self.flatten_dict(deadline_job)
        job["JobFrames"] = job["Frames"]

        try:
            task_call = self.con.Tasks.GetJobTasks(job["_id"])["Tasks"]
        except:
            print(self.con.Tasks.GetJobTasks(job["_id"]))
            print("Task call failed")
            return

        task_dicts = []

        for task in task_call:
            print(task)
            task = self.flatten_dict(task)
            task["RenderTime"] = self.calculate_task_duration(task)

            # print(job)
            # print(task)
            combined_dict = {**job, **task}
            combined_dict = self.add_resource_info(combined_dict)
            combined_dict = self.add_total_frames_field(combined_dict)

            for discard_key in self.get_discarded_keys():
                try:
                    del combined_dict[discard_key]
                except:
                    if discard_key in ["SchdDays", "AWSPortalAssetFileWhiteList", "StackSize", "Views",
                                       "PerformanceProfilerDir", "PerformanceProfiler"]: ### These are only sporadically present in the data and seem to be of dubious use.
                                                                                         ### Same as other discarded keys TODO: to run the numbers if the keys are actually irrelevant.
                        pass
                    else:
                        print("Couldn't find", discard_key, "in the following Task:")
                        print(combined_dict)
                task_dicts.append(task)
            return task_dicts

    def add_resource_info(self, task):
        import re
        # Extract the relevant information from the worker name
        worker_name = task.get("worker_name", "")

        # Initialize default values
        task["MemoryAssigned"] = None
        task["CoresAssigned"] = None
        task["GPUAssigned"] = None

        # Regex to parse the worker name pattern
        # This pattern checks for the presence of 'gpu', captures the memory size, and optionally captures two more numbers
        regex_pattern = r'renderserver-(gpu_)?(\d+)g(_(\d+))?(_(\d+))?'
        match = re.search(regex_pattern, worker_name)

        if match:
            # Check for GPU presence
            gpu = match.group(1) is not None
            task["GPUAssigned"] = True if gpu else None

            # Memory assigned
            mem = int(match.group(2)) if match.group(2) else None
            task["MemoryAssigned"] = mem

            # Cores assigned - check if the correct groups are captured for cores
            cores = None
            if match.group(4) and match.group(6):
                # Third group is cores if three numbers are present and the first is GPU
                cores = int(match.group(6))
            elif match.group(4) and gpu:
                # Second number is cores when two numbers are present with GPU
                cores = int(match.group(4))
            task["CoresAssigned"] = cores
        return task

    def get_metajob_from_deadline_job(self, deadline_job):
        metajob = MetaJob(self)
        metawrite = MetaWrite(metajob)
        metawrite.profile = self.get_job_profile(deadline_job["Props"]["PlugInfo"]["SceneFile"])
        metajob.active_write_nodes.append(metawrite)
        for task_info in self.con.Tasks.GetJobTasks(deadline_job["_id"])["Tasks"]:
            metatask = MetaTask(self)
            metatask.info = self.deadline_utility.get_less_stupid_dictionary_keys(task_info)
            metawrite.tasks.append(metatask)

        job_info = self.flatten_dict(self.con.Jobs.GetJob(deadline_job["_id"]))
        metawrite.info = self.deadline_utility.get_less_stupid_dictionary_keys(job_info)
        return metajob

    def get_all_tasks(self):

        task_db = []
        job_call = self.con.Jobs.GetJobs()

        for n, job in enumerate(job_call):
            if n%100 == 0 and n!=0:
                print(f"Parsed {n} of {len(job_call)} jobs.")
            combined_dict = self.combine_job_task_dict(job)

            task_db.append(combined_dict)
            # print(combined_dict)
        return task_db

    def get_worker_report(self, worker_name):
        info = self.con.Slaves.GetSlaveInfo(worker_name)
        history = self.con.Slaves.GetSlaveHistoryEntries(worker_name)
        reports = self.con.Slaves.GetSlaveReports(worker_name)
        return {"info": info, "history": history, "reports": reports}

    def get_task(self, jid, tid):
        return self.con.Tasks.GetJobTask(jid, tid)

    def get_job_profile(self, script_path, node):
        server_ip = '10.175.19.128'  # outbound IP of renderserver
        server_port = 12123

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        args = {}

        try:
            request = {"Type": "GetProfile", "Payload": {"script_path": script_path, "write_node": node}}
            message = json.dumps(request)
            client_socket.connect((server_ip, server_port))
            client_socket.sendall(message.encode('utf-8'))
            print("Sending script path and write not to get target profile.")

            response = client_socket.recv(1024).decode('utf-8')
            args = json.loads(response)

        except socket.error as e:
            print(f"Socket error occurred: {e}")

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        finally:
            client_socket.close()

        profile = TaskProfile(**args)
        return profile

    def assign_containers_to_job(self, metajob):
        ### TODO: This is the legacy version, actually make it do the thing.
        pass
        # containers_to_assign = []
        # for container in self.con_mng.running_containers:
        #     if metajob.profile.required_mem == container.mem \
        #             and metajob.profile.required_cpus == container.cpus \
        #             and metajob.profile.required_gpu == container.gpu:
        #         containers_to_assign.append(container.name)
        #
        # self.logger.debug("X#X#X#X#DEBUG: Assigning containers: "+str(
        #     self.con.Jobs.AddSlavesToJobMachineLimitList(metajob.info["_id"], containers_to_assign)))

    def initialize_metajob(self, script_path, write_nodes):
        ### TODO: Initialize with profiler instead.
        metajob = MetaJob(self)
        for node in write_nodes:
            metawrite = MetaWrite(metajob)
            metawrite.profile = self.get_job_profile(script_path, node)
            metajob.active_write_nodes.append(metawrite)
        self.create_backup(script_path)
        for write_node in metajob.active_write_nodes:
            if write_node.profile.pcomp_flag:
                self.auto_pcomp(metajob,
                                write_node)  ### Apply automatic pcomps to script if profiler flags it to do so.
            write_node.submit()
            if not write_node.active:  ### If not already active, set to active and add to active job list.
                self.active_jobs.append(write_node)
                write_node.active = True

    def create_backup(self, nuke_script):
        ### TODO: create a backup of this nuke_script
        pass

    def auto_pcomp(self, metajob, write_node):
        ### TODO: Add pcomp nodes, add Write nodes to metajob.active_write_nodes, set to StrictRenderSequence, save script as reference.
        ### - Check BBOX on blurs and transforms
        ### - Concatenate successive Transforms into one.
        ### - Suggest pcomp/auto-update if something changed.
        pass

    def wrangler_heuristics(self):
        ### TODO: Apply heuristics to each task (Requeue on timeout,  etc.)
        pass

    def check_jobs_status(self):
        ### TODO: Check all active jobs for their status
        finished_jobs = []
        failed_jobs = []
        return finished_jobs, failed_jobs

    def run_output_check(self, job):
        ### TODO: do quick sanity check of the outputs (are all frames present in the outdir, maybe random sampling if frames are black.)
        ### TODO: Mark as failed and throw back to active jobs if it didn't pass.
        passed = True
        return passed

    def add_to_next_db_update(self, job, success):
        ### TODO: Add the successful jobs to the vector database of successful profiles to do similarity checks on.
        ### TODO: Also add them to the perpetual db (Throw them into the Ocean, my dude.).
        pass

    def failure_analysis(self, job):
        ### TODO: Do a more in-depth analysis of the failed job to try and find what might have been going on.
        ### (This most likely has to move out of the main run() loop due to performance and instead update every X ticks.)
        ### e.g. do a script diff with a lighthouse profile
        analysis = {}
        return analysis

    def send_full_failure_notification(self, job, analysis):
        ### TODO: Once there is a slackbot, notify a TD about this issue.
        pass

    def precalc_script(self, payload):
        ### TODO: Call ocean database to calculate and add new path to be ready for later
        print(payload)
        pass

    def manage_containers(self, hostname):
        ### 1. apply gaussian distribution based on prio.
        ### 2. Spawn containers big to small until no more fit.
        ### 3. Assign all active jobs all workers that fit their profile.
        for metajob in self.active_jobs:
            for write_node in metajob.active_write_nodes:
                task_event = write_node.profile ### Make sure render order is enforced
                result = self.con_mng.spawn_container(hostname=hostname,
                                                      mem=task_event.required_mem,
                                                      id=task_event.id,
                                                      cpus=task_event.required_cpus,
                                                      gpu=task_event.required_gpu,
                                                      creation_time=task_event.creation_time)
                if result:
                    print("Job triggered!")
                    write_node.history.append(write_node.profile) ### Add current profile to history.

    def handle_client(self, client_socket):
        ### Currently, the supported request types are:
        ### PreCalc, NewJobSubmission
        request = client_socket.recv(1024).decode('utf-8').strip()
        print(f"Received: {request}")
        request = json.loads(request)
        if request['Type'] in self.SUPPORTED_REQUEST_TYPES:
            response = f"MetaWrangler received {request['Type']} request successfully!"
        else:
            response = f"A request of the type {request['Type']} is not supported."

        client_socket.send(response.encode('utf-8'))
        client_socket.close()

        if request.get("Type") == "HandShake":
            subprocess.Popen(["/opt/Nuke/Nuke14.0v2/Nuke14.0", "-t", request["Payload"]])

        if request.get("Type") == "PreCalc":
            ### When a user opens a nuke script, we precalculate the profile (and spin up a worker?) if there is room.
            print(request["Payload"])
            # self.precalc_script(request["Payload"])

        if request.get("Type") == "NewJobSubmission":
            submitted_nuke_script, write_nodes = request["Payload"]
            self.initialize_metajob(submitted_nuke_script, write_nodes)

        return request

    def run(self, sandbox=False):
        import subprocess
        from datetime import datetime
        print("Starting MetaWrangler Service...")

        hostname = socket.gethostname()
        host = '0.0.0.0'
        port = 12121 if not sandbox else 12120 ### Set to different port to stop live submits to enter MetaWrangler when debugging

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(20)
        server_socket.setblocking(False)

        os.environ["METAWRANGLER_PATH"] = os.path.dirname(__file__)
        os.environ["METAWRANGLER_CALLBACKS"] = os.path.dirname(__file__) + "/callbacks/nuke"

        sandbox_flag_str = " | (Sandbox Mode)" if sandbox else ""
        print(f"MetaWrangler Service is listening on {self.get_local_ip()}:{port}{sandbox_flag_str}")

        command = "podman kill $(podman ps -q -f name=meta)"

        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        tick_times = []
        while True:
            loop_start = time.time() ### DEBUG: Analyse main loop performance, should probably stay <1-2s
            ### Clean up old, idle workers
            if self.con_mng.running_containers:
                self.con_mng.kill_idle_containers()

            try:
                client_socket, client_address = server_socket.accept()
                print(f"Connection from {client_address} has been established!")
                self.handle_client(client_socket)
            except BlockingIOError:
                pass

            finished_jobs, failed_jobs = self.check_jobs_status()
            for finished_job in finished_jobs:
                passed = self.run_output_check(finished_job)
                if passed:
                    self.add_to_next_db_update(finished_job, success=True)
                    self.active_jobs.remove(finished_job)

            for failed_job in failed_jobs:
                if len(failed_job.history) > self.NUM_ATTEMPTS_TO_TRY: ### Stop incrementing on the profile after X attempts.
                    analysis = self.failure_analysis(failed_job)
                    self.send_full_failure_notification(failed_job, analysis)
                    self.add_to_next_db_update(failed_job, success=False)
                    self.active_jobs.remove(failed_job)
                failed_job.profile.mutate() ### Try to get closer to correct profile if initial one failed.

            self.manage_containers(hostname)

            self.wrangler_heuristics()

            tick_times.append(time.time() - loop_start) ### Do an average over how long we take per loop
            if len(tick_times) > 100:
                # print("### DEBUG: Estimated time spent per loop:", sum(tick_times) / len(tick_times), "Seconds.")
                tick_times = []


if __name__ == "__main__":
    import sys
    import os
    import subprocess

    wrangler = MetaWrangler()

    def run_mode(sandbox=False):
        wrangler.run(sandbox)

    def debug_mode():
        ### This is where I manually check functions
        # metajob = wrangler.get_metajob_from_deadline_job(wrangler.con.Jobs.GetJob("6639a543ee72d7dfc75d8178"))
        # metajob = MetaJob(wrangler)
        # print(metajob.submit(override={"Name": "OverrideTest", "ChunkSize": 11}))
        print(wrangler.get_worker_report("renderserver-4g_0")["info"])

    if len(sys.argv) == 1:
        run_mode()
    elif len(sys.argv) == 2:
        if sys.argv[1] == "--run":
            run_mode()
        elif sys.argv[1] == "--run_sandbox":
            run_mode(sandbox=True)
        elif sys.argv[1] == "--debug":
            debug_mode()
        else:
            print("Invalid option. Usage: python MetaWrangler.py [--run | --run_sandbox | --debug ]")
            sys.exit(1)
    else:
        print("Invalid option. Usage: python MetaWrangler.py [--run | --run_sandbox | --debug ]")
        sys.exit(1)


# WorkerStat 2 -> Idle

# WorkerStat 4 -> Stalled

# TaskStat 5 -> Complete
# TaskStat 6 -> Fail
# TaskStat 3 -> Suspended