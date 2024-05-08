# https://docs.thinkboxsoftware.com/products/deadline/10.3/2_Scripting%20Reference/index.html

from deadline_api.Deadline.DeadlineConnect import DeadlineCon as Connect
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from managers.ContainerManager import ContainerManager
from managers.JobManager import JobManager
import logging
from utils import DeadlineUtility

class TaskProfile():
    def __init__(self, id, mem, cpus, gpu, creation_time, batch_size, timeout):
        self.id = id
        self.required_mem = mem
        self.required_cpus = cpus
        self.required_gpu = gpu
        self.creation_time = creation_time
        self.batch_size = batch_size
        self.timeout = timeout

class MetaJob():
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.info = {}
        self.profile = None
        self.assigned_workers = []
        self.history = []
        self.tasks = []
        self.nodes = []

    def submit(self, override={}):
        return wrangler.con.Jobs.SubmitJobs([wrangler.deadline_utility.get_job_plug_info(self.info, override)])

class MetaTask():
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.info = {} # 'JobId', 'TaskId', etc.
        self.profile = None
        self.assigned_workers = []
        self.history = []

    def requeue(self):
        return wrangler.con.Tasks.RequeueJobTasks(self.info['JobID'], self.info['TaskID'])

class MetaWrangler():
    def __init__(self):
        self.con = Connect(self.get_local_ip(), 8081)
        self.task_event_stack = []
        self.task_event_history = {}

        logging.basicConfig(filename='/mnt/x/temp/4renderserver/MetaWrangler3.logs',
                            format='%(asctime)s %(message)s',
                            filemode='w')
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.DEBUG)
        self.con_mng = ContainerManager(self)
        self.job_mng = JobManager(self)
        self.manual_mode = False
        self.deadline_utility = DeadlineUtility(self.con)

    def get_local_ip(self):
        import socket
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

        worker_db = wrangler.get_worker_db(worker.name)
        last_render_time_str = None
        self.logger.debug(str(worker.name))
        self.logger.debug(worker_db)
        self.logger.debug(f"######## MINUTES DIFFERENCE: {minutes_difference} with delta {delta_min}")
        if minutes_difference < delta_min:
            return False

        if worker_db["info"]:
            if len(worker_db["info"]):
                last_render_time_str = worker_db["info"]["StatDate"]

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
        metajob.profile = self.get_job_profile(deadline_job["Props"]["PlugInfo"]["SceneFile"])
        for task_info in self.con.Tasks.GetJobTasks(deadline_job["_id"])["Tasks"]:
            metatask = MetaTask(self)
            metatask.info = self.deadline_utility.get_less_stupid_dictionary_keys(task_info)
            metatask.profile = metajob.profile ### For now, deadline doesn't allow more granular than job level.
            metajob.tasks.append(metatask)

        job_info = self.flatten_dict(self.con.Jobs.GetJob(deadline_job["_id"]))
        metajob.info = self.deadline_utility.get_less_stupid_dictionary_keys(job_info)
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

    def get_worker_db(self, worker_name):
        info = self.con.Slaves.GetSlaveInfo(worker_name)
        history = self.con.Slaves.GetSlaveHistoryEntries(worker_name)
        reports = self.con.Slaves.GetSlaveReports(worker_name)
        return {"info": info, "history": history, "reports": reports}

    def get_task(self, jid, tid):
        return self.con.Tasks.GetJobTask(jid, tid)

    def create_task_event(self, id, mem, cpus, gpu, creation_time, batch_size, timeout):
        task_event = TaskProfile(id, mem, cpus, gpu, creation_time, batch_size, timeout)
        self.task_event_stack.append(task_event)

    def get_job_profile(self, job):
        ### TODO: PLACEHOLDER
        profile = TaskProfile(id=0, mem=4, cpus=2, gpu=False,
        batch_size=10, timeout=10, creation_time=str(datetime.now().strftime('%y%m%d_%H%M%S')))
        return profile

    def assign_containers_to_job(self, metajob):
        containers_to_assign = []
        for container in self.con_mng.running_containers:
            if metajob.profile.required_mem == container.mem \
                    and metajob.profile.required_cpus == container.cpus \
                    and metajob.profile.required_gpu == container.gpu:
                containers_to_assign.append(container.name)

        self.logger.debug("X#X#X#X#DEBUG: Assigning containers: "+str(
            self.con.Jobs.AddSlavesToJobMachineLimitList(metajob.info["_id"], containers_to_assign)))

    def handle_client(self, client_socket):
        request = client_socket.recv(1024).decode('utf-8').strip()
        print(f"Received: {request}")

        response = f"MetaWrangler received {request['Type']} request successfully!"
        client_socket.send(response.encode('utf-8'))

        client_socket.close()

    def run(self, sandbox=False):
        import socket
        import subprocess
        from datetime import datetime
        print("Starting MetaWrangler Service...")

        hostname = socket.gethostname()
        host = '0.0.0.0'
        port = 12121 if not sandbox else 12120 ### Set to different port to stop live submits to enter MetaWrangler

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(20)
        server_socket.setblocking(False)

        sandbox_flag_str = " | (Sandbox Mode)" if sandbox else ""
        print(f"MetaWrangler Service is listening on {self.get_local_ip()}:{port}{sandbox_flag_str}")

        command = "podman kill $(podman ps -q -f name=meta)"

        result = subprocess.run(command, shell=True, capture_output=True, text=True)


        while True:
            try:
                client_socket, client_address = server_socket.accept()
                print(f"Connection from {client_address} has been established!")
                self.handle_client(client_socket)
            except BlockingIOError:
                pass

            # self.logger.debug(f"Numbers of tasks in stack:{len(self.task_event_stack)}")

            if not self.manual_mode:

                if self.con_mng.running_containers:
                    self.con_mng.kill_idle_containers()

                if self.task_event_history:
                    for k in self.task_event_history.keys():
                        self.assign_containers_to_job(self.task_event_history[k]["job"])

            # time.sleep(3)  # Wait for 10 seconds before the next execution and for kill move to finish
            # print("Service is checking for tasks...")
            if self.task_event_stack:
                metajob = self.task_event_stack[0]
                task_event = metajob.profile
                result = self.con_mng.spawn_container(hostname=hostname,
                                            mem=task_event.required_mem,
                                            id=task_event.id,
                                            cpus=task_event.required_cpus,
                                            gpu=task_event.required_gpu,
                                            creation_time=task_event.creation_time)

                self.logger.debug(f"RESULT OF SPAWNCONTAINER: {result}")
                if result:
                    print("Job triggered!")
                    self.task_event_history[str(task_event.id)] = {"profile": task_event, "job":metajob}
                    self.task_event_stack.pop(0)
                else:
                    self.task_event_stack.append(self.task_event_stack.pop(0)) ### put task to the end of the stack in case one gets stuck


if __name__ == "__main__":
    import sys
    import os
    import subprocess

    wrangler = MetaWrangler()

    def run_mode(sandbox=False):
        wrangler.run(sandbox)

    def debug_mode():
        # metajob = wrangler.get_metajob_from_deadline_job(wrangler.con.Jobs.GetJob("6639a543ee72d7dfc75d8178"))
        metajob = MetaJob(wrangler)
        print(metajob.submit(override={"Name": "OverrideTest", "ChunkSize": 11}))

    def manual_mode():
        wrangler.manual_mode = True
        for n in range(5):
            gpu = True if n < 3 else False
            metajob = MetaJob({})
            profile = TaskProfile(id=n, mem=32, cpus=16, gpu=gpu, creation_time=str(datetime.now().strftime('%y%m%d_%H%M%S')), batch_size=10, timeout=10)
            metajob.profile = profile
            wrangler.task_event_stack.append(metajob)
        wrangler.run()

    if len(sys.argv) == 1:
        run_mode()
    elif len(sys.argv) == 2:
        if sys.argv[1] == "--run":
            run_mode()
        elif sys.argv[1] == "--run_sandbox":
            run_mode(sandbox=True)
        elif sys.argv[1] == "--debug":
            debug_mode()
        elif sys.argv[1] == "--manual":
            manual_mode()
        else:
            print("Invalid option. Usage: python MetaWrangler.py [--run | --run_sandbox | --debug |--manual ]")
            sys.exit(1)
    else:
        print("Invalid option. Usage: python MetaWrangler.py [--run | --run_sandbox | --debug |--manual ]")
        sys.exit(1)


# WorkerStat 2 -> Idle

# WorkerStat 4 -> Stalled

# TaskStat 5 -> Complete
# TaskStat 6 -> Fail
# TaskStat 3 -> Suspended