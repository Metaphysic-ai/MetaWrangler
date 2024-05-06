# https://docs.thinkboxsoftware.com/products/deadline/10.3/2_Scripting%20Reference/index.html

from deadline_api.Deadline.DeadlineConnect import DeadlineCon as Connect
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from managers.ContainerManager import ContainerManager

class TaskEvent():
    def __init__(self, id, mem, cpus, gpu, creation_time, batch_size, timeout):
        self.id = id
        self.required_mem = mem
        self.required_cpus = cpus
        self.required_gpu = gpu
        self.creation_time = creation_time
        self.batch_size = batch_size
        self.timeout = timeout

class MetaWrangler():
    def __init__(self):
        self.con = Connect(self.get_local_ip(), 8081)
        self.task_event_stack = []
        self.task_event_history = {}

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
        return [job for job in jobs if job["RenderingChunks"] or job["QueuedChunks"] ]

    def flatten_dict(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, k, sep=sep).items())
            elif isinstance(v, list):
                # if len(v) > 1:
                #     print(f"List at key '{k}' has mself.con = Connect(self.get_local_ip(), 8081)ultiple elements; only the first element is used.")
                items.append((k, v[0] if v else None))
            else:
                items.append((k, v))
        return dict(items)

    def calculate_task_duration(self, task):

        start_time = task['StartRen']
        completion_time = task['Comp']

        # Use the new parse function
        start_dt = self.parse_datetime(start_time)
        completion_dt = self.parse_datetime(completion_time)

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
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

            # If no format matches, raise an exception or return None
            raise ValueError(f"Date format for '{date_str}' is not supported")

    def is_worker_idle(self, worker, creation_time, delta_min=5):
        time_difference = datetime.now() - creation_time
        minutes_difference = time_difference.total_seconds() / 60
        x_minutes = 2
        if minutes_difference > x_minutes:
            return False

        worker_db = wrangler.get_worker_db(worker)
        if not worker_db["info"]:
            return False

        last_render_time_str = worker_db["info"]["StatDate"]
        if last_render_time_str is None:
            return False
        else:
            # Parse the last render time
            last_render_time = self.parse_datetime(last_render_time_str)

            # Get the current time with timezone aware if required
            current_time = datetime.now(timezone.utc)

            # Check if the difference is greater than 5 minutes
            difference = current_time - last_render_time
            return difference > timedelta(minutes=delta_min)

    def get_all_tasks(self):

        # return self.con.Tasks.GetJobTasks()
        # return self.con.Jobs.GetJobs()
        def add_total_frames_field(task):
            frames = task["JobFrames"]
            if len(frames.split(",")) > 1: ### Sometimes it returns the frames as a list of individual, comma-separated frames.
                task["TotalJobFrames"] = len(frames.split(","))
            elif len(frames.split("-")) < 2:
                task["TotalJobFrames"] = 1
            else:
                min_frame, max_frame = (int(frames.split("-")[0]), int(frames.split("-")[1]))
                total_frames = max_frame - min_frame
                task["TotalJobFrames"] = total_frames
            return task

        def add_resource_info(task):
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

        task_db = []
        job_call = self.con.Jobs.GetJobs()
        ### Purging a bunch of keys that should be irrelevant to job success to not clutter the model
        ### (After WIP use something like PCA to actually see which keys mostly contribute to job success.)
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

        for n, job in enumerate(job_call):
            if n%100 == 0 and n!=0:
                print(f"Parsed {n} of {len(job_call)} jobs.")
            job = self.flatten_dict(job)
            job["JobFrames"] = job["Frames"]
            try:
                task_call = self.con.Tasks.GetJobTasks(job["_id"])["Tasks"]
            except:
                print(self.con.Tasks.GetJobTasks(job["_id"]))
                print("Task call failed")
                return
            for task in task_call:
                task = self.flatten_dict(task)
                task["RenderTime"] = self.calculate_task_duration(task)

                # print(job)
                # print(task)
                combined_dict = {**job, **task}
                combined_dict = add_resource_info(combined_dict)
                combined_dict = add_total_frames_field(combined_dict)
                for discard_key in discarded_keys:
                    try:
                        del combined_dict[discard_key]
                    except:
                        if discard_key in ["SchdDays", "AWSPortalAssetFileWhiteList", "StackSize", "Views", "PerformanceProfilerDir", "PerformanceProfiler"]:
                            pass
                        else:
                            print("Couldn't find", discard_key, "in the following Task:")
                            print(combined_dict)
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
        task_event = TaskEvent(id, mem, cpus, gpu, creation_time, batch_size, timeout)
        self.task_event_stack.append(task_event)

    def run(self):
        import socket
        import subprocess
        from datetime import datetime
        hostname = socket.gethostname()

        command = "podman kill $(podman ps -q -f name=meta)"

        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        mng = ContainerManager(self)
        self.create_task_event(id=0, mem=16, cpus=16, gpu=False, batch_size=10, timeout=10, creation_time=datetime.now().strftime("%/%m%d_%H%M%S"))
        self.create_task_event(id=1, mem=2, cpus=1, gpu=False, batch_size=10, timeout=10,
                               creation_time=datetime.now().strftime("%/%m%d_%H%M%S"))
        self.create_task_event(id=2, mem=4, cpus=4, gpu=True, batch_size=10, timeout=10,
                               creation_time=datetime.now().strftime("%/%m%d_%H%M%S"))
        self.create_task_event(id=3, mem=8, cpus=2, gpu=False, batch_size=10, timeout=10,
                               creation_time=datetime.now().strftime("%/%m%d_%H%M%S"))
        self.create_task_event(id=4, mem=32, cpus=16, gpu=True, batch_size=10, timeout=10,
                               creation_time=datetime.now().strftime("%/%m%d_%H%M%S"))

        print("### DEBUG: Numbers of tasks in stack:", len(self.task_event_stack))

        while True:
            # print(self.get_running_jobs())  # Execute your periodic task

            if mng.running_containers:
                mng.kill_idle_containers()

            time.sleep(3)  # Wait for 10 seconds before the next execution and for kill move to finish

            print("Service is checking for tasks...")
            if self.task_event_stack:
                task_event = self.task_event_stack[0]
                result = mng.spawn_container(hostname=hostname,
                                            mem=task_event.required_mem,
                                            cpus=task_event.required_cpus,
                                            gpu=task_event.required_gpu,
                                            creation_time=task_event.creation_time)
                if result:
                    print("Job triggered!")
                    self.task_event_history[str(task_event.id)] = {"event": task_event}
                    self.task_event_stack.pop(0)
                else:
                    self.task_event_stack.append(self.task_event_stack.pop(0)) ### put task to the end of the stack in case one gets stuck
            print("### DEBUG: Numbers of tasks in stack:", len(self.task_event_stack))


if __name__ == "__main__":
    import sys

    wrangler = MetaWrangler()

    def run_mode():
        wrangler.run()

    def info_mode():
        print("Entering Status(Analysis mode...")
        worker_db = wrangler.get_worker_db("renderserver-meta_16_16_0")
        print(worker_db)
        print("### DEBUG: THIS ISN'T FULLY IMPLEMENTED YET ###")

    if __name__ == "__main__":
        if len(sys.argv) == 1:
            run_mode()
        elif len(sys.argv) == 2:
            if sys.argv[1] == "--run":
                run_mode()
            elif sys.argv[1] == "--info":
                info_mode()
            else:
                print("Invalid option. Usage: python MetaWrangler.py [--run | --info]")
                sys.exit(1)
        else:
            print("Invalid option. Usage: python MetaWrangler.py [--run | --info]")
            sys.exit(1)

    # wrangler.run()

# task_db = wrangler.get_all_tasks()
# date_keys = ['Date', 'DateStart', 'DateStart', 'DateComp', 'Start', 'StartRen', 'Comp']
# percent_keys = ['SnglTskPrg', 'Prog']
# factorize_keys = ['User', 'Dept', 'Version', 'WriteNode', 'RenderMode', 'Mach', 'Plug', 'JobID']
# task_df = wrangler.convert_dict_to_df(task_db, date_keys, percent_keys, factorize_keys)
# del task_db

# worker = "renderserver-4g_0"
# worker = "renderservermeta-2g_1_0"
#
# worker_db = wrangler.get_worker_db(worker)
# print(worker_db["info"])
# print(wrangler.is_worker_idle(worker, delta_min=10000))

# WorkerStat 2 -> Idle

# WorkerStat 4 -> Stalled

# TaskStat 5 -> Complete
# TaskStat 6 -> Fail
# TaskStat 3 -> Suspended