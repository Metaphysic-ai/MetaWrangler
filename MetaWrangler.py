# https://docs.thinkboxsoftware.com/products/deadline/10.3/2_Scripting%20Reference/index.html

from Deadline.DeadlineConnect import DeadlineCon as Connect
import time
from datetime import datetime
import pandas as pd

class MetaWrangler():
    def __init__(self):
        self.con = Connect('10.175.19.98', 8081)

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
                #     print(f"List at key '{k}' has multiple elements; only the first element is used.")
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

    def get_all(self):

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

        worker_db = {}
        for worker_name in self.con.Slaves.GetSlaveNames():
            history = self.con.Slaves.GetSlaveHistoryEntries(worker_name)
            reports = self.con.Slaves.GetSlaveReports(worker_name)
            worker_db[worker_name] = {"history": history, "reports": reports}
        # print(worker_db["renderserver-4g_0"]["reports"])

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
        return worker_db, task_db

    def get_task(self, jid, tid):
        return self.con.Tasks.GetJobTask(jid, tid)

    def run(self):
        try:
            while True:
                print(self.get_running_jobs())  # Execute your periodic task
                time.sleep(10)   # Wait for 10 seconds before the next execution
        except KeyboardInterrupt:
            print("Stopped by user.")

wrangler = MetaWrangler()
worker_db, task_db = wrangler.get_all()
date_keys = ['Date', 'DateStart', 'DateStart', 'DateComp', 'Start', 'StartRen', 'Comp']
percent_keys = ['SnglTskPrg', 'Prog']
factorize_keys = ['User', 'Dept', 'Version', 'WriteNode', 'RenderMode', 'Mach', 'Plug', 'JobID']
task_df = wrangler.convert_dict_to_df(task_db, date_keys, percent_keys, factorize_keys)
print(task_db[0])
print(len(task_db))
del task_db
print(task_df)
task_df.to_pickle('./dataframes/task_df.pkl')
# print(worker_db['renderserver-32g_12']['reports'][0])
# print(worker_db['renderserver-32g_12']['history'][0])
# j_id = worker_db['renderserver-32g_12']['reports'][0]['Reps'][0]['Job']
# t_id = worker_db['renderserver-32g_12']['reports'][0]['Reps'][0]['Task']

#print(wrangler.get_task("66268da69c2f089a37f5b800", "1"))

# Stat 5 -> Complete
# Stat 6 -> Fail
# Stat 3 -> Suspended