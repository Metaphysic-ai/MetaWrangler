# https://docs.thinkboxsoftware.com/products/deadline/10.3/2_Scripting%20Reference/index.html

from Deadline.DeadlineConnect import DeadlineCon as Connect
import time

class MetaWrangler():
    def __init__(self):
        self.con = Connect('10.175.19.98', 8081)

    def get_running_jobs(self):
        jobs = self.con.Jobs.GetJobs()
        return [job for job in jobs if job["RenderingChunks"] or job["QueuedChunks"] ]

    def run(self):
        try:
            while True:
                print(self.get_running_jobs())  # Execute your periodic task
                time.sleep(10)   # Wait for 10 seconds before the next execution
        except KeyboardInterrupt:
            print("Stopped by user.")

wrangler = MetaWrangler()
wrangler.run()