import subprocess
import time
import psutil
import asyncio

class Manager:
    def __init__(self):
        self.running_containers = []
        self.job_trigger_event = False

    def spawn_container(self, container_name):
        system_cpu_usage, system_mem_usage = self.get_system_usage()
        if system_cpu_usage < 80.0 and system_mem_usage < 80.0:
            try:
                print(f"Starting container: {container_name}")
                result = subprocess.run(
                    self.get_container_command("renderserver-meta", container_name, "2g", (0, 1), False),
                    capture_output=True, text=True, check=True
                )
                container_id = result.stdout.strip()
                self.running_containers.append(Container(container_name, container_id))
                print(f"Container {container_id} started successfully.")
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while running the container: {e}")

    def get_container_command(self, hostname, name, memory, cpuset, gpu):
        command = [
            'podman', 'run',
            '--name', name,  # No extra quotes
            '--hostname', hostname,  # No extra quotes
            '--userns=keep-id',
            '--umask=0002',
            '--net=host',
            '-e', f'CONTAINER_NAME={name}',  # No extra quotes, directly assign
            '-e', 'foundry_LICENSE=4101@100.68.207.27',  # Corrected format
            '-e', 'PATH=$PATH:/opt/Thinkbox/Deadline10/bin/',  # Corrected format
            '-e', 'LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH',  # Corrected format
            '-e', 'NUKE_PATH=$NUKE_PATH:/mnt/x/PROJECTS/software/nuke',  # Corrected format
            '-e', 'HOME=/root',  # Corrected format
            '-e', 'pixelmania_LICENSE=5053@pixelmanialic',  # Corrected format
            '-v', '/etc/group:/etc/group',
            '-v', '/etc/passwd:/etc/passwd',
            '-v', '/mnt/x:/mnt/x',
            '-v', '/mnt/data:/mnt/data',
            '-v', '/opt/Nuke/Nuke13.2v4:/opt/Nuke/Nuke13.2v4',
            '-v', '/opt/Nuke/Nuke14.0v2:/opt/Nuke/Nuke14.0v2',
            '-v', '/opt/hfs20.0.590:/opt/hfs20.0.590',
            '-v', '/etc/init.d:/etc/init.d',
            '-v', '/usr/lib:/usr/lib',
            '-v', '/usr/lib/sesi:/usr/lib/sesi',
            '-v', '/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu',
            '-v', '/usr/local:/usr/local',
            '-v', '/usr/share/fonts:/usr/share/fonts',
            '-v', '/home/sadmin:/home/sadmin',
            '-v', '/root:/root',
            '-v', '/sys:/sys',
            '--memory', memory,
            '--memory-swap', memory,
            '--cpuset-cpus', f'{cpuset[0]}-{cpuset[1]}',
            '--rm',
            '--replace',
            'localhost/deadline_runner_ubuntu', './startup.sh'
        ]
        if gpu:
            command.append('--device=nvidia.com/gpu=0')
        return command

        return command_args
    def get_system_usage(self):
        # Get CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Get memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent  # percentage of memory usage

        return cpu_usage, memory_usage

    def on_job_trigger(self):
        # Placeholder function for job triggering
        # Replace this with actual event checking logic
        time.sleep(1)  # Simulate waiting for a trigger
        self.job_trigger_event = True

    def run(self):
        while True:
            print("Service is checking for jobs...")
            if self.job_trigger_event:
                self.job_trigger_event = False
                print("Job triggered!")
                # Run the container - replace 'python:3.8-slim' with your specific container
                self.spawn_container("2g_1_0")
                time.sleep(5)  # Wait a bit before checking again

class Container:
    def __init__(self, name, id, mem="2g", cpuset=(8), gpu=False):
        self.name = name
        self.id = id
        self.mem = mem
        self.cpuset = cpuset
        self.gpu = gpu
        self.markedForShutdown = False

if __name__ == "__main__":
    mng = Manager()
    mng.on_job_trigger()
    mng.run()