import subprocess
import time
import psutil
import os
import asyncio

class Container:
    def __init__(self, name, id, mem="2g", cpuset=(8), gpu=False):
        self.name = name
        self.id = id
        self.mem = mem
        self.cpuset = cpuset
        self.gpu = gpu
        self.markedForShutdown = False

    def kill(self):
        try:
            print(f"Killing container: {self.name}")
            result = subprocess.run(f"podman stop --time 30 {self.id}", capture_output=True, text=True, check=True, shell=True)
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

            print(f"Container {self.name} killed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while killing the container: {e}")

class ContainerManager:
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.running_containers = []
        self.job_trigger_event = False
        self.MANAGER_ROOT = os.getcwd()
        print(self.MANAGER_ROOT)

    def spawn_container(self, container_name):
        worker_name_root = "renderservermeta"
        system_cpu_usage, system_mem_usage = self.get_system_usage()
        if system_cpu_usage < 80.0 and system_mem_usage < 80.0:
            try:
                print(f"Starting container: {container_name}")
                result = subprocess.run(
                    self.get_container_command(worker_name_root, container_name, "2g", (120, 121), False),
                    capture_output=True, text=True, check=True, shell=True
                )
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
                container_id = result.stdout.strip()
                worker_name = f"{worker_name_root}-{container_name}"
                self.running_containers.append(Container(worker_name, container_id))
                print(f"Container {worker_name} started successfully.")
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while running the container: {e}")

    def get_container_command(self, hostname, name, memory, cpuset, gpu):
        gpu_index = 0

        command = [
            "podman", "run",
            "--name", name,
            "--hostname", hostname,
            "--userns", "keep-id",
            "--umask", "0002",
            "--net", "host",
            "--log-level", "debug",
            "-e", f"CONTAINER_NAME={name}",
            "-e", "foundry_LICENSE=4101@100.68.207.27",
            "-e", "PATH=$PATH:/opt/Thinkbox/Deadline10/bin/",
            "-e", "LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH",
            "-e", "NUKE_PATH=$NUKE_PATH:/mnt/x/PROJECTS/software/nuke",
            "-e", "HOME=/root",
            "-e", "pixelmania_LICENSE=5053@pixelmanialic",
            "-v", "/etc/group:/etc/group",
            "-v", "/etc/passwd:/etc/passwd",
            "-v", "/mnt/x:/mnt/x",
            "-v", "/mnt/data:/mnt/data",
            "-v", "/opt/Nuke/Nuke13.2v4:/opt/Nuke/Nuke13.2v4",
            "-v", "/opt/Nuke/Nuke14.0v2:/opt/Nuke/Nuke14.0v2",
            "-v", "/opt/hfs20.0.590:/opt/hfs20.0.590",
            "-v", "/etc/init.d:/etc/init.d",
            "-v", "/usr/lib:/usr/lib",
            "-v", "/usr/lib/sesi:/usr/lib/sesi",
            "-v", "/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu",
            "-v", "/usr/local:/usr/local",
            "-v", "/usr/share/fonts:/usr/share/fonts",
            "-v", "/home/sadmin:/home/sadmin",
            "-v", "/root:/root",
            "-v", "/sys:/sys",
            "--memory", memory,
            "--memory-swap", memory,
            "--cpuset-cpus", f"{cpuset[0]}-{cpuset[1]}",
            "--rm",
            "--replace"
        ]

        if gpu:
            command += ["--device", f"nvidia.com/gpu={gpu_index}"]

        command += ["localhost/deadline_runner_ubuntu", "/home/sadmin/repos/MetaWrangler/startup.sh"]
        return command

    def get_system_usage(self):
        # Get CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Get memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent  # percentage of memory usage

        return cpu_usage, memory_usage

    def kill_idle_containers(self):
        containers_to_shutdown = []
        for container in self.running_containers:
            if self.wrangler.is_worker_idle(container.name, delta_min=1):
                container.markedForShutdown = True
                containers_to_shutdown.append(container)

        for container in containers_to_shutdown:
            container.kill()
            try:
                self.running_containers.remove(container)
            except ValueError as e:
                print("Couldn't remove Container from running Container List:", e)

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

            if self.running_containers:
                self.kill_idle_containers()

            time.sleep(5)  # Wait a bit before checking again