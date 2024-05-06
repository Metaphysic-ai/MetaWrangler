import subprocess
import time
import psutil
import os
import asyncio
import GPUtil
from datetime import datetime

class Container:
    def __init__(self, name, suffix, id, mem="2g", cpuset=(8, 9), gpu=False, gpu_index=None, creation_time=None):
        self.name = name
        self.suffix = suffix
        self.id = id
        self.mem = mem
        self.cpuset = cpuset
        self.gpu = gpu if gpu_index is None else True
        self.gpu_index = gpu_index
        self.markedForShutdown = False
        self.creation_time = creation_time

    def __repr__(self):
        return f'Container(\'{self.name}\', {self.id})'

class ContainerManager:
    def __init__(self, wrangler):
        self.wrangler = wrangler
        self.running_containers = []
        self.job_trigger_event = False
        self.MANAGER_ROOT = os.getcwd()
        self.occupied_cpus = [True if cpu_index<8 else False for cpu_index in range(os.cpu_count())] # Occupy the first 8 cores for system stuff.
        self.occupied_gpus = [False for gpu_index in GPUtil.getAvailable(limit=4)]
        self.spawn_index = 0
        self.containers_spawned = []
        self.wrangler.logger.debug("[FOUND GPUS ON INIT]")
        self.wrangler.logger.debug(GPUtil.getAvailable(limit=4))
        self.wrangler.logger.debug(self.occupied_gpus)

    def spawn_container(self, hostname, id=None, mem=2, cpus=1, gpu=False, creation_time=None):
        self.wrangler.logger.debug(f"{hostname}, {mem}, {cpus}, {gpu}, {creation_time}")
        gpu_suffix = "gpu_" if gpu else ""
        mem_suffix = str(mem)
        cpus_suffix = f"_{cpus}"
        id = self.spawn_index if id is None else id
        index = f"_{id}_{creation_time}"
        self.spawn_index += 1
        container_name = "meta_"+gpu_suffix+mem_suffix+cpus_suffix+index
        worker_name = hostname + "-" + container_name
        gpu_index = None

        cpuset = self.assign_cpus(cpus)
        if cpuset is None:
            print(f"Not enough cores left on the system for worker {worker_name}. Skipping.")
            return False

        if gpu:
            gpu_index = self.assign_gpu()
            if gpu_index is None:
                print(f"Not enough gpus left on the system for worker {worker_name}. Skipping. ")
                return False

        system_cpu_usage, system_mem_usage = self.get_system_usage()
        if system_cpu_usage < 70.0 and system_mem_usage < 70.0:
            print(f"Starting container: {worker_name}")
            subprocess.Popen(
                self.get_container_command(hostname, container_name, f"{mem}g", cpuset, gpu, gpu_index), stdout=subprocess.DEVNULL, shell=True
            )
            # print("STDOUT:", result.stdout)
            # print("STDERR:", result.stderr)

            worker_name = f"{hostname}-{container_name}"
            self.running_containers.append(Container(worker_name, container_name, 0, creation_time=creation_time, gpu_index=gpu_index))
            self.containers_spawned.append(container_name)
            print(f"Container {worker_name} started successfully.")
            return True
            # except subprocess.SubprocessError as e:
            #     print(f"An error occurred while running the container: {e}")
            #     return False
        else:
            print("The server is currently at high load. Skipping worker creation")
            return False

    def kill_container(self, container):
        print(f"Killing container: {container.name}")
        # the same profile and the same id is created, it would kill it on spawn.
        subprocess.Popen(f"podman stop --timeout 0 {container.suffix} || true", shell=True)
        self.free_up_cpus(container.cpuset)
        if container.gpu_index is not None:
            self.free_up_gpu(container.gpu_index)

            print(f"Container {container.name} killed successfully.")

    def free_up_cpus(self, cpuset):
        for cpu_index in cpuset:
            self.occupied_cpus[cpu_index] = False
        self.wrangler.logger.debug("[Freeing up cpus]: Occupied:")
        self.wrangler.logger.debug(self.occupied_cpus.count(True))
        self.wrangler.logger.debug("Free:")
        self.wrangler.logger.debug(self.occupied_cpus.count(False))

    def assign_cpus(self, cpus):
        if self.occupied_cpus.count(False) < cpus: ### Check if there are enough free cores available
            return None
        else:
            cpu_counter = cpus
            cores_assigned = []
            for core_index, core_status in enumerate(self.occupied_cpus):
                if core_status == False and cpu_counter>0: ### First entry that isn't occupied while there are still cores left to assign
                    self.occupied_cpus[core_index] = True ### Set the cpu index to occupied
                    cores_assigned.append(core_index)
                    cpu_counter -= 1
            self.wrangler.logger.debug("[Assigning cpus]: Occupied:")
            self.wrangler.logger.debug(self.occupied_cpus.count(True))
            self.wrangler.logger.debug(self.occupied_cpus.count(False))
            return tuple(cores_assigned)

    def free_up_gpu(self, gpu_index):
        self.occupied_gpus[gpu_index] = False
        self.wrangler.logger.debug("[Freeing up gpu]: Occupied:")
        self.wrangler.logger.debug(str(self.occupied_gpus.count(True)))
        self.wrangler.logger.debug(self.occupied_gpus.count(False))

    def assign_gpu(self):
        if self.occupied_gpus.count(False) < 1:  ### Check if there are enough free gpus available
            return None
        else:
            for gpu_index, gpu_status in enumerate(self.occupied_gpus):
                if gpu_status == False:
                    self.occupied_gpus[gpu_index] = True
                    self.wrangler.logger.debug("[Assigning gpu]: Occupied:")
                    self.wrangler.logger.debug(self.occupied_gpus.count(True))
                    self.wrangler.logger.debug(self.occupied_gpus.count(False))
                    return gpu_index

    def get_container_command(self, hostname, name, memory, cpuset, gpu, gpu_index):

        if len(cpuset) == 1:
            cpuset = str(cpuset)[1:-2].replace(" ", "")
        else:
            cpuset = str(cpuset)[1:-1].replace(" ", "")

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
            "--cpuset-cpus", str(cpuset),
            "--rm",
            "--replace"
        ]

        if gpu:
            command.extend(["--device", f"nvidia.com/gpu={gpu_index}"])
            self.wrangler.logger.debug(f"Calling gpu device: nvidia.com/gpu={gpu_index}")

        command.extend(["localhost/deadline_runner_ubuntu", "/home/sadmin/repos/MetaWrangler/managers/startup.sh"])
        command = " ".join(command)
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
            if self.wrangler.is_worker_idle(container.name, creation_time=container.creation_time, delta_min=1):
                container.markedForShutdown = True
                containers_to_shutdown.append(container)
        self.wrangler.logger.debug("Identified idle workers due for shutdown:")
        self.wrangler.logger.debug(containers_to_shutdown)

        for container in containers_to_shutdown:
            self.kill_container(container)
            self.running_containers.remove(container)


if __name__=="__main__":
    pass