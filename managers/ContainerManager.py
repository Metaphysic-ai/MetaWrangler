import subprocess
import time
import psutil
import asyncio

class Manager():
    def __init__(self):
        self.running_containers = []
        self.job_event = asyncio.Event()
    async def spawn_container(self, container_name):
        try:
            system_cpu_usage, system_mem_usage = self.get_system_usage()
            if system_cpu_usage < 80.0 and system_mem_usage < 80.0:
                # Running a podman container and capturing the container ID
                print(f"Starting container: {container_name}")
                result = subprocess.run(self.get_container_command("renderserver-meta", container_name, "2g", (0,1), False
                ),
                                        capture_output=True, text=True, check=True)

                # Extract the container ID from the command output
                container_id = result.stdout.strip()
                new_container = Container(container_name, container_id)
                self.running_containers.append(new_container)

                print(f"Container {container_id} started successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while running the container: {e}")

    def get_container_command(self, hostname, name, memory, cpuset, gpu):
        command_args = []
        command_args.append(f'podman run')
        command_args.append(f'--name "{name}"')
        command_args.append(f'--hostname "{hostname}"')
        command_args.append(f'--userns=keep-id')
        if gpu:
            command_args.append(f'--device "nvidia.com/gpu=0"')
        command_args.append(f'--umask=0002')
        command_args.append(f'--net=host')
        command_args.append(f'-e CONTAINER_NAME="{name}"')
        command_args.append(f'-e foundry_LICENSE="4101@100.68.207.27"')
        command_args.append(f'-e PATH="$PATH:/opt/Thinkbox/Deadline10/bin/"')
        command_args.append(f'-e LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"')
        command_args.append(f'-e NUKE_PATH="$NUKE_PATH:/mnt/x/PROJECTS/software/nuke"')
        command_args.append(f'-e HOME="/root"')
        command_args.append(f'-e pixelmania_LICENSE="5053@pixelmanialic"')
        command_args.append(f'-v /etc/group:/etc/group')
        command_args.append(f'-v /etc/passwd:/etc/passwd')
        command_args.append(f'-v /mnt/x:/mnt/x')
        command_args.append(f'-v /mnt/data:/mnt/data')
        command_args.append(f'-v /opt/Nuke/Nuke13.2v4:/opt/Nuke/Nuke13.2v4')
        command_args.append(f'-v /opt/Nuke/Nuke14.0v2:/opt/Nuke/Nuke14.0v2')
        command_args.append(f'-v /opt/hfs20.0.590:/opt/hfs20.0.590')
        command_args.append(f'-v /etc/init.d:/etc/init.d')
        command_args.append(f'-v /usr/lib:/usr/lib')
        command_args.append(f'-v /usr/lib/sesi:/usr/lib/sesi')
        command_args.append(f'-v /lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu')
        command_args.append(f'-v /usr/local:/usr/local')
        command_args.append(f'-v /usr/share/fonts:/usr/share/fonts')
        command_args.append(f'-v /home/sadmin:/home/sadmin')
        command_args.append(f'-v /root:/root')
        command_args.append(f'-v /sys:/sys')
        command_args.append(f'--memory="{memory}"')
        command_args.append(f'--memory-swap "{memory}"')
        command_args.append(f'--cpuset-cpus="{cpuset[0]}-{cpuset[1]}"')
        command_args.append(f'--rm')
        command_args.append(f'--replace')
        command_args.append(f'localhost/deadline_runner_ubuntu ./startup.sh &')

        return command_args
    def get_system_usage(self):
        # Get CPU usage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Get memory usage
        memory = psutil.virtual_memory()
        memory_usage = memory.percent  # percentage of memory usage

        return cpu_usage, memory_usage

    async def on_job_trigger(self):
        # Placeholder function for job triggering
        # Replace this with actual event checking logic
        await asyncio.sleep(1)  # Simulate waiting for a trigger
        await self.job_event.set()

    async def run(self):
        while True:
            print("Service is checking for jobs...")
            await mng.job_event.wait()
            print("Job triggered!")
            # Run the container - replace 'python:3.8-slim' with your specific container
            await mng.spawn_container("python:3.8-slim")
        else:
            print("No job triggered.")
        await asyncio.sleep(5)  # Wait a bit before checking again

class Container:
    def __init__(self, name, id, mem="2g", cpuset=(8), gpu=False):
        self.name = name
        self.id = id
        self.mem = mem
        self.cpuset = cpuset
        self.gpu = gpu
        self.markedForShutdown = False

async def main():
    mng = Manager()
    await mng.run()
    await mng.on_job_trigger()

if __name__ == "__main__":
    asyncio.run(main())