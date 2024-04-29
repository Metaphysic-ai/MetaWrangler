import subprocess
import time
import psutil

class Manager():
    def __init__(self):
        self.running_containers = []

    def spawn_container(self, container_name):
        try:
            system_cpu_usage, system_mem_usage = self.get_system_usage()
            if system_cpu_usage < 80.0 and system_mem_usage < 80.0:
                # Running a podman container and capturing the container ID
                print(f"Starting container: {container_name}")
                result = subprocess.run(["podman", "run", "-d", container_name], capture_output=True, text=True, check=True)

                # Extract the container ID from the command output
                container_id = result.stdout.strip()
                new_container = Container(container_name, container_id)
                self.running_containers.append(new_container)

                print(f"Container {container_id} started successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while running the container: {e}")

    def get_container_command(self):


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
        time.sleep(10)  # Simulate waiting for a trigger
        return True

class Container:
    def __init__(self, name, id, mem="4g", cpuset=[8], gpu=False):
        self.name = name
        self.id = id
        self.mem = mem
        self.cpuset = cpuset
        self.gpu = gpu
        self.markedForShutdown = False

if __name__ == "__main__":
    mng = Manager()
    while True:
        print("Service is checking for jobs...")
        if mng.on_job_trigger():
            print("Job triggered!")
            # Run the container - replace 'python:3.8-slim' with your specific container
            mng.spawn_container("python:3.8-slim")
        else:
            print("No job triggered.")
        time.sleep(5)  # Wait a bit before checking again