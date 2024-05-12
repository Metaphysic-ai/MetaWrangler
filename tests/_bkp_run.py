def run(self, sandbox=False):
    return
    '''
    This is just here as a lookup as to how an earlier version worked, don't use.
    '''
    import socket
    import subprocess
    from datetime import datetime
    print("Starting MetaWrangler Service...")

    hostname = socket.gethostname()
    host = '0.0.0.0'
    port = 12121 if not sandbox else 12120  ### Set to different port to stop live submits to enter MetaWrangler

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
            request = self.handle_client(client_socket)
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
                self.task_event_history[str(task_event.id)] = {"profile": task_event, "job": metajob}
                self.task_event_stack.pop(0)
            else:
                self.task_event_stack.append(
                    self.task_event_stack.pop(0))  ### put task to the end of the stack in case one gets stuck