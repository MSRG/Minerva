#! /usr/bin/python3
import pathlib
import sys
import json
import socket
import os
import subprocess
import time
import math
from pathlib import Path

# remote:
# run startservers.py separately on each server (start, stop)
# start server remote use ssh to call startservers.py on all servers


ROOT_PATH = str(Path(__file__).resolve().parent.parent)
PROJECT_PATH = ROOT_PATH + "/src/DB-Server"
CWD_PATH  = str(os.path.dirname(Path(__file__).resolve()))
START_PORT = 5000

REMOTE_USER_NAME = "ubuntu"
REMOTE_KEY_FILE = "~/msrg"
REMOTE_STORE_PATH = "~/Minerva"


def each_server_json(node_id: int, servers_per_host: int, host_list: list , selfip: str,  print_addr: bool = False) -> str:
    '''
    Generate the JSON file of a single host and store it in [CWD]/configs directory. 
    '''
    res = []
    addresses = []
    addresses_to_print = []

    i = 0
    for ip in host_list:
        port_offset = 0
        for _ in range(servers_per_host):
            isself = False
            if (i == node_id and ip == selfip):
                isself = True

            cfg = {
                "Id": i,
                "Address": ip,
                "Port": START_PORT + port_offset,
                "IsSelfNode": isself
            }
            res.append(cfg)
            addresses.append(ip + ":" + str(START_PORT + port_offset))
            addresses_to_print.append(ip + ":" + str(START_PORT + port_offset))
            i += 1
            port_offset += 1

    if print_addr:
        print("Server's client interface addresses:")
        print(addresses_to_print)
        

    return json.dumps(res), addresses


def generate_json(servers_per_host : int, host_list : list) -> list:
    '''
    Generate JSON files for each server instances and store them in [CWD]/configs directory. 
    Return a list of addresses of all servers.
    '''

    

    if host_list == []:
        # local ip
        selfip = socket.gethostbyname(socket.gethostname())
        host_list = [selfip]

    addresses = []
    i = 0

    for ip in host_list:
        for _ in range(servers_per_host):
            cfg_json, addresses = each_server_json(i, servers_per_host, host_list, ip, i == 0)
            f = open(CWD_PATH + "/configs/cluster_config." + str(i) + ".json", "w")
            f.write(cfg_json)
            f.close()
            i += 1

    return addresses

def build_server():
    '''
    Build a single self-contained executable for the server and stored in [CWD]/build directory.
    '''
    print("Building...")
    
    # create build directory if not exist
    if not os.path.exists(CWD_PATH + "/build"):
        os.makedirs(CWD_PATH + "/build")
    else:
        # clean build
        subprocess.Popen(["rm", "-rf", CWD_PATH + "/build/*"]).wait()

    subprocess.Popen(
            ["dotnet", "publish", PROJECT_PATH, "-c", "Release", "-r", "linux-x64", "--self-contained", "true", "-f", "net9.0" , "--output", CWD_PATH + "/build"]).wait()
    # subprocess.Popen(
    #         ["dotnet", "build", PROJECT_PATH, "-r", "linux-x64", "--self-contained", "true", "-f", "net9.0" , "--output", CWD_PATH + "/temp"]).wait()
    


def start_server(servers_per_host : int, host_list : list = []) -> list:
    '''
    Start [servers_per_host] * [host_list] servers. 
    If servers_list is empty, start servers on local host.
    '''

    addresses = generate_json(servers_per_host, host_list)

    if (host_list == []):
        build_server()
        run_servers_local(servers_per_host)
    else:
        run_servers_remote(1, host_list)
    
    return addresses

def run_servers_local(servers_per_host : int):
    raise NotImplementedError

def send_binary(servers_list : list):
    print("Sending files to remote node")
    delete_remote_files(servers_list)
    for ip in servers_list:
        # make remote directory if not exist
        subprocess.Popen(
            ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "mkdir -p " + REMOTE_STORE_PATH]).wait()

        # send binary and json to remote server
        subprocess.Popen(
            ["scp", "-i", REMOTE_KEY_FILE, "-r", CWD_PATH + "/build", REMOTE_USER_NAME + "@" + ip + ":" + REMOTE_STORE_PATH], stdout=subprocess.DEVNULL).wait()
        # change permission
        subprocess.Popen( 
            ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "chmod +x " + REMOTE_STORE_PATH]).wait()


def send_config(servers_list : list):
    print("Sending config files to remote node")
    for ip in servers_list:
        # send json to remote server
        subprocess.Popen(
            ["scp", "-i", REMOTE_KEY_FILE, "-r", CWD_PATH + "/configs", REMOTE_USER_NAME + "@" + ip + ":" + REMOTE_STORE_PATH], stdout=subprocess.DEVNULL).wait()



def run_servers_remote(num_server : int, servers_list : list):
    send_config(servers_list)
    temp_file = open(CWD_PATH + "/temp/ip_list_file.txt", "w")
    
    i = 0
    print("Starting processes")
    for ip in servers_list:
        temp_file.write(ip + "\n")
        for _ in range(num_server):
            cluster_cfg = REMOTE_STORE_PATH + "/configs/cluster_config." + str(i) + ".json"
            logger_cfg = REMOTE_STORE_PATH + "/configs/logger_config.json"
            db_cfg = REMOTE_STORE_PATH + "/configs/minerva_config.json"
            flog = open(CWD_PATH + "/temp/log." + str(i) + ".txt", "w")
            # run DB-Server at remote and output to flog
            subprocess.Popen(["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, REMOTE_STORE_PATH + "/build/DB-Server", db_cfg, cluster_cfg, logger_cfg], stdout=flog, stderr=flog)
            if (i == 0):
                time.sleep(1)

            i += 1

    temp_file.close()
            
    
def stop_server(delete_log: bool):
    # two types of temp_file 
    pid_temp_file = CWD_PATH + "/temp/pid_list_file.txt"
    ip_temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    # check which one exists
    if os.path.exists(pid_temp_file) and os.path.exists(ip_temp_file):
        raise IndentationError("Both local and remote servers are started!")
    
    print("Stopping processes")
    if os.path.exists(pid_temp_file):
        _stop_server(delete_log)
    elif os.path.exists(ip_temp_file):
        _stop_server_remote(delete_log)
    else:
        raise IndentationError("Servers are not started!")

# Benchmark *should* gracefully stop the servers
# this is just in case
def _stop_server_remote(delete_log: bool):
    temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    with open(temp_file, "r") as ftemp:
        line = ftemp.readline()
        while(line):
            ip = line.strip()
            proc = subprocess.run(
                ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "killall -SIGINT DB-Server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            line = ftemp.readline()

    # just in case
    with open(temp_file, "r") as ftemp:
        line = ftemp.readline()
        while(line):
            ip = line.strip()
            proc = subprocess.run(["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "killall DB-Server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            line = ftemp.readline()

    if delete_log:
        _temp_cleanup()


def delete_remote_files(servers_list : list):
    for ip in servers_list:
        subprocess.Popen(
            ["ssh", "-i", REMOTE_KEY_FILE, REMOTE_USER_NAME + "@" + ip, "rm -rf " + REMOTE_STORE_PATH]).wait()


def _stop_server(delete_log: bool):
    # delete json files
    raise NotImplementedError

def restart_server():
    # two types of temp_file 
    pid_temp_file = CWD_PATH + "/temp/pid_list_file.txt"
    ip_temp_file = CWD_PATH + "/temp/ip_list_file.txt"

    # check which one exists
    if os.path.exists(pid_temp_file) and os.path.exists(ip_temp_file):
        raise IndentationError("Both local and remote servers are started!")
    
    if os.path.exists(pid_temp_file):
        # get number of servers in pid_temp_file
        with open(pid_temp_file, "r") as ftemp:
            num_server = len(ftemp.readlines())

        _stop_server(True)
        start_server(num_server)
    elif os.path.exists(ip_temp_file):
        # get ip list
        ip_list = []
        with open(ip_temp_file, "r") as ftemp:
            line = ftemp.readline()
            while(line):
                ip_list.append(line.strip())
                line = ftemp.readline()

        _stop_server_remote(True)
        start_server(1, ip_list)
    else:
        raise IndentationError("Servers are not started!")


def _temp_cleanup():
    '''
    Delete everything in CWD + "/temp" directory
    '''
    temp_dir = os.path.join(os.getcwd(), "temp")
    
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)


if __name__ == "__main__":
    try:
        action = sys.argv[1]
    except Exception:
        raise ValueError(
            'Wrong action, Usage: StartServers.py [start/stop/restart/rstart] [number_of_servers]')

    if (action == "start"):
        try:
            num_server = int(sys.argv[2])
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py start [number_of_servers]')
        start_server(num_server)
    
    elif (action == "rdeploy"):
        try:
            ip_list_file = sys.argv[2]
            with open(ip_list_file, "r") as f:
                host_ips = [line.strip() for line in f.readlines()]
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py rdeploy [ip_list_file]')
        build_server()
        send_binary(host_ips)
    elif (action == "rdelete"):
        try:
            ip_list_file = sys.argv[2]
            with open(ip_list_file, "r") as f:
                host_ips = [line.strip() for line in f.readlines()]
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py rdelete [ip_list_file]')
        delete_remote_files(host_ips)

    elif (action == "rstart"):
        try:
            ip_list_file = sys.argv[2]
            with open(ip_list_file, "r") as f:
                host_ips = [line.strip() for line in f.readlines()]
        except Exception:
            raise ValueError(
                'Need number of server, Usage: StartServers.py rstart [ip_list_file]')
        start_server(1, host_ips)
    elif (action == "stop"):
        try:
            if sys.argv[2] == "0":
                delete_log = False
            else:
                delete_log = True
        except Exception:
            raise ValueError(
                'Usage: StartServers.py stop [0: not delete log; 1: delete log]')
        stop_server(delete_log)
    
    elif (action == "restart"):
        restart_server()
    elif (action == "build"):
        build_server()
    else:
        raise ValueError(
            'Wrong action, Usage: StartServers.py [start/stop/restart] [number_of_servers]')