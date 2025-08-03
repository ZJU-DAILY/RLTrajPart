import torch
import ast
from collections import defaultdict
from config import *
from utils import *
import math
import numpy as np
import random
from RL import RL
from kafka import KafkaConsumer, KafkaProducer
import json
import time
torch.backends.cudnn.enabled = False
c_sys_time = time.time()
p_sys_time = time.time()
time_list = []
subtask_data_volumes = {}
subtask_state_buffer = defaultdict(dict)
time_data_count = defaultdict(list)
timestamp_list = []
flag = False
index_size = 0
grid_size = 0
import sys

import os

def setup_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True
 
setup_seed(42)

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory {directory_path} created.")
    else:
        print(f"Directory {directory_path} already exists.")


size = f"{MIN_nCell}-{MAX_nCell}"
time_rate = 0
imbalance_rate = 0.5
index_size_rate = 0.5
throughput_rate = 0
times = 2


state_reward_path = f"train/state_reward"
ensure_directory_exists(state_reward_path)

log_path = "train_log.txt"


print(log_path)
def log(message, file_path= log_path):
    with open(file_path, 'a') as f:
        f.write(message + "\n")



def main():
    global flag, subtask_state_buffer,time_data_count,c_sys_time,p_sys_time
    actions = np.linspace(MIN_nCell, MAX_nCell, K).astype(int)
    current_state = np.zeros(nCell_state_lat * nCell_state_lon)
    print(len(current_state))
    next_state = torch.from_numpy(current_state).float()
    next_state = torch.from_numpy(current_state).view(nCell_state_lat, nCell_state_lon).to(
        dtype=torch.float32)
    next_state = next_state.clone().detach()
    step = 0
    ptime = 0
    time_step = 0
    consumer = KafkaConsumer(
    state_topic,
    subtask_topic,
    bootstrap_servers=kafka_servers,
    auto_offset_reset='earliest',  
    enable_auto_commit=False,  
    value_deserializer=lambda x: x.decode('utf-8') 
)
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    has_sent_first_action = False 
    nCell_size = 0
    while True:
        if has_sent_first_action:
            for message in consumer:
                if message.topic == state_topic:
                    id_str, time_str, task_str, grid_size_str, index_size_str, array_str = message.value.split(',', 5)
                    time_step = int(time_str)
                    task_cnt = int(task_str)
                    if(int(grid_size_str)!= 0):
                        grid_size = int(grid_size_str)
                        index_size = - int(index_size_str)
                    array = ast.literal_eval(array_str)
                    time_data_count[time_step].append(task_cnt)
                    subtask_state_buffer[time_step][id_str] = array

                if (len(subtask_state_buffer[time_step]) == PARALLELISM):
                    log(f"ptime: {ptime}, time_step: {time_step}")
                    imbalance_reward = None
                    if(time_step - ptime != TIMEGAP):
                        log(f"subtask_state_buffer: {len(subtask_state_buffer[time_step])}  keys:{subtask_state_buffer[time_step].keys()}")
                    c_sys_time = time.time()
                    imbalance_reward = calculate_imbalance_reward(time_data_count[time_step],log_path)
    
                    if subtask_state_buffer[time_step] and nCell_size and imbalance_reward and index_size and grid_size:

                        with open(f"{state_reward_path}/state_reward_{step}.txt", 'w') as of_state:
                            for i in range(nCell_state_lat * nCell_state_lon):
                                of_state.write(f"{current_state[i]}, ")
                            of_state.write(f"action: {nCell_size}, imbalance_reward: {imbalance_reward}, index_size_reward: {index_size}, grid_size: {grid_size}")

                    state_list = [0] * len(next(iter(subtask_state_buffer[time_step].values())))

                    for id, arr in subtask_state_buffer[time_step].items():
                        state_list = [sum(x) for x in zip(state_list, arr)]

                    current_state = np.array(state_list)
                    next_state = torch.from_numpy(current_state).view(nCell_state_lat, nCell_state_lon).to(
                        dtype=torch.float32)
                    next_state = next_state.clone().detach()
                    del subtask_state_buffer[time_step]
                    del time_data_count[time_step]
                    time_list.clear()
                    ptime = time_step
                    p_sys_time = c_sys_time
                    break

        step += 1


    
        nCell_size = actions[(step-1)%len(actions)]

        producer.send(producer_topic, nCell_size)
        producer.flush()
        log(f"step: {step}  Action: {nCell_size}")
        has_sent_first_action = True  



if __name__ == "__main__":
    main()

