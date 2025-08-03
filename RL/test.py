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
min_time_reward = math.inf
max_time_reward = -math.inf
min_throughput_reward = math.inf
max_throughput_reward = -math.inf
min_imbalance_reward = math.inf
max_imbalance_reward = -math.inf
min_index_size_reward = math.inf
max_index_size_reward = -math.inf
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
a = []
def setup_seed(seed):
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    np.random.seed(seed)
    random.seed(seed)
    torch.backends.cudnn.deterministic = True
 
setup_seed(24)

def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory {directory_path} created.")
    else:
        print(f"Directory {directory_path} already exists.")



query = "range"

size = f"{MIN_nCell}-{MAX_nCell}"
time_rate = 0
imbalance_rate = 0.5
index_size_rate = 0.5
throughput_rate = 0.5

state_reward_path = f"{query}/state_reward"
ensure_directory_exists(state_reward_path)

log_path = "test_log.txt"
print(log_path)



def main():
    global flag, subtask_state_buffer,time_data_count,max_throughput_reward,min_throughput_reward,c_sys_time,p_sys_time
    rl = RL(lr)
    rl.log_path = log_path
    rl.process_training_data(train_data_path)
    rl.train(100)
    index_size = 0
    grid_size = 0
    current_state = np.zeros(nCell_state_lat * nCell_state_lon)
    print(len(current_state))
    next_state = current_state.copy()
    final_reward = float('inf')
    online_experiences = []
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
    th = {}
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
                    log(f"ptime: {ptime}, time_step: {time_step}",rl.log_path)
                    imbalance_reward = None
                    throughput_reward = None
                    if(time_step - ptime != TIMEGAP):
                        log(f"subtask_state_buffer: {len(subtask_state_buffer[time_step])}  keys:{subtask_state_buffer[time_step].keys()}",rl.log_path)
                    c_sys_time = time.time()
                    imbalance_reward = calculate_imbalance_reward(time_data_count[time_step], rl.log_path)
                    throughput_reward = calculate_throughput_reward(time_data_count[time_step],c_sys_time,p_sys_time)
                    log(
                            f"Imbalance {imbalance_reward}, throughput_reward {throughput_reward}, Grid_size {grid_size},Index_size {index_size}",rl.log_path)
                    th[time_step] = throughput_reward
                    if subtask_state_buffer[time_step]:
                        state = next_state.copy()

                        if imbalance_reward != None and throughput_reward != None and index_size != 0:
                            if  step > 1 and step <= online_steps:

                                rl.min_throughput_reward = min(rl.min_throughput_reward, throughput_reward)
                                rl.max_throughput_reward= max(rl.max_throughput_reward, throughput_reward)
                                online_experiences.append((state, action_l,action_h, imbalance_reward, index_size, throughput_reward))
                                if step == online_steps:
                                    log(f"min_throughput_reward: {rl.min_throughput_reward}, max_throughput_reward: {rl.max_throughput_reward}",rl.log_path)
                            if step == online_steps:
                                for experience in online_experiences:
                                    state, action_l, action_h, imbalance_reward, index_size, throughput_reward = experience
                                    rl.insert_experience(state, action_l, action_h, imbalance_reward, index_size, throughput_reward)

                                online_experiences.clear()
                        with open(f"{state_reward_path}/state_reward_{step}.txt", 'w') as of_state:
                            for i in range(nCell_state_lat * nCell_state_lon):
                                of_state.write(f"{current_state[i]}, ")
                            of_state.write(f"Action: {nCell_size}, imbalance_reward {imbalance_reward}, index_size_reward {index_size},  Final_reward: {final_reward},Grid_size {grid_size}")
                        if  step > online_steps:
                            rl.insert_experience(state, action_l, action_h, imbalance_reward, index_size, throughput_reward)

                    state_list = [0] * len(next(iter(subtask_state_buffer[time_step].values())))

                    for id, arr in subtask_state_buffer[time_step].items():
                        state_list = [sum(x) for x in zip(state_list, arr)]

                    current_state = np.array(state_list)
                    next_state = current_state.copy()
                    del subtask_state_buffer[time_step]
                    del time_data_count[time_step]
                    time_list.clear()
                    ptime = time_step
                    p_sys_time = c_sys_time
                    break

        if  step == online_steps:
            rl.train_online(10)
        if  has_sent_first_action :                     
            if  step > online_steps:
                loss = rl.update_model_by_test(batch_size)
                loss = rl.update_model_by_train(batch_size)
            else :
                loss = rl.update_model_by_train(batch_size)
            action_l,action_h = rl.select_action(next_state)
        else :
            action_h = random.choice(rl.actions)
            has_sent_first_action = True  
        a.append(action_h)
        nCell_size = action_h
        producer.send(producer_topic, nCell_size)
        producer.flush()
        step += 1
        rl.step = step
        log(f"\nstep: {step}  Action: {nCell_size}",rl.log_path)
        log(", ".join(map(str, a)), rl.log_path)
        log(str(th),rl.log_path)
        

if __name__ == "__main__":
    main()

