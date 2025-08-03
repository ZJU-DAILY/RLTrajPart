import numpy as np
import random
import torch



class Experience:
    def __init__(self, state,action_l, action_h=None, final_reward=None, throughput_reward=None):
        self.state = torch.tensor(state, dtype=torch.float32) if isinstance(state, np.ndarray) else state.clone().detach()
        if isinstance(action_l, (np.ndarray, np.int64, int, float)): 
            self.action_l = torch.tensor(action_l, dtype=torch.float32)
        else:
            self.action_l = action_l.clone().detach()
        self.final_reward = torch.tensor(final_reward, dtype=torch.float32) if isinstance(final_reward, (np.ndarray, np.int64, int, float)) else final_reward.clone().detach()
        if action_h is not None:
            if isinstance(action_h, (np.ndarray, np.int64, int, float)): 
                self.action_h = torch.tensor(action_h, dtype=torch.float32)
            else:
                self.action_h = action_h.clone().detach()
        else:
            self.action_h = None

        if throughput_reward is not None:
            if isinstance(throughput_reward, (np.ndarray, np.int64, int, float)):
                self.throughput_reward = torch.tensor(throughput_reward, dtype=torch.float32)
            else:
                self.throughput_reward = throughput_reward.clone().detach()
        else:
            self.throughput_reward = None

    def __lt__(self, other):
        return self.final_reward.item() < other.final_reward.item()



def log(message, file_path):
    with open(file_path, 'a') as f:
        f.write(message + "\n")



def calculate_imbalance_reward(data_volumes, file_path):
    total_volume = np.sum(data_volumes)
    mean_volume = np.mean(data_volumes)
    log(f"total_volume: {total_volume}", file_path)
    if mean_volume == 0 or total_volume < 1000:
        return None
    std_dev = np.std(data_volumes)
    reward = -(std_dev / mean_volume)  
    return reward
def calculate_throughput_reward(data_volumes,  current_time, previous_time):
    total_volume = np.sum(data_volumes)
    time_interval = (current_time - previous_time)  # Use seconds between messages
    # print(total_volume)
    # print(f"current_time {current_time} previous_time {previous_time}")
    if time_interval == 0 or total_volume < 1000:
        return None
    throughput = total_volume / time_interval
    return throughput


