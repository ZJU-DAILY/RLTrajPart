import torch
import torch.optim as optim

# from main_subtask_state import log
from utils import *
from model import *
from config import *
from numpy.linalg import norm
import time
class RL:
    def __init__(self, lr):
        self.lr = lr
        self.weight_decay = ADAM_WEIGHT_DECAY
        self.model = Model()
        # self.model.to(torch.device("cuda"))
        self.device = torch.device("cpu")
        self.model.to(self.device)
        self.optimizer = optim.Adam(self.model.parameters(), lr=lr)
        self.replay_buffer = {} 
        self.replay_buffer_size = 0
        self.cluster_centers = {} 
        self.train_replay_buffer = {}
        self.train_cluster_centers = {}
        self.actions = np.linspace(MIN_nCell, MAX_nCell, K).astype(int)
        self.base_interval = int(np.diff(self.actions).mean())
        self.delta_range = self.base_interval // 2
        self.actions_cnt = {action:0 for action in range(MIN_nCell,MAX_nCell+1)}
        self.K = len(self.actions)
        self.epsilon = EPSILON
        self.min_imbalance_reward = 1e6
        self.max_imbalance_reward = -1e6
        self.min_index_size_reward = 1e6
        self.max_index_size_reward = -1e6
        self.min_throughput_reward = 1e6
        self.max_throughput_reward = -1e6
        self.alpha = 0.5
        self.beta = 0.5
        self.lambda_param = LAMBDA
        self.max_memory_size = MAX_MEMORY_SIZE
        self.mc = 0  
        self.nc = 0  
        self.clu_id = 0
        # self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.state = np.zeros((1, nCell_state_lat * nCell_state_lon), dtype=np.float32)
        self.log_path = ""
        self.step = 0
        # self.process_training_data(train_data_path)
        self.exploration_time_list = []
        self.exploitation_time_list = []

    def process_training_data(self, file_path):
        data = np.load(file_path, allow_pickle=True)
        n_clusters = data['n_clusters']

        for cluster_id in range(n_clusters):
            imbalance_rewards = data[f"cluster_{cluster_id}_imbalance_rewards"]
            index_size_rewards = data[f"cluster_{cluster_id}_index_size_rewards"]
            self.min_imbalance_reward = min(self.min_imbalance_reward, np.min(imbalance_rewards))
            self.max_imbalance_reward = max(self.max_imbalance_reward, np.max(imbalance_rewards))
            self.min_index_size_reward = min(self.min_index_size_reward, np.min(index_size_rewards))
            self.max_index_size_reward = max(self.max_index_size_reward, np.max(index_size_rewards))

        for cluster_id in range(n_clusters):
            states = data[f"cluster_{cluster_id}_states"]
            actions = data[f"cluster_{cluster_id}_actions"]
            imbalance_rewards = data[f"cluster_{cluster_id}_imbalance_rewards"]
            index_size_rewards = data[f"cluster_{cluster_id}_index_size_rewards"]
            grid_sizes = data[f"cluster_{cluster_id}_grid_sizes"]
            centroid = data[f"cluster_{cluster_id}_centroid"]

            self.train_cluster_centers[cluster_id] = {"state": centroid, "cluster_id": cluster_id}
            # print(len(centroid))
            replay_buffer = []
            for state, action_l, imbalance_reward, index_size_reward, grid_size in zip(
                states, actions, imbalance_rewards, index_size_rewards, grid_sizes
            ):
                log(f"Imbalance grid_size: {grid_size}",self.log_path)
                final_reward = self.compute_final_reward(imbalance_reward, index_size_reward)
                # print(state)
                experience = Experience(state, action_l, None, final_reward, None)
                replay_buffer.append(experience)
                # experience = Experience(state, action_l,None, final_reward, None)
                # replay_buffer.append(experience)

            self.train_replay_buffer[cluster_id] = replay_buffer

        log("Training data processed successfully.",self.log_path)

    def insert_experience(self, state, action_l,action_h, imbalance_reward, index_size_reward, throughput_reward):

        final_reward = self.compute_final_reward(imbalance_reward, index_size_reward)
        throughput_reward = self.compute_norm_throughput(throughput_reward)
        new_experience = Experience(state, action_l,action_h, final_reward, throughput_reward)
        self.add_to_replay_buffer(new_experience)

    def direct_store(self, experience, cluster_id, is_center):

        if cluster_id not in self.replay_buffer:
            self.replay_buffer[cluster_id] = [] 

        self.replay_buffer[cluster_id].append((experience, is_center))
        self.replay_buffer_size += 1

    def add_to_replay_buffer(self, experience):
    
        cluster_id, new_center = self.odmedoids(experience)


        if self.replay_buffer_size < self.max_memory_size:
            self.direct_store(experience, cluster_id, new_center)
            if new_center:
                self.cluster_centers[cluster_id] = experience
                self.mc += 1
                self.nc += 1
        else:
            if new_center:
                self.rs_assign_center(experience, cluster_id)
            else:
                self.rs_assign_noncenter(experience, cluster_id)

    def rs_assign_center(self, experience, cluster_id):
        if any(not is_center for cluster in self.replay_buffer.values() for _, is_center in cluster):
            largest_cluster = self.get_largest_cluster()
            self.remove_random_instance_from_cluster(largest_cluster)
            self.direct_store(experience, cluster_id, True)
            self.cluster_centers[cluster_id] = experience
            self.mc += 1
            self.nc += 1
        else:
            sample_prob = random.random()
            if sample_prob < self.mc / self.nc:
                self.remove_random_center()
                self.direct_store(experience, cluster_id, True)
                self.cluster_centers[cluster_id] = experience
                self.mc += 1
                self.nc += 1

    def rs_assign_noncenter(self, experience, cluster_id):
        has_non_center_instance = any(
            not is_center for cluster in self.replay_buffer.values() for _, is_center in cluster
        )
        
        if has_non_center_instance:  
            cluster_instances = self.replay_buffer.get(cluster_id, [])
            if len(cluster_instances) < self.get_largest_cluster_size():
                self.replace_noncenter_instance_in_largest_cluster(experience, cluster_id)
            else:
                self.replace_noncenter_instance_in_cluster(experience, cluster_id)
        else:
            log(f"No non-center instances found across all clusters, skipping replacement.",self.log_path)

    def replace_noncenter_instance_in_largest_cluster(self, experience, cluster_id):
        largest_cluster = self.get_largest_cluster()
        self.remove_random_instance_from_cluster(largest_cluster)
        self.direct_store(experience, cluster_id, False)

    def replace_noncenter_instance_in_cluster(self, experience, cluster_id):
        cluster_instances = self.replay_buffer.get(cluster_id, [])
        sample_prob = random.random()
        if sample_prob < len(cluster_instances) / self.max_memory_size:
            self.remove_random_instance_from_cluster(cluster_id)
            self.direct_store(experience, cluster_id, False)

    def remove_random_instance_from_cluster(self, cluster_id):
        cluster_instances = self.replay_buffer.get(cluster_id, [])
        non_center_instances = [i for i, (_, is_center) in enumerate(cluster_instances) if not is_center]
        if non_center_instances:
            random_instance_index = random.choice(non_center_instances)
            del cluster_instances[random_instance_index]
            self.replay_buffer_size -= 1
            if not cluster_instances:
                del self.replay_buffer[cluster_id]

    def remove_random_center(self):
        if not self.cluster_centers:
            return
        random_center_id = random.choice(list(self.cluster_centers.keys()))
        del self.cluster_centers[random_center_id]
        if random_center_id in self.replay_buffer:
            self.replay_buffer_size -= len(self.replay_buffer[random_center_id])
            del self.replay_buffer[random_center_id]
        self.mc -= 1

    def get_largest_cluster(self):
        return max(self.replay_buffer, key=lambda cid: len(self.replay_buffer[cid]))

    def get_largest_cluster_size(self):
        if not self.replay_buffer:
            return 0
        return max(len(cluster) for cluster in self.replay_buffer.values())

    def odmedoids(self, experience):
        current_state = np.array(experience.state)
        if not self.cluster_centers:
            self.clu_id += 1
            return self.clu_id, True

        similarities = []
        for cluster_id, center in self.cluster_centers.items():
            cluster_state = np.array(center.state)
            similarity = np.dot(current_state, cluster_state) / (norm(current_state) * norm(cluster_state) + 1e-8)
            similarities.append((cluster_id, similarity))

        most_similar = max(similarities, key=lambda x: x[1])
        max_similarity = most_similar[1]
        min_dist = 1 - max_similarity

        prob = min_dist / self.lambda_param
        sample_prob = random.random()
        
        if sample_prob < prob:
            # print(f"{sample_prob} < {prob}")
            self.clu_id += 1
            return self.clu_id, True
        else:
            return most_similar[0], False


    def update_model_by_train(self, batch_size):

        if not self.train_replay_buffer or not self.train_cluster_centers:
            log("No training data available.",self.log_path)
            return -1

        current_state = np.array(self.state) 

        similarities = []
        for cluster_id, cluster_center in self.train_cluster_centers.items():
            cluster_state = np.array(cluster_center["state"]) 
            # print(type(current_state))
            # print(type(cluster_state))
            dot_product = np.dot(current_state, cluster_state)
            similarity = dot_product / (norm(current_state) * norm(cluster_state) + 1e-8)
            similarities.append((cluster_id, similarity))

        similarities.sort(key=lambda x: x[1], reverse=True)
        closest_clusters = [cluster_id for cluster_id, _ in similarities]

        selected_experiences = []

        for cluster_id in closest_clusters:
            cluster_experiences = self.train_replay_buffer.get(cluster_id, [])
            
            if len(cluster_experiences) >= batch_size - len(selected_experiences):
                selected_experiences.extend(random.sample(cluster_experiences, batch_size - len(selected_experiences)))
                break
            else:
                selected_experiences.extend(cluster_experiences)
        actual_batch_size = len(selected_experiences)
        if actual_batch_size == 0:
            log("No experiences available for training.",self.log_path)
            return -1

        log(f"Selected {actual_batch_size} experiences for training (requested {batch_size}).",self.log_path)
        state_batch = []
        action_batch = []
        final_reward_batch = []


        for i, experience in enumerate(selected_experiences):
            state_batch.append(experience.state.clone().detach())
            action_batch.append(experience.action_l.clone().detach())
            final_reward_batch.append(experience.final_reward.clone().detach())


        state_batch = torch.stack(state_batch).unsqueeze(1).to(self.device)  # (N, 1, H, W)
        action_batch = torch.stack(action_batch).unsqueeze(1).to(self.device)  # (N, 1, D)
        final_reward_batch = torch.stack(final_reward_batch).unsqueeze(1).to(self.device)  # (N, 1)
        state_batch = state_batch.view(state_batch.size(0), 1, nCell_state_lat, nCell_state_lon)
        action_batch = action_batch.view(action_batch.size(0), 1, -1)
        output,_= self.model(state_batch, action_batch)

        loss = torch.nn.functional.mse_loss(output, final_reward_batch)

        total_loss = loss
        self.optimizer.zero_grad()
        total_loss.backward()
        self.optimizer.step()
        log(f"Loss1 (final reward): {loss.item()}, Total Loss: {total_loss.item()}",self.log_path)

        return total_loss.item()


    def update_model_by_test(self, batch_size):
        if not self.replay_buffer:
            return -1
        total_experiences = sum(len(experiences) for experiences in self.replay_buffer.values())
        if total_experiences < batch_size:
            batch_size = total_experiences

        current_state = self.state

        similarities = []
        for cluster_id, cluster_center in self.cluster_centers.items():
            cluster_state = np.array(cluster_center.state) 
            
            dot_product = np.dot(current_state, cluster_state)
            similarity = dot_product / (norm(current_state) * norm(cluster_state) + 1e-8)
            similarities.append((cluster_id, similarity))

        similarities.sort(key=lambda x: x[1], reverse=True)
        closest_clusters = [cluster_id for cluster_id, _ in similarities]

        selected_experiences = []

        for cluster_id in closest_clusters:
            cluster_experiences = self.replay_buffer.get(cluster_id, [])
            if len(cluster_experiences) >= batch_size - len(selected_experiences):
                selected_experiences.extend(
                    [experience for experience, _ in random.sample(cluster_experiences, batch_size - len(selected_experiences))]
                )
                break
            else:
                selected_experiences.extend([experience for experience, _ in cluster_experiences])
        actual_batch_size = len(selected_experiences)
        if actual_batch_size == 0:
            log("No experiences available for training.",self.log_path)
            return -1

        log(f"Selected {actual_batch_size} online experiences for training (requested {batch_size}).",self.log_path)
        state_batch = []
        action_l_batch = []
        action_h_batch = []
        final_reward_batch = []
        throughput_reward_batch = []
        for i, experience in enumerate(selected_experiences):
            state_batch.append(experience.state.clone().detach())
            action_l_batch.append(experience.action_l.clone().detach())
            action_h_batch.append(experience.action_h.clone().detach())
            final_reward_batch.append(experience.final_reward.clone().detach())
            throughput_reward_batch.append(experience.throughput_reward.clone().detach())
        state_batch = torch.stack(state_batch).unsqueeze(1).to(self.device)  # (N, 1, H, W)
        action_l_batch = torch.stack(action_l_batch).unsqueeze(1).to(self.device)  # (N, 1, D)
        action_h_batch = torch.stack(action_h_batch).unsqueeze(1).to(self.device)
        final_reward_batch = torch.stack(final_reward_batch).unsqueeze(1).to(self.device)  # (N, 1)
        throughput_reward_batch = torch.stack(throughput_reward_batch).unsqueeze(1).to(self.device)
        state_batch = state_batch.view(state_batch.size(0), 1, nCell_state_lat, nCell_state_lon)
        action_l_batch = action_l_batch.view(action_l_batch.size(0), 1, -1)
        action_h_batch = action_h_batch.view(action_h_batch.size(0), 1, -1)
        


        self.optimizer.zero_grad()
        output_low,output_high = self.model(state_batch, action_l_batch)
        loss_low = F.mse_loss(output_low, final_reward_batch)
        loss_high = F.mse_loss(output_high, throughput_reward_batch)
        loss = loss_low * 0.5 + loss_high * 0.5 
        output_low,output_high = self.model(state_batch, action_h_batch) 
        loss_high = F.mse_loss(output_high, throughput_reward_batch)
        total_loss = loss * 0.5  + loss_high * 0.5 
        total_loss.backward()
        self.optimizer.step()

        
        log(f"Loss1 (final reward): {loss_low.item()}, Loss2 (throughput_reward): {loss_high.item()}, Total Loss: {total_loss.item()}", self.log_path)
        result = total_loss.item()


        return result



    def exploitation(self):
        state_batch = torch.empty((self.K, 1, nCell_state_lat, nCell_state_lon))
        action_batch = torch.empty((self.K, 1, 1))

        for i in range(self.K):
            state_batch[i][0] = torch.tensor(self.state, dtype=torch.float32).view(1, 1, nCell_state_lat, nCell_state_lon)
            action_batch[i][0] = torch.tensor([self.actions[i]], dtype=torch.float32)

        state_batch = state_batch.to(self.device)
        action_batch = action_batch.to(self.device)

        self.model.eval()
        with torch.no_grad():
            final_reward,throughput_reward = self.model(state_batch, action_batch)
        

        self.model.train()
        if self.step >= online_steps:
            balanced_reward = self.beta*final_reward+(1 - self.beta)*throughput_reward
        else:
             
            balanced_reward = final_reward
        l_idx = torch.argmax(balanced_reward.view(-1)).item()
        a_l = self.actions[l_idx]
        a_h = a_l
        log("\nActions and predicted rewards:", self.log_path)
        for i in range(self.K):
            log(f"Action: {self.actions[i]}, Final Reward: {final_reward[i].item()}, throughput_reward: {throughput_reward[i].item()}, balanced_reward: {balanced_reward[i].item()}", self.log_path)
        if self.step > online_steps:
            low = max(a_l - self.delta_range, MIN_nCell)
            high = min(a_l + self.delta_range, MAX_nCell)
            high_actions = np.arange(low, high+1)
    
            state_batch = torch.empty((len(high_actions),1, nCell_state_lat, nCell_state_lon))
            action_batch = torch.empty((len(high_actions), 1, 1))

            for i in range(len(high_actions)):
                state_batch[i][0] = torch.tensor(self.state, dtype=torch.float32).view(1, 1, nCell_state_lat, nCell_state_lon)
                action_batch[i][0] = torch.tensor([high_actions[i]], dtype=torch.float32)

            state_batch = state_batch.to(self.device)
            action_batch = action_batch.to(self.device)
            self.model.eval()
            with torch.no_grad():
                _,throughput_reward = self.model(state_batch, action_batch)
            
            self.model.train()
            h_idx = torch.argmax(throughput_reward.view(-1)).item()
            a_h = high_actions[h_idx]
            log("\nActions and predicted rewards:", self.log_path)
            for i in range(len(high_actions)):
                log(f"Action: {high_actions[i]}, Throughput Reward: {throughput_reward[i].item()}", self.log_path)

        log(f"Selected action based on low reward: {a_l}", self.log_path)
        log(f"Selected action based on high reward: {a_h}", self.log_path)

        
        
        
        return a_l,a_h


    def exploration(self):
        a_l = self.get_max_bald_action(self.state,self.actions,False)

        low = max(a_l - self.delta_range, MIN_nCell)
        high = min(a_l + self.delta_range, MAX_nCell)
        high_actions = np.arange(low, high+1)


        a_h = self.get_max_bald_action(self.state,high_actions,True)
        self.actions_cnt[a_l] += 1
        self.actions_cnt[a_h] += 1

        return a_l,a_h

    def sample_model_predictions_batch(self, actions, current_state,h):

        num_samples = 5
        batch_size = len(actions)

        all_samples_output1 = [[] for _ in range(batch_size)]
        self.model.train()
        for _ in range(num_samples):
            current_state_batch = (
                torch.tensor(current_state, dtype=torch.float32)
                .unsqueeze(0)
                .unsqueeze(1)
                .repeat(batch_size, 1, 1, 1)
                .to(self.device)
            )
            actions_batch = (
                torch.tensor(actions, dtype=torch.float32)
                .view(batch_size, 1, -1)
                .to(self.device)
            )

            current_state_batch = current_state_batch.view(batch_size, 1, nCell_state_lat, nCell_state_lon)

            if h:
                _,outputs = self.model(current_state_batch, actions_batch)
            else:
                outputs1,outputs2 = self.model(current_state_batch, actions_batch)
            for i in range(batch_size):
                all_samples_output1[i].append(outputs[i].item())

        return all_samples_output1

    def get_max_bald_action(self, current_state,actions,h):

        max_info_action = None
        max_info_value = -float("inf")

        samples_output1_batch = self.sample_model_predictions_batch(actions, current_state,h)

        for i, action in enumerate(actions):
            samples_output1 = samples_output1_batch[i]

            # if any(np.isnan(samples_output1)) or any(np.isnan(samples_output2)):
            #     log(f"NaN in model output for action {action}: {samples_output1}, {samples_output2}",self.log_path)
            # if any(np.isinf(samples_output1)) or any(np.isinf(samples_output2)):
            #     log(f"Inf in model output for action {action}: {samples_output1}, {samples_output2}",self.log_path)

            entropy_output1 = self.calculate_entropy(samples_output1)

            mutual_info = entropy_output1  - (
                self.calculate_entropy(np.mean(samples_output1)) 
            )

            log(f"action: {action}, mutual_info: {mutual_info}",self.log_path)
            if mutual_info > max_info_value:
                max_info_value = mutual_info
                max_info_action = action

        return max_info_action


    def calculate_entropy(self, samples):

        probabilities = np.array(samples)
        if np.any(np.isnan(probabilities)) or np.any(np.isinf(probabilities)):
            log(f"Invalid samples detected: {samples}",self.log_path)
            return 0  

        total_sum = np.sum(probabilities)
        if total_sum == 0:
            log(f"Sum of probabilities is 0 for samples: {samples}",self.log_path)
            return 0

        probabilities = probabilities / total_sum  
        probabilities = np.clip(probabilities, 1e-9, 1.0)  
        entropy = -np.sum(probabilities * np.log(probabilities))
        return entropy




    def select_action(self, state):
        self.state = state
        start_time = time.time()
        if np.random.rand() < self.epsilon:
            log(f"exploration------------------------------------------------------------",self.log_path)
            a_l,a_h =  self.exploration()
        
            elapsed_time = time.time() - start_time
            self.exploration_time_list.append(elapsed_time)
        else:
            log(f"exploitation------------------------------------------------------------",self.log_path)

            a_l,a_h =  self.exploitation()
            elapsed_time = time.time() - start_time
            self.exploitation_time_list.append(elapsed_time)
        return a_l,a_h
    
    def average_exploration_time(self):
        if self.exploration_time_list:
            return sum(self.exploration_time_list) / len(self.exploration_time_list)
        else:
            return 0.0

    def average_exploitation_time(self):
        if self.exploitation_time_list:
            return sum(self.exploitation_time_list) / len(self.exploitation_time_list)
        else:
            return 0.0

    def compute_final_reward(self, imbalance_reward, index_size_reward):
        normalized_imbalance_reward = (
            (imbalance_reward - self.min_imbalance_reward) / (self.max_imbalance_reward - self.min_imbalance_reward)
            if self.max_imbalance_reward > self.min_imbalance_reward else 0.0
        )
        normalized_index_size_reward = (
            (index_size_reward - self.min_index_size_reward) / (self.max_index_size_reward - self.min_index_size_reward)
            if self.max_index_size_reward > self.min_index_size_reward else 0.0
        )
        final_reward = normalized_imbalance_reward * self.alpha+ normalized_index_size_reward * (1 - self.alpha)
        log(f"Imbalance Reward: {normalized_imbalance_reward}, Index Size Reward: {normalized_index_size_reward}, Final Reward: {final_reward}",self.log_path)
        return final_reward
    def compute_norm_throughput(self, throughput_reward):
        normalized_throughput_reward = (
                (throughput_reward - self.min_throughput_reward) / (self.max_throughput_reward - self.min_throughput_reward)
                if self.max_throughput_reward > self.min_throughput_reward else 0.0
            )

        log(f"Throughput Reward: {normalized_throughput_reward}",self.log_path)
            
        return normalized_throughput_reward

    
    def train(self, epochs):

        if not self.train_replay_buffer:
            log("Train replay buffer is empty. Cannot train the model.",self.log_path)
            return

        

        for epoch in range(epochs):
            # print(f"Starting epoch {epoch + 1}/{epochs}...")

            total_loss = 0
            total_samples = 0

            state_batches = []
            action_batches = []
            final_reward_batches = []


            for cluster_id, experiences in self.train_replay_buffer.items():
                for experience in experiences:
                    state_batches.append(experience.state.clone().detach())
                    action_batches.append(experience.action_l.clone().detach())
                    final_reward_batches.append(experience.final_reward.clone().detach())

                    # print(experience.state.size())
                    # print(experience.action.size())

            state_batches = torch.stack(state_batches).unsqueeze(1).to(self.device)  # (N, 1, H, W)
            action_batches = torch.stack(action_batches).unsqueeze(1).to(self.device)  # (N, 1, D)
            final_reward_batches = torch.stack(final_reward_batches).unsqueeze(1).to(self.device)  # (N, 1)

            state_batches = state_batches.view(state_batches.size(0), 1, nCell_state_lat, nCell_state_lon)

            action_batches = action_batches.view(action_batches.size(0), 1, -1)

            dataset_size = state_batches.size(0)
            for i in range(0, dataset_size, batch_size):
                end_idx = min(i + batch_size, dataset_size)

                state_batch = state_batches[i:end_idx]
                action_batch = action_batches[i:end_idx]
                final_reward_batch = final_reward_batches[i:end_idx]
                if state_batch.size(0) == 1:
                    continue
                output1,_ = self.model(state_batch, action_batch)

                loss = torch.nn.functional.mse_loss(output1, final_reward_batch)

                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                total_loss += loss.item() * (end_idx - i)
                total_samples += (end_idx - i)

            avg_loss = total_loss / total_samples if total_samples > 0 else 0
            if (epoch + 1)%10 == 0:
                log(f"Epoch {epoch + 1}/{epochs} completed. Average Loss: {avg_loss:.6f}",self.log_path)

        log("Training completed.",self.log_path)


    def get_dynamic_weights(self,epoch, total_epochs):
        min_weight = 0.1  
        max_weight = 0.9  
        progress = epoch / total_epochs
        final_weight = max_weight - (max_weight - min_weight) * progress  
        throughput_weight = 1.0 - final_weight  

        return final_weight, throughput_weight
    

