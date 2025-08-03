from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
import numpy as np
from utils import *
from config import *
import uuid
import numpy as np
import os
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
import numpy as np

def reduce_dimension(states, n_components=256):
    states_flat = np.vstack(states)  
    pca = PCA(n_components=n_components)  
    reduced_states = pca.fit_transform(states_flat)  
    print(f"Explained variance ratio by the first {n_components} components: {np.sum(pca.explained_variance_ratio_):.4f}")
    return reduced_states, pca

def parse_line(line):
    parts = line.split(', action: ')
    state_vector = parts[0].strip().split(', ')
    
    state = np.array([float(x) for x in state_vector])
    
    action_reward_info = parts[1]
    action_info, reward_info = action_reward_info.split(', imbalance_reward: ')
    action = int(action_info.strip())  
    
    reward_parts = reward_info.split(', ')
    imbalance_reward = None if "None" in reward_parts[0] else float(reward_parts[0].strip())
    index_size_reward = float(reward_parts[1].split(': ')[1].strip())
    grid_size = int(reward_parts[2].split(': ')[1].strip())
    
    return state, action, imbalance_reward, index_size_reward, grid_size

def read_specific_steps(directory_path, start_steps, num_steps):
    states = []
    actions = []
    imbalance_rewards = []
    index_size_rewards = []
    grid_sizes = []
    
    for step in range(start_steps, start_steps + num_steps):
        file_name = f"state_reward_{step}.txt"
        file_path = os.path.join(directory_path, file_name)
        # print(file_path)
        if os.path.isfile(file_path): 
            with open(file_path, 'r') as file:
                line = file.readline().strip()
                state, action, imbalance_reward, index_size_reward, grid_size = parse_line(line)
                states.append(state)
                actions.append(action)
                imbalance_rewards.append(imbalance_reward)
                index_size_rewards.append(index_size_reward)
                grid_sizes.append(grid_size)
    
    return states, actions, imbalance_rewards, index_size_rewards, grid_sizes

directory_path = './state_reward'
start = 1
num_initial_steps = 480  
states, actions, imbalance_rewards, index_size_rewards, grid_sizes = read_specific_steps(
    directory_path, start, num_initial_steps
)

print("States shape:", len(states), len(states[0]) if states else "N/A")
print("Actions:", actions)
print("Imbalance Rewards:", imbalance_rewards)
print("Index Size Rewards:", index_size_rewards)
print("Grid Sizes:", grid_sizes)

def normalize_states(states, min_sum=1000000):
    normalized_states = []
    for state in states:
        state = np.array(state)
        state_sum = np.sum(state)
        if state_sum >= min_sum:
            if state_sum == 0:
                normalized_states.append(state) 
            else:
                normalized_states.append(state)
        else:
            print(f"State with sum {state_sum} skipped.")
    reduced_states, pca = reduce_dimension(normalized_states)
    return normalized_states,reduced_states,pca

def gmm_clustering_and_evaluation(states, k_range):
    states_flat = np.vstack(states)
    print(states_flat[0])
    print(len(states_flat))
    print(len(states_flat[0]))
    
    bic_scores = []
    aic_scores = []
    silhouette_scores = []
    gmm_models = {}
    
    for k in k_range:
        gmm = GaussianMixture(n_components=k, random_state=42)
        gmm.fit(states_flat)
        gmm_models[k] = gmm
        
        bic = gmm.bic(states_flat)
        aic = gmm.aic(states_flat)
        bic_scores.append(bic)
        aic_scores.append(aic)
        cluster_labels = gmm.predict(states_flat)
        silhouette = silhouette_score(states_flat, cluster_labels)
        silhouette_scores.append(silhouette)
        print(f"k={k}: BIC={bic:.2f}, AIC={aic:.2f}, Silhouette={silhouette:.4f}")
    best_k_bic = k_range[np.argmin(bic_scores)]
    best_k_aic = k_range[np.argmin(aic_scores)]
    best_k_silhouette = k_range[np.argmax(silhouette_scores)]
    
    evaluation_results = {
        "bic_scores": bic_scores,
        "aic_scores": aic_scores,
        "silhouette_scores": silhouette_scores
    }
    return best_k_bic, best_k_aic, best_k_silhouette, gmm_models, evaluation_results


k_range = range(2, 21)
normalized_states,reduced_states,pca =  normalize_states(states)
best_k_bic, best_k_aic, best_k_silhouette, gmm_models, evaluation_results = gmm_clustering_and_evaluation(reduced_states, k_range)

import os
import numpy as np
from sklearn.mixture import GaussianMixture


def save_all_clusters_with_centroids(output_file, clusters, states, actions, imbalance_rewards, index_size_rewards, grid_sizes, centroids, n_clusters):
    cluster_data = {"n_clusters": n_clusters} 
    for cluster_id, indices in clusters.items():
        cluster_data[f"cluster_{cluster_id}_states"] = np.array([states[i] for i in indices])
        cluster_data[f"cluster_{cluster_id}_actions"] = np.array([actions[i] for i in indices])
        cluster_data[f"cluster_{cluster_id}_imbalance_rewards"] = np.array([imbalance_rewards[i] for i in indices])
        cluster_data[f"cluster_{cluster_id}_index_size_rewards"] = np.array([index_size_rewards[i] for i in indices])
        cluster_data[f"cluster_{cluster_id}_grid_sizes"] = np.array([grid_sizes[i] for i in indices])
        cluster_data[f"cluster_{cluster_id}_centroid"] = centroids[cluster_id]
    
    np.savez_compressed(output_file, **cluster_data)
    print(f"All cluster data, centroids, and cluster count saved to {output_file}")

def cluster_and_save_to_single_file_with_centroids(states,reduced_states, actions, imbalance_rewards, index_size_rewards, grid_sizes, best_k_aic, output_file,pca):
    states_flat = np.vstack(reduced_states) 
    print(len(states),len(states[0]))
    print(len(reduced_states),len(reduced_states[0]))
    gmm = GaussianMixture(n_components=best_k_aic, random_state=42)
    gmm.fit(states_flat)
    cluster_labels = gmm.predict(states_flat) 

    clusters = {i: [] for i in range(best_k_aic)}  
    for idx, label in enumerate(cluster_labels):
        clusters[label].append(idx)


    centroids = gmm.means_
    centroids = pca.inverse_transform(centroids)

    save_all_clusters_with_centroids(output_file, clusters, states, actions, imbalance_rewards, index_size_rewards, grid_sizes, centroids, best_k_aic)


output_file = "train_data.npz" 
cluster_and_save_to_single_file_with_centroids(
    normalized_states,
    reduced_states,
    actions,
    imbalance_rewards,
    index_size_rewards,
    grid_sizes,
    best_k_aic,
    output_file,
    pca
)
