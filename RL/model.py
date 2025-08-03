import torch
import torch.nn as nn
import torch.nn.functional as F
from config import *

class Model(nn.Module):
    def __init__(self):
        super(Model, self).__init__()

        self.action_size = 1
        self.nCell_state_lat = nCell_state_lat
        self.nCell_state_lon = nCell_state_lon
        self.conv_layers = 5 
        self.cnn_channel = [1, 16, 32, 64, 128, 256] 
        self.cnn_kernel = [5, 3, 3, 3, 3]  
        self.cnn_stride = [1, 1, 1, 1, 1]
        self.cnn_padding = [2, 1, 1, 1, 1]  
        self.pool_kernel = [2, 2, 2, 2, 2]  
        self.pool_stride = [2, 2, 2, 2, 2]  
        self.fc_size = [1024, 512, 256, 128, 1]

        self.action_extension = 1024
        self.Conv2d = nn.ModuleList()
        self.BatchNorm2d = nn.ModuleList()
        self.MaxPool2d = nn.ModuleList()

        height, width = self.nCell_state_lat, self.nCell_state_lon
        for i in range(self.conv_layers):
            self.Conv2d.append(
                nn.Conv2d(self.cnn_channel[i], self.cnn_channel[i + 1], self.cnn_kernel[i], stride=self.cnn_stride[i],
                          padding=self.cnn_padding[i]))
            width = (width + 2 * self.cnn_padding[i] - self.cnn_kernel[i]) // self.cnn_stride[i] + 1
            height = (height + 2 * self.cnn_padding[i] - self.cnn_kernel[i]) // self.cnn_stride[i] + 1

            self.BatchNorm2d.append(nn.BatchNorm2d(self.cnn_channel[i + 1]))

            self.MaxPool2d.append(nn.MaxPool2d(self.pool_kernel[i], stride=self.pool_stride[i]))
            width = (width - self.pool_kernel[i]) // self.pool_stride[i] + 1
            height = (height - self.pool_kernel[i]) // self.pool_stride[i] + 1

        self.fc_size[0] = self.cnn_channel[-1] * width * height
        print(self.fc_size[0])
        
        self.fc_action = nn.Linear(self.action_size, self.action_extension)
        self.batchNorm_fc_action = nn.BatchNorm1d(self.action_extension)
        self.fc2 = nn.Linear(self.fc_size[0] + self.action_extension, self.fc_size[1])
        self.batchNorm_fc2 = nn.BatchNorm1d(self.fc_size[1])
        self.fc3 = nn.Linear(self.fc_size[1], self.fc_size[2])
        self.batchNorm_fc3 = nn.BatchNorm1d(self.fc_size[2])
        self.fc4 = nn.Linear(self.fc_size[2], self.fc_size[3])
        self.batchNorm_fc4 = nn.BatchNorm1d(self.fc_size[3])

        self.fc_final_reward = nn.Linear(self.fc_size[3], self.fc_size[4]) 
        self.fc_throughput_reward = nn.Linear(self.fc_size[3], self.fc_size[4]) 

        self.weight_initialization()

    def forward(self, state, action):
        assert len(state.size()) == 4  
        assert len(action.size()) == 3  
        for i in range(self.conv_layers):
            state = self.Conv2d[i](state)
            state = self.BatchNorm2d[i](state)
            state = F.relu(state)
            state = self.MaxPool2d[i](state)
        
        state = state.view(-1, self.fc_size[0]) 
        
        action = action.view(-1, self.action_size)
        action = self.fc_action(action)
        action = self.batchNorm_fc_action(action)
        action = F.relu(action)
        action = F.dropout(action, 0.2, self.training)

        # print(state.size())
        # print(action.size())
        concat = torch.cat((action, state), 1)
        concat = self.fc2(concat)
        concat = self.batchNorm_fc2(concat)
        concat = F.relu(concat)
        concat = F.dropout(concat, 0.2, self.training)

        concat = self.fc3(concat)
        concat = self.batchNorm_fc3(concat)
        concat = F.relu(concat)
        concat = F.dropout(concat, 0.2, self.training)

        concat = self.fc4(concat)
        concat = self.batchNorm_fc4(concat)
        concat = F.relu(concat)
        concat = F.dropout(concat, 0.2, self.training)


        final_reward = self.fc_final_reward(concat)
        throughput_reward = self.fc_throughput_reward(concat)
        return final_reward, throughput_reward

    def weight_initialization(self):
        for i in range(self.conv_layers):
            nn.init.kaiming_normal_(self.Conv2d[i].weight)
        nn.init.kaiming_normal_(self.fc_action.weight)
        nn.init.kaiming_normal_(self.fc2.weight)
        nn.init.kaiming_normal_(self.fc3.weight)
        nn.init.kaiming_normal_(self.fc4.weight)
        nn.init.kaiming_normal_(self.fc_final_reward.weight) 
        nn.init.kaiming_normal_(self.fc_throughput_reward.weight)  