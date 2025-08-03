state_topic = "StateTopic"
subtask_topic = "TaskCountTopic"
producer_topic = 'GridSizeTopic'
kafka_servers = []

MIN_nCell = 10
MAX_nCell = 400
K = 20
EPSILON = 0.1

lr = 0.0005
online_steps = 16
batch_size = 32


nCell_state_lat = 64
nCell_state_lon = 64
ADAM_WEIGHT_DECAY = 0
MAX_MEMORY_SIZE = 160

LAMBDA = 0.5

PARALLELISM = 96
TIMEGAP = 900000
train_data_path = "./train_data.npz"