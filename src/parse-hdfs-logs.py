import re
import pandas as pd
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

config = TemplateMinerConfig()
drain_parser = TemplateMiner(config=config)

log_pattern = re.compile(
    r"(?P<Date>\d{6})\s"
    r"(?P<Time>\d{6})\s"
    r"(?P<ThreadID>\d+)\s"
    r"(?P<Level>\w+)\s"
    r"(?P<Component>[^\:]+):\s"
    r"(?P<Content>.*)"
)


i = 0
with open('extracted_data/HDFS.log', 'r') as log_file:
    for line in log_file:
        line = line.strip()
        match = log_pattern.match(line)
        if match:
            i+=1
            print(i)
            log_content = match.group("Content")
            result = drain_parser.add_log_message(log_content)

log_data = []
with open('extracted_data/HDFS.log', 'r') as log_file:
    for line in log_file:
        match = log_pattern.match(line)
        if match:
            log_content = match.group("Content")
            date = match.group("Date")
            time = match.group("Time")
            threadID = match.group("ThreadID")
            level = match.group("Level")
            component = match.group("Component")
            
            result = drain_parser.match(log_content)

            log_entry = {
                "Date": date,
                "Time": time,
                "ThreadID": threadID,
                "Level": level,
                "Component": component,
                "Content": log_content,
                "Matched Template": result.get_template(),
                "Cluster ID": result.cluster_id
            }
            log_data.append(log_entry)

df = pd.DataFrame(log_data)
df.to_csv('parsed-hdfs-data.csv', index=False)
