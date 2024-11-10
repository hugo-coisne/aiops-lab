from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
import regex as re
import pandas as pd

config = TemplateMinerConfig()
drain_parser = TemplateMiner(config=config)

log_pattern = re.compile(
    r".*\s"
    r"\d{10}\s"
    r"(?P<Date>\d{4}.\d{2}.\d{2})\s"
    r"(?P<Node>R\d{2}-M(1|0)-N\d-C:J\d{2}-U(0|1){2})\s"
    r"(?P<FullTimestamp>\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6})\s"
    r"(?P<NodeRepetition>R\d{2}-M(1|0)-N\w-C:J\d{2}-U(0|1){2})\s"
    r"RAS\s"
    r"(?P<Component>(APP|BGLMASTER|CMCS|DISCOVERY|HARDWARE|KERNEL|LINKCARD|MMCS))\s"
    r"(?P<Level>(INFO|DEBUG|WARNING|FATAL|ERROR|SEVERE))\s"
    r"(?P<Content>.*)"
)

linecount=0
limit = 10000000


with open('log/BGL.log', 'r', encoding='utf-8') as log_file:
    for line in log_file:
        line = line.strip()
        match = log_pattern.match(line)
        if match:
            linecount+=1
            if linecount > limit:
                break
            log_content = match.group("Content")
            result = drain_parser.add_log_message(log_content)
print('first parsing done')

log_data = []
with open('log/BGL.log', 'r', encoding='utf-8') as log_file:
    for log in log_file:
        match = log_pattern.match(log.strip())
        if match:
            node = match.group("Node")
            full_timestamp = match.group("FullTimestamp")
            component = match.group("Component")
            level = match.group("Level")
            content = match.group("Content")

            log_entry = {
                "Node": node,
                "FullTimestamp": full_timestamp,
                "Component": component,
                "Level": level,
                "Content": content,
                "Matched Template": result.get_template(),
                "Cluster ID": result.cluster_id
            }

            log_data.append(log_entry)


df = pd.DataFrame(log_data)
print(df.head())
df.to_csv('csv/BGL.csv')
print('done')
