from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
import regex as re
import pandas as pd

config = TemplateMinerConfig()

bgl_log_path = '../log/BGL.log' 
bgl_csv_path = '../csv/BGL.csv'
bgl_log_pattern = re.compile(
    r"(?P<Label>.*)\s"
    r"(?P<Timestamp>\d{10})\s"
    r"(?P<Date>\d{4}.\d{2}.\d{2})\s"
    r"(?P<Node>R\d{2}-M(1|0)-N\d-C:J\d{2}-U(0|1){2})\s"
    r"(?P<Time>\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6})\s"
    r"(?P<NodeRepeat>R\d{2}-M(1|0)-N\d-C:J\d{2}-U(0|1){2})\s"
    r"(?P<Type>RAS)\s"
    r"(?P<Component>(APP|BGLMASTER|CMCS|DISCOVERY|HARDWARE|KERNEL|LINKCARD|MMCS))\s"
    r"(?P<Level>(INFO|DEBUG|WARNING|FATAL|ERROR|SEVERE))\s" # could use .* for Component and Level but execution time would be too long
    r"(?P<Content>.*)"
)

def train(drain_parser, log_pattern, log_file_path):
    total = 0
    with open(log_file_path, 'r', encoding='utf-8') as log_file:
        for line in log_file:
            line = line.strip()
            match = log_pattern.match(line)
            if match:
                total+=1
                print(total)
                log_content = match.group("Content")
                drain_parser.add_log_message(log_content)
    return drain_parser, total

def structureBGLData(drain_parser: TemplateMiner, log_pattern: re.Pattern, total: int, log_file_path):
    log_data = []
    with open(log_file_path, 'r', encoding='utf-8') as log_file:
        for log in log_file:
            match = log_pattern.match(log.strip())
            if match:
                label = match.group("Label")
                timestamp = match.group("Timestamp")
                date = match.group("Date")
                node = match.group("Node")
                time = match.group("Time")
                noderepeat = match.group("NodeRepeat")
                type = match.group("Type")
                component = match.group("Component")
                level = match.group("Level")
                content = match.group("Content")

                result = drain_parser.match(content)

                log_entry = {
                    "Label": label,
                    "Timestamp": timestamp,
                    "Date": date,
                    "Node": node,
                    "Time": time,
                    "NodeRepeat": noderepeat,
                    "Type":type,
                    "Component": component,
                    "Level": level,
                    "Content": content,
                    "Template": result.get_template(),
                    "Event ID": result.cluster_id
                }

                log_data.append(log_entry)
                print(100*len(log_data)/total,'%')
    return log_data


structures = {"BGL":structureBGLData}

def parse(log_pattern: re.Pattern, csv_path: str, log_file_path: str, dataname: str):
    total = 0
    drain_parser = TemplateMiner(config=config)

    drain_parser, total = train(drain_parser, log_pattern, log_file_path)

    print('drain parser training done')

    structure = structures[dataname]
    log_data = structure(drain_parser, log_pattern, total, log_file_path)
    print('data structuration done')

    df = pd.DataFrame(log_data)
    print(df.head())
    print(df.shape)
    print('saving to csv')
    df.to_csv(csv_path)
    print('done saving to csv')
    return df