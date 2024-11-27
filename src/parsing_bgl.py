from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
import regex as re
import pandas as pd
from os.path import dirname

bgl_log_path = 'log/BGL.log' 
bgl_csv_path = 'csv/BGL.csv'
bgl_log_pattern = re.compile(
    r"(?P<Label>.*)\s"
    r"(?P<Timestamp>\d{10})\s"
    r"(?P<Date>\d{4}.\d{2}.\d{2})\s"
    r"(?P<Node>R\d{2}-M(1|0)-N\d-C:J\d{2}-U(0|1){2})\s"
    r"(?P<Time>\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6})\s"
    r"(?P<NodeRepeat>R\d{2}-M(1|0)-N\d-C:J\d{2}-U(0|1){2})\s"
    r"(?P<Type>RAS)\s"
    r"(?P<Component>(APP|BGLMASTER|CMCS|DISCOVERY|HARDWARE|KERNEL|LINKCARD|MMCS))\s"
    r"(?P<Level>(INFO|DEBUG|WARNING|FATAL|ERROR|SEVERE))\s"
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
                log_content = match.group('Level') + ' ' + match.group('Content')
                drain_parser.add_log_message(log_content)
    return drain_parser, total

def structureBGLData(drain_parser: TemplateMiner, log_pattern: re.Pattern, total: int, log_file_path):
    log_data = []
    event_templates=[]
    progress = 0
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
                log_content = level + ' ' +content
                result = drain_parser.match(log_content)

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
                    "Event": 'E'+str(result.cluster_id)
                }
                log_data.append(log_entry)
                event_templates.append({'Event':log_entry['Event'],'Template':log_entry['Template']})
                progress+=1
                print(f"{100*progress/total:2f}",'%')
    return log_data, event_templates


structures = {"BGL":structureBGLData}

def parse(log_pattern: re.Pattern, csv_path: str, log_file_path: str, dataname: str, config: TemplateMinerConfig):
    total = 0
    drain_parser = TemplateMiner(config=config)

    drain_parser, total = train(drain_parser, log_pattern, log_file_path)
    print('drain parser training done')

    structure = structures[dataname]
    log_data, event_templates = structure(drain_parser, log_pattern, total, log_file_path)
    print('data structuration done')

    print('saving event templates')
    dft = pd.DataFrame(event_templates)
    print(dft.columns, dft.head())
    filtered_df = dft.groupby("Event").first().sort_index()
    print(filtered_df.head(), filtered_df.shape)
    filtered_df.to_csv('csv/event_templates.csv')

    df = pd.DataFrame(log_data)
    print(df.head())
    print(df.shape)
    print('saving to csv')
    df.to_csv(csv_path)
    print('done saving to csv')
    return df

config = TemplateMinerConfig()
config.load("src/drain3.ini")

parse(bgl_log_pattern, bgl_csv_path, bgl_log_path, "BGL", config)