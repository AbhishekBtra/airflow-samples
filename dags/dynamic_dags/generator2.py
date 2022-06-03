from re import template
from jinja2 import Environment,FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template('template2.jinja2')

for filename in os.listdir(file_dir):
    if filename.startswith("template") and filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}","r") as config_file:
            config = yaml.safe_load(config_file)

            for i in range(len(config['tables'])):

                with open(f"dags/job_{config['tables'][i]['dag_id']}.py","w") as f:
                    f.write(template.render(config['tables'][i]))

