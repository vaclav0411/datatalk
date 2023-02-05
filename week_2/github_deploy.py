from pathlib import Path
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.filesystems import GitHub
from prefect.flows import load_flow_from_script

BASE_DIR = Path(__file__).resolve().parent
to_path = BASE_DIR.joinpath('github_homework')

github_block = GitHub.load("zoom")
github_block.get_directory(from_path='week_2/', local_path=to_path)


docker_dep = Deployment.build_from_flow(
    flow=load_flow_from_script(path=f'{to_path}\week_2\etl_web_to_gcs 2.py', flow_name='etl_web_to_gcs'),
    name="github-flow",
)


if __name__ == "__main__":
    docker_dep.apply()
