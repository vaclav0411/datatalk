from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="docker-flow",
    infrastructure=github_block,
)


if __name__ == "__main__":
    docker_dep.apply()
