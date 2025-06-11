from setuptools import find_packages, setup

setup(
    name="synapse_flow",
    packages=find_packages(exclude=["DatasetAutomation_Dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
