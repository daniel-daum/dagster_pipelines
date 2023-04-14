from setuptools import find_packages, setup

setup(
    name="data_pipelines",
    packages=find_packages(exclude=["data_pipelines_tests"]),
    install_requires=[
        "dagster",
        "dagster-postgres",
        "dagster-docker",
        "dagster-aws",
        "dagster-dbt",
        "dagster-pandas",
        "requests",
        "pandas",
        "boto3",
        "sqlalchemy",
        "psycopg2",
        "pydantic",
        "dbt-postgres",
        "dbt-core",
        "defusedxml",
        "python-dotenv",
    ],
    extras_require={
        "dev": [
            "dagit",
            "pytest",
            "pytest-sugar",
            "pytest-cov",
            "black",
            "isort",
            "mypy",
            "bandit",
            "safety",
        ]
    },
)
