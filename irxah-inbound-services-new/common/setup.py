from setuptools import setup, find_packages

# Exclude pytz,pysocpg2 package in wheel file. Only lambda function handlers will use the provided pytz.
# Glue jobs will use built in pytz. Reason is because Glue job will encounter an issue regarding
# timezone not being found, throwing an exception. Glue uses pg instead of psycopg2.
setup(
    name="irxah_common",
    version="0.1",
    setup_requires=["wheel"],
    packages=find_packages(
        exclude=(
            "pytz",
            "psycopg2",
        )
    ),
    install_requires=["pg8000"],
    python_requires=">=3.6",
    url="https://confluence.anthem.com/display/IBAH/IRx+BCI+Accumulator+Hub",
    description="Accumulator Hub - Common library of functions",
    author="Accum Hub Team",
    author_email="DL-IRXAccumsHubTeam@anthem.com",
)
