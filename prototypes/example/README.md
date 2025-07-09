# Example prototype

This prototype allows the user to look add different tables of the OCDM.

## Setup

Follow the setup instruction for the prototyping environment no additional dependencies or steps are needed.

## Usage

1. set env variables for Team and Application key

This prototype can load Celonis data sets for this it requires the `EMS_APP_KEY` and `EMS_BASE_URL` to be set as environment variables
For this praktikum you can use the following APP_KEY and BASE_URL:
Setting `EMS_APP_KEY` can be done via:
```commandline
export EMS_APP_KEY="YTM1MGM1MDgtMTBmYy00ZjEwLTg1YTAtNDQ4MTNjNzFhMjc3OitqRW5TR3l3RkViZFdUZUNjbTc1OVkzQURHTWQ3QlZPZkZFSThqMlpGRGk4"
export EMS_BASE_URL="rwth-celonis-lab.try.celonis.cloud"
```
or for Windows via:
```commandline
$env:EMS_APP_KEY="YTM1MGM1MDgtMTBmYy00ZjEwLTg1YTAtNDQ4MTNjNzFhMjc3OitqRW5TR3l3RkViZFdUZUNjbTc1OVkzQURHTWQ3QlZPZkZFSThqMlpGRGk4"
$env:EMS_BASE_URL="rwth-celonis-lab.try.celonis.cloud"
poetry run python -m streamlit run prototypes/example/main.py
```

2. start the streamlit application via
```commandline
poetry run python -m streamlit run prototypes/example/main.py
```
or via

```commandline
poetry run python run prototypes/example/bootstrap.py
```
The bootstrap.py file can also be executed using the play button within IntelliJ.
3. select a data pool
4. select a data model
5. click on the execute prototype button
