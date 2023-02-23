# cm_pipeline
Modular pipeline for testing and using an improved version of CM for generating well-connected clusters.

## Input
- The input to the pipeline script is a [param.config](param.config) file.
- Description of the config file keys can be found here [param_template.config](param_template.config) 

## Requirements [WIP]
- Create a python venv with necessary packages (runleiden, [CM](https://www.notion.so/Lab-Journal-2fcb00b0f77543fa932ff3cec650125f))

## Setup 
- Clone the cm_pipeline repository
- Edit the `network_name`, `output_dir`  and `resolution` values in `[default]` section of [param.config](param.config); and `input_file` under `[cleanup ]` section of the cloned repository (‘~’ is allowed for user home in the `output_dir` path and this directory need not exist)
- Edit [start_cm_pp.sh](start_cm_pp.sh) to point to the right venv and the cloned repository path of the cm_pipeline by giving the full path from user home or any other directory.)
- Any of the below methods can be used to start the pipeline

  ### Method 1
  1. edit the venv path and the cloned repository path for this repository in [start_cm_pp.sh](start_cm_pp.sh)
  2. Run `source start_cm_pp.sh` 

  ### Method 2
  1. Activate the venv which has the neccessary packages 
  2. python -m main param.config


## Setting the levels for logging
- cm pipeline logs the data on to console and file.
- Log levels for each of these can be modified in [log.config](./log.config)
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL [logging levels](https://docs.python.org/3/library/logging.html#logging-levels)
- Log files are created in `./logs` directory.

## Output Files [WIP]
- The commands executed during the workflow are captured in `./logs/executed-cmds/executed-cmds-timestamp.txt`
- The Output files generated during the workflow are stored in the folder `user-defined-output-dir/network_name-cm-pp-output-timestamp/`
- The descriptive analysis files can be found in the folder `user-defined-output-dir/network_name-cm-pp-output-timestamp/analysis` with the `*.csv` file for each of the resolution values.

## References
- [https://engineeringfordatascience.com/posts/python_logging/](https://engineeringfordatascience.com/posts/python_logging/)
- [https://docs.python.org/3/library/logging.config.html#logging-config-fileformat](https://docs.python.org/3/library/logging.config.html#logging-config-fileformat)


