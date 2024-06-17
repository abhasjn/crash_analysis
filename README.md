## Car Crash Analysis - Project Structure

- `config/`: Configuration files for input and output paths.
- `data/`: Directory for input CSV files.
- `output/`: Directory for output CSV files.
- `car_crash_analysis/`: Source code for the analysis.
- `README.md`: Project documentation.
- `requirements.txt`: Python dependencies.

## Setup

1. Make a virtual environment and activate it (optional)
   ```bash
   python -m venv cc_env
   source cc_env/bin/activate
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
3. Specify JAVA_HOME path in input_config.
4. Make folder "data" which will have all the input files.
   5. To run the script and store the results in a folder, go to the main car_crash_analysis directory:

      a. Either run the script directly using:

         python car_crash_analysis/main.py
   
      b. Run the script as a cli command. Run with -h flag to see help options:
      
         python car_crash_analysis run -h
      
      Run the script like below for running a particular analysis only.

         python car_crash_analysis run -n 4
         python car_crash_analysis run -n 4,6,7
         python car_crash_analysis run -n all



## Consideration
The project has been completed using PySpark Dataframe APIs , however , for creating output CSVs , pandas was used.The output file currently has the results of all the Analysis.
