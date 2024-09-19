import sys
import pandas

import json
import requests
import os
import io
import subprocess
import yaml
from typing import List, Tuple, Optional
import zipfile
import sys
import platform
import traceback
import pandas as pd
from conda.cli.python_api import run_command, Commands
from conda.exceptions import EnvironmentLocationNotFound

def download_file(url: str, filename: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            f.write(response.content)
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise

def load_dataset_info(json_file_path: str) -> Tuple[float, float, List[str], List[List[str]]]:
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        study = data['study'][0]

        lat = study['site'][0]['geo']['geometry']['coordinates'][1]
        lon = study['site'][0]['geo']['geometry']['coordinates'][0]

        file_urls = []
        variables_list = []

        for site in study['site']:
            for paleo_data in site['paleoData']:
                # Access the first element within 'dataFile'
                data_file = paleo_data['dataFile'][0]

                file_urls.append(data_file['fileUrl'])

                # Access 'variables' within the first element of 'dataFile'
                variables_list.append([v.get('variableName', None) for v in data_file.get('variables', [])])

        return lat, lon, file_urls, variables_list

    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Error loading dataset info: {e}")
        raise

import requests
import io

def download_and_process_data(file_url: str, variables: Optional[list[str]] = None) -> pandas.DataFrame:
    try:
        response = requests.get(file_url)
        response.raise_for_status()
        content = response.text

        # Find the start of the data section
        data_start = content.find('Data:')
        if data_start == -1:
            raise ValueError("Data section not found in the file")

        # Extract the data section
        data_section = content[data_start + 5:].strip()  # Skip 'Data:' and any leading/trailing whitespace

        # Split the data section into lines
        data_lines = data_section.splitlines()

        # Find the first non-comment line (assuming it contains the header)
        header_line = next((line for line in data_lines if not line.startswith('#')), None)

        if header_line:
            # Remove comment character if present and split into column names
            column_names = [col.replace('#', '').strip() for col in header_line.split()]
            # Filter out empty column names
            column_names = [col for col in column_names if col]

            # Read the data, skipping the first row (header) if it's duplicated
            df = pandas.read_csv(io.StringIO('\n'.join(data_lines)), delim_whitespace=True, comment='#', header=0, names=column_names)
            if df.iloc[0].equals(pandas.Series(column_names)):  # Check if first row is the header
                df = df.iloc[1:].copy()  # Skip the first row if it's a duplicate header
        else:
            # If no header line is found, read the data without column names
            df = pandas.read_csv(io.StringIO(data_section), delim_whitespace=True, comment='#', header=None)

        if df.empty:
            raise ValueError("No data was parsed from the file")

        # If variables are provided and match the number of columns, use them
        if variables and len(variables) == df.shape[1]:
            df.columns = variables

        print(f"Successfully processed data from {file_url}. Shape: {df.shape}")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error downloading data from {file_url}: {e}")
    except ValueError as e:
        print(f"Error processing data from {file_url}: {e}")
    except Exception as e:
        print(f"Unexpected error processing data from {file_url}: {e}")

    return pandas.DataFrame()

# Example usage:
urls = [
    "https://www.ncei.noaa.gov/pub/data/paleo/paleolimnology/northamerica/usa/colorado/blue2019dust-coreb.txt",
    "https://www.ncei.noaa.gov/pub/data/paleo/paleolimnology/northamerica/usa/colorado/blue2019dmar-ens.txt",
    "https://www.ncei.noaa.gov/pub/data/paleo/paleolimnology/northamerica/usa/colorado/blue2019dmar.txt",
    "https://www.ncei.noaa.gov/pub/data/paleo/paleolimnology/northamerica/usa/colorado/blue2019dust-corea.txt"
]

for url in urls:
    print(f"Processing file: {url}")
    # You can provide variables if you know them, or leave it as None
    variables = None  # or [list of variable names if known]
    df = download_and_process_data(url, variables)
    if not df.empty:
        print(df.head())
        print(f"Columns: {df.columns.tolist()}")
    print("\n")

import subprocess

def setup_conda_environment():
    env_name = "table-understanding"

    # Create the environment
    create_env_cmd = ["conda", "create", "-n", env_name, "python=3.10", "-y"]
    result = subprocess.run(create_env_cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print("Environment created successfully.")
    else:
        print(f"Error creating environment: {result.stderr}")
        return

    # Install packages
    install_cmd = ["conda", "run", "-n", env_name, "pip", "install", "pandas", "requests", "pyyaml"]
    result = subprocess.run(install_cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print("Packages installed successfully.")
    else:
        print(f"Error installing packages: {result.stderr}")

setup_conda_environment()

def run_in_conda_env(env_name: str, command: str):
    try:
        result = run_command(Commands.RUN, ["-n", env_name] + command.split())
        return result
    except Exception as e:
        print(f"Error running command in conda environment: {e}")
        raise

def setup_isi_table_understanding(env_name: str, skip_psl_download: bool = False):
    try:
        if os.path.exists("isi-table-understanding"):
            print("isi-table-understanding directory already exists. Updating...")
            os.chdir("isi-table-understanding")
            subprocess.run(["git", "pull"], check=True)
        else:
            subprocess.run(["git", "clone", "-b", "impl", "https://github.com/usc-isi-i2/isi-table-understanding.git"], check=True)
            os.chdir("isi-table-understanding")
        if os.path.exists("requirements.txt"):
            run_in_conda_env(env_name, "pip install -r requirements.txt")
        else:
            print("requirements.txt not found. Skipping package installation.")
        if not skip_psl_download:
            os.makedirs("data", exist_ok=True)
            psl_url = input("Please enter the URL for the PSL files zip (or press Enter to skip): ")
            if psl_url:
                try:
                    download_file(psl_url, "data/psl_files.zip")
                    with zipfile.ZipFile("data/psl_files.zip", 'r') as zip_ref:
                        zip_ref.extractall("data/")
                    print("PSL files downloaded and extracted successfully.")
                except requests.exceptions.RequestException as e:
                    print(f"Failed to download PSL files: {e}")
                    print("Continuing without PSL files. Some functionality may be limited.")
            else:
                print("Skipping PSL files download.")
        else:
            print("Skipping PSL files download.")
        if not os.path.exists("InferSent"):
            subprocess.run(["git", "clone", "https://github.com/facebookresearch/InferSent.git"], check=True)
        os.chdir("InferSent")
        os.makedirs("GloVe", exist_ok=True)
        glove_url = "http://nlp.stanford.edu/data/glove.840B.300d.zip"
        download_file(glove_url, "GloVe/glove.840B.300d.zip")
        with zipfile.ZipFile("GloVe/glove.840B.300d.zip", 'r') as zip_ref:
            zip_ref.extractall("GloVe/")
        os.makedirs("encoder", exist_ok=True)
        download_file("https://dl.fbaipublicfiles.com/infersent/infersent1.pkl", "encoder/infersent1.pkl")
        download_file("https://dl.fbaipublicfiles.com/infersent/infersent2.pkl", "encoder/infersent2.pkl")
        os.chdir("..")
        psl_config = {
            "psl": {
                "model_path": os.path.abspath("path/to/pretrained/psl/model"),
                "infersent_path": os.path.abspath("InferSent"),
                "glove_path": os.path.abspath("InferSent/GloVe/glove.840B.300d.txt")
            }
        }
        os.makedirs("cfg", exist_ok=True)
        with open("cfg/psl_config.yaml", "w") as f:
            yaml.dump(psl_config, f)
        print("ISI Table Understanding setup completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error in setup: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error in setup: {e}")
        raise

def run_table_understanding(env_name: str, input_file: str, output_dir: str):
    try:
        config_file = "cfg/psl_config.yaml"
        files_config = f"- {input_file}"

        with open("cfg/files.yaml", "w") as f:
            f.write(files_config)

        result = run_in_conda_env(env_name, f"python main.py --config {config_file} --files cfg/files.yaml --output {output_dir}")
        return result
    except Exception as e:
        print(f"Error running table understanding: {e}")
        raise

def main(skip_psl_download: bool = False):
    try:
        json_file_path = 'data.json'
        output_dir = os.path.abspath('./output')

        print(f"Current working directory: {os.getcwd()}")
        print(f"Attempting to create output directory: {output_dir}")

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory created/confirmed: {output_dir}")
        print(f"Output directory exists: {os.path.exists(output_dir)}")
        print(f"Output directory is writable: {os.access(output_dir, os.W_OK)}")

        lat, lon, file_urls, variables_list = load_dataset_info(json_file_path)

        print(f"Latitude: {lat}, Longitude: {lon}")

        env_name = setup_conda_environment()
        setup_isi_table_understanding(env_name, skip_psl_download)

        results = []
        for file_url, variables in zip(file_urls, variables_list):
            print(f"Processing file: {file_url}")
            print(f"Variables: {variables}")

            # If variables are all None, set it to None to let download_and_process_data handle it
            if all(var is None for var in variables):
                variables = None

            df = download_and_process_data(file_url, variables)
            if df.empty:
                print(f"Skipping empty dataframe for {file_url}")
                continue

            print("DataFrame head:")
            print(df.head())

            csv_file = os.path.join(output_dir, f"{os.path.basename(file_url)}.csv")
            print(f"Attempting to save file: {csv_file}")
            try:
                df.to_csv(csv_file, index=False)
                print(f"Saved data to {csv_file}")
            except Exception as e:
                print(f"Error saving data to {csv_file}: {e}")
                print(f"File path exists: {os.path.exists(os.path.dirname(csv_file))}")
                print(f"File path is writable: {os.access(os.path.dirname(csv_file), os.W_OK)}")
                continue

            try:
                table_understanding_result = run_table_understanding(env_name, csv_file, output_dir)

                # Load table understanding output
                with open(os.path.join(output_dir, "table-understanding-results.json"), "r") as f:
                    table_blocks = json.load(f)

                # Evaluate block identification
                for block in table_blocks:
                    block_columns = block["col_headers"]
                    block_data = block["cells"]

                    # Check if all columns in a block correspond to a single variable
                    is_correct = any(all(col in var for col in block_columns) for var in variables)

                    print(f"Block: {block_columns}")
                    print(f"Data Series:")
                    for row in block_data:
                        print(row)
                    print(f"Correctly identified: {is_correct}\n")

            except Exception as e:
                print(f"Error running table understanding or evaluating results: {e}")
                table_understanding_result = None

            results.append({
                'file_url': file_url,
                'dataframe': df,
                'table_understanding_result': table_understanding_result
            })

            print("\n")

        print("Processing complete. Check the output directory for results.")
        return results

    except Exception as e:
        print(f"An error occurred in main: {e}")
        traceback.print_exc()
        return None

# Run the main function and display results
results = main(skip_psl_download=True)
if results:
    for result in results:
        print(f"File: {result['file_url']}")
        print("DataFrame head:")
        display(result['dataframe'].head())
        print("Table Understanding Result:")
        print(result['table_understanding_result'])
        print("\n")
else:
    print("No results were returned. Check the error messages above for more information.")
