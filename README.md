# Table_Understanding_Pipeline

This project processes paleoclimate data files, extracts information, and applies a table understanding pipeline to identify data blocks.
Prerequisites

Python 3.10
Conda

##Setup##

Clone this repository:
Copygit clone https://github.com/your-username/table-understanding-pipeline.git
cd table-understanding-pipeline

Initialize Conda for your shell:
Copyconda init <your-shell-name>
Replace <your-shell-name> with bash, zsh, fish, or whatever shell you're using.
Close and reopen your terminal, or run the following command to reload your shell configuration:
Copysource ~/.bashrc  # for bash
           or
source ~/.zshrc   # for zsh

Create and activate the Conda environment:
Copyconda env create -f environment.yml
conda activate table-understanding

Install the required packages:
Copypip install -r requirements.txt


Usage

Prepare your input data:

Place your JSON file (containing dataset information) in the project root directory and name it data.json.


Run the main script:
Copypython main.py

Follow the prompts to provide any additional information (e.g., PSL files URL if needed).

Input
The script expects a data.json file in the root directory. This file should contain information about the dataset, including file URLs and variable names.
Output
The script will create an output directory containing:

CSV files of the processed data
JSON files with the results of the table understanding pipeline
Console output detailing the processing steps and accuracy of block detection

Accuracy Report
The script will print an accuracy report for each processed file, showing:

The detected blocks
The data series within each block
Whether the block was correctly identified (matching a single variable from the input data)

Check the console output or redirect it to a file for a detailed accuracy report.
Troubleshooting
If you encounter any issues:

Ensure all prerequisites are correctly installed.
Check that the data.json file is correctly formatted and contains the necessary information.
Verify that you have internet access for downloading additional required files.
Check the console output for any error messages or exceptions.
If you see the error "CondaError: Run 'conda init' before 'conda activate'", make sure you've followed steps 2 and 3 in the Setup section above.

For further assistance, please open an issue in the GitHub repository.
