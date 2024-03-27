# IPM File Parsing Project

## Overview

This project is a Python-based toolkit for parsing Mastercard IPM (Interbank Payment Message) files. It provides a comprehensive solution for processing financial transaction data, offering functionalities for data extraction, validation, and database insertion. The suite is designed to automate the handling of complex financial datasets, enhancing efficiency and accuracy in financial data processing workflows.

## Features

- **Data Parsing**: Extracts data from Mastercard IPM files for further processing.
- **Database Interaction**: Facilitates data insertion into SQL databases for persistent storage and analysis.
- **Error Handling**: Robust logging and error handling mechanisms ensure reliable process execution.
- **Multiprocessing Support**: Utilizes Python's multiprocessing capabilities for improved processing speed.
- **Configurable Setup**: Customizable settings via `SetUp.py` to match the processing environment's requirements.

## Prerequisites

- Python 3.6 or newer
- pyodbc
- pandas
- numpy
- sqlalchemy (for database interaction)
- A configured SQL Server database

## Setup

1. Clone the repository to your local machine.
2. Ensure Python 3.6+ is installed and accessible.
3. Install necessary Python packages:

```bash
pip install pyodbc pandas numpy sqlalchemy

Configure database and processing parameters in SetUp.py to match your environment.
Usage
Ensure IPM files are placed in the input directory specified in SetUp.py.
Execute the main parsing script:
python IPM_Main.py


Monitor the output and log files for process status and any potential errors.
Contributing
Contributions to enhance the project are welcome. Please feel free to submit issues for bugs or suggestions and pull requests for code additions or fixes.

