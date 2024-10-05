# ETL-Songs: Analysis of Grammy Winners and Spotify Data

# Introduction

This project explores the relationship between Grammy Award winners and their performance on Spotify. Using historical Grammy Award data and the musical characteristics of the songs on Spotify, a detailed analysis is performed to identify trends and patterns in the award-winning music.

# Project Structure

The project structure is as follows:

- **notebooks/**: Contains tests, exploratory analysis, and visualizations generated during the development of the project.
- **data/**: Contains the CSV files of the Grammys and Spotify, organized into raw and processed subfolders for raw and transformed data, respectively.
- **src/**: Contains the main scripts for the ETL process (Extraction, Transformation, and Loading).
- **db_connection/**: Scripts for the connection to the PostgreSQL database.
- **extract_grammys/**: Scripts for the extraction of data from the PostgreSQL database and its visualization.
- **extract_spotify/**: Scripts to get data from the Spotify API and generate visualizations.
- **transform_grammys/**: Clean and fill missing artists in the Grammys data using the Spotify API.
- **transform_spotify/**: Clean up the Spotify CSV data.
- **merge/**: Merge the data from the PostgreSQL database and the Spotify CSV, complemented with information from the Spotify API.
- **load/**: Scripts to load the merged dataset to PostgreSQL.
- **load_drive/**: Scripts to upload the merged dataset to Google Drive.
- **dags/song_dag/**: Contains the Airflow DAG to orchestrate the ETL task flow.
- **.gitignore**: List of files and folders that should not be included in version control.
- **requirements.txt**: List of all dependencies required for the project.

# Requirements

Make sure you have the following requirements installed before running the project:

- Python 3.7 or higher
- PostgreSQL
- Airflow (for running DAGs)
- Google API Client Library (for accessing Google Drive)
- Spotipy (for interacting with the Spotify API)
- Virtual Environment Configuration

### Environment Configuration

1. Clone this repository:

```
git clone https://github.com/willyoung21/ETL-Songs.git
cd ETL-Songs.git
```

2. Create a virtual environment and activate it:
```
python -m venv env
source env/bin/activate # On Windows: env\Scripts\activate
```

3. Install the dependencies:
```
pip install -r requirements.txt
```

## Data Preparation

1. Download the CSV files of the awards Grammy and Spotify from the link provided:

- https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
- https://www.kaggle.com/datasets/unanimad/grammy-awards

2. Place the spotify csv in the data/raw folder

- **data/raw/**: Place the Spotify CSV here.
- **data/processed/**: The transformed files will be stored here.

3. Database Setup

Create a database in PostgreSQL: Create a database called grammys_db and a table called grammy_awards by running the following SQL:

```

CREATE TABLE grammy_awards (
year INT,
title TEXT,
published_at TIMESTAMP,
updated_at TIMESTAMP,
category TEXT,
nominee TEXT,
artist TEXT,
workers TEXT,
img TEXT,
winner BOOLEAN
);
```

4. Import the Grammys CSV into PostgreSQL:

Once the table is created, import the Grammys CSV into the grammy_awards table using the import option of your preferred PostgreSQL client (e.g. pgAdmin).

5. Configuring the .env file, you need to download the following library
```

pip install python-dotenv
```

Then create a .env file in the root of the project and add the following environment variables with your credentials:

```

DB_HOST=localhost
DB_PORT=5432
DB_USER=your_postgresql_user
DB_PASS=your_postgresql_password
DB_NAME=grammys_db

SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:3000
```

6. Configuring Credentials for Google Drive

Create a project in the Google Cloud Console and enable the Google Drive API.
Get the client_secret.json file and place it in a folder called credentials/ in the root of the project.
Setting Up Credentials for the Spotify API

7. Create a developer account in the Spotify Developer Dashboard and generate your client_id and client_secret.
Add the values ​​to your .env file as above.

# Running the Project

Follow the following order to run the project scripts:

1. make the connection to the database
```
python src/db_connection
```
2. extract the data from the table in the database
```
python src/extract_grammys
```
3. extract the data from the spotify csv
```
python src/extract_spotify
```
4. clean the grammys dataset and create a new spotify_id column
```
python src/transform_grammys
```
5. clean the spotify dataset
```
python src/transform_spotify
```
6. merge the two datasets and complete the information with the spotify api
```
python src/merge
```
7. upload the new table that came out of the merge to postgres
```
python src/load
```
8. upload the final merge to your google drive account
```
python src/load_drive
```

# Running the Task with Airflow

To run the DAG in Airflow, make sure that Airflow is correctly installed and configured. Modify the airflow.cfg file to set the DAG path, then start Airflow with the following commands: airflow standalone airflow scheduler Navigate to http://localhost:8080, log in with the default Airflow credentials, and run the DAG from the interface.

## Contributions Contributions are welcome. If you would like to contribute, follow these steps: Fork the project.
Create a new branch (git checkout -b feature/new-feature).
Make your changes and commit (git commit -am 'Add new functionality').
Upload your changes (git push origin feature/new-feature).
Open a Pull Request.

## License This project is licensed under the MIT License. See the LICENSE file for more information.

## Contact me
- **Name** : William Alejandro Botelo Florez
- **Email** : [william.botero@uao.edu.co]
- **Project Link**: [https://github.com/willyoung21/ETL-Songs.git]