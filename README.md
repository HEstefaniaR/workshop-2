**Made by:** Estefanía Hernández Rojas

# ETL Pipeline

![](adjuntos/ETL%20Pipeline%20Diagram.png)
The goal of this project is to build an ETL pipeline that extracts data from a CSV file and a database, transforms both datasets, merges them into a dimensional model, loads it into a Data Warehouse and stores it in Google Drive, and finally, using the Data Warehouse, creates a dashboard.

## Pipeline Steps

1. **Extract**:
   - **Sources:** CSV file for the Spotify dataset and a local MySQL database for the Grammy dataset.
2. **Transform**:
   - Clean, normalize, and remove irrelevant data using Python & Pandas.
   - Separate tables into dimensions and intermediate tables for future merging.
3. **Merge**:
   - Create the fact table using the merged data from both datasets.
   - Create primary and foreign keys to link dimensions to the fact table.
4. **Load**:
   - Upload the data to Google Drive in CSV format.
   - Load the data into a MySQL container as a Data Warehouse for later visualization.

# Star Schema

The Star Schema design is as follows:

- **Fact Table:** `award_fact` (contains `track_id` if the track won, `artist_id` if the artist won, and `grammy_event_id`)
- **Dimensions:**
  - `grammy_event_dim` (year, title, nominated category, published_at, updated_at)
  - `artist_dim` (artist name)
  - `track_dim` (track_name, album_name, duration_ms, explicit, danceability, energy, key, loudness, mode, speechiness, instrumentalness, liveness, valence, tempo, time_signature)
  - `genre_dim` (genre name)
- **Bridge Tables:** many-to-many relationships between track and artist, and track and genre

This design allows efficient joins between the fact table and dimensions, enabling comprehensive analysis from multiple perspectives. The model combines Spotify musical information with historical Grammy results.

# Airflow DAG Design


# Project Structure

```
.
├── EDA.ipynb
├── README.md
├── config
├── dags
│   └── dag_etl.py
├── diagrams
├── docker-compose.yaml
├── init
│   ├── init.py
│   └── init_oauth.py
├── logs
├── metabase-data
├── plugins
├── processed_data
├── raw_data
├── requirements.txt
├── scripts
│   ├── extract.py
│   ├── load.py
│   ├── merge.py
│   └── transform.py
└── visualizations
```


- `EDA.ipynb`: contains the Exploratory Data Analysis before the pipeline.
- `raw_data/`: contains the initial CSV files.
- `config/`, `logs/`, `plugins/`, and `dags/`: volumes used by Apache Airflow.
  - `dags/` contains the ETL DAG that runs automatically when the Docker container starts.
- `scripts/`: contains the functions for each ETL task.
- `init/`: contains scripts to run before starting the Docker containers.
- `requirements.txt`: Python environment dependencies.
- `docker-compose.yaml`: configuration for Airflow, Metabase, and MySQL containers.
- `metabase-data/`: volume storing the Metabase database and dashboard.
- `visualizations/`: contains images and PDF of the Metabase dashboard.
- `processed_data/`: volume storing the merged datasets.

# Key Decisions

## EDA

**Spotify DB:**

- **Completeness:** one row contained three null values (`artist`, `album_name`, `track_name`) and was removed.
- **Uniqueness:** `track_id` duplicates exist because tracks can have multiple genres; this is handled via many-to-many relationships in the model.
- **Consistency:** no inconsistencies between duplicated `track_id`s.
- **Validation:** all variable ranges are correct except for `tempo` and `time_signature` values of 0 (<1.5% of data). One row with duration=0 was removed.
- **Accuracy:** genre names and track/artist names are accurate.
- **Timeliness:** not applicable (no date fields to evaluate).

**Grammy DB:**

- **Completeness:** nulls found in `nominee` (6, 0.12%), `artist` (1840, 38.25%), `workers` (2190, 45.53%), and `img` (1367, 28.42%). Six rows with mostly null values were removed.
- **Uniqueness:** no duplicate rows when considering year, category, and nominee.
- **Consistency:** minor variations in category names normalized.
- **Validation:** `published_at` and `updated_at` invalid formats fixed (4291 and 108 records affected).
- **Accuracy:** 3810 records have `published_at` and `updated_at` not matching the event year; acceptable for analysis.
- **Timeliness:** data spans 1958–2019; newer records not included.

**General Notes:**

- `Spotify DB`: `Unnamed: 0` column irrelevant (index only).
- `Grammy DB`: `img`, `workers`, and `winner` columns removed. `winner` had all ones, as all rows represent winners.

## Data Merging

1. Categories classified as track-related or artist-related using keywords to match nominees efficiently.
   - Artist categories → match `artist_dim`, `track_id` set to null
   - Track categories → match `track_dim`, `artist_id` set to null
2. Fact table (`award_fact`) created using valid matches; rows without winners or invalid events removed. Only `winner_artist_id` or `winner_track_id` populated.
3. Non-winning tracks, artists, and genres retained for comparative analysis.

# How to Run the Project

1. **Clone the repository:**

```bash
git clone https://github.com/HEstefaniaR/workshop-2
cd workshop-2
```

2. **Create a virtual environment:**

```
python -m venv env
source env/bin/activate   # Mac/Linux
env\Scripts\activate      # Windows
```

3. **Install dependencies:**

```
pip install -r requirements.txt
```

4. **Start the grammys_db database:** Make sure the variables in init/init.py match your local MySQL.

```
config = {
    "user": "root",
    "password": "root",
    "host": "localhost",
    "port": 3306
}
```

Create the database and tables:

```
python init/init.py
```

5. **Download the Google Drive OAuth client ID JSON** and move it to ../workshop-2
   Follow the guide: [Google Drive OAuth setup](https://help.qlik.com/talend/en-US/components/8.0/google-drive/how-to-access-google-drive-using-client-secret-json-file-the)
   Name the file client_secret.json and include:

```json
{"web":{"client_id":"REPLACE_ME",
"project_id":"REPLACE_ME",
"auth_uri":"https://accounts.google.com/o/oauth2/auth",
"token_uri":"https://oauth2.googleapis.com/token",
"auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
"client_secret":"REPLACE_ME",
"redirect_uris":["http://localhost:8090/","http://localhost:8090/"],
"javascript_origins":["http://localhost:8080","http://127.0.0.1:8080"]}}
```

Then create the access token:

```
python init/init_oauth.py
```

6. **Open Docker Desktop and start the Airflow containers:**

```
docker-compose up -d
```

7. **Access Airflow to monitor DAGs:**
   http://localhost:8080
8. **When all tasks finish, open Metabase:**
   http://localhost:3000/dashboard/2?year=
   Credentials:

   - email: example@gmail.com
   - password: root1234
     You can interact with the dashboard and change the year filter.

   You can also check the MySQL server connection in the container:

   http://localhost:3000/admin/databases/
