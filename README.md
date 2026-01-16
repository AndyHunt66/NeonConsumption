# Neon Consumption graph generator


This project takes the consumption data from Neon's project consumption API, pushes it to a postgresql database for historical reporting and provides a small Flask web app to display the data graphically.


### Getting started
- Get your Neon API key (instructions here: https://neon.com/docs/manage/api-keys)
Use an Organization-scoped key.
- Set up a Postgresql database somewhere and get the connection string.
    Use the sql in sql/createTables.sql to set up the tables
- Add the API token and connection string to neonToPostgres.py (or add them to your environment variables)
- Run `neonToPostgres.py` to pull the data from the Neon API into your database.
- Run `flask app.py` to run the web app
- Browse to http://localhost:5000
