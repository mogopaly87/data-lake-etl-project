<h3>PURPOSE</h3>
<br>

<p>The purpose of this database is to help the startup, Sparkify to 
analyze the data they have been collecting on songs and user activities from their new music
streaming application<br>
This project provides a database where Sparkify's analytics team can easily query the data
they need.</p>
<br><br>

<h3>HOW TO RUN PYTHON SCRIPTS</h3><br>

<ul>
  <li>First thing is to drop any existing tables and databases, and create new ones by running the   `create_tables.py` script</li>
  <li>Next, run the `etl.py` script to create a data pipeline</li>
  <li>Run the `test.ipynb` to test and confirm your data pipline was successfully created.</li>
</ul>
<br><br>

<h3>EXPLANATION OF THE FILES IN THE REPOSITORY</h3><br>

<ul>
  <li>`test.ipynb`: used to test to confirm the state of your database tables</li>
  <li>`create_tables.py`: used to drop and create tables.  You run this file to reset your tables before each time you run your ETL scripts.</li>
  <li>`etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.</li>
  <li>`etl.py` reads and processes files from song_data and log_data and loads them into your tables.</li>
  <li>`sql_queries.py` contains all your sql queries, and is imported into test.ipynb, create tables.py, and etl.py scripts</li>
  <li>`README.md` contains instructions on how to use this software.</li>
</ul>
<br><br>

<h3>JUSTIFICATION FOR DATABASE SCHEMA DESGIN AND ETL PIPELINE </h3>
<br>

<ol>
  <li><strong>Star Schema was used</strong></li>
  <p>I used a star schema because, since the Sparkify analytics team will be interesting in
  understanding users, we needed to provide various dimensions</p>
  <li><strong>The ETL pipeline used was in batch </strong></li>
  <p>A batch-style ETL was chosen because their files exist in JSON logs.</p>
</ol>

<h3>USAGE</h3>
<br>
<p>To begin, set the following environment variables on your EMR cluster master node:</p>
<ol>
    <li>INPUT_SONG_DATA for the s3 bucket and subfolder containing song data</li>
    <li>INPUT_LOG_DATA for the s3 bucket and subfolder containing log data</li>
    <li>OUTPUT_DATA_DIR for the s3 bucket and [optionally, subfolder] where processed parquet files will be stored</li>
</ol>
<p>
The 'test_notebook.ipynb' file is used to test-run all the requirements. All tests are done on an EMR cluster master node. Once tests are completed, the 'etl.py' script is copied to EMR cluster master node using the command: <br>
<strong>scp -i your_key_pair.pem etl.py hadoop@ec2-XX-XXX-XX-XXX.compute-1.amazonaws.com:/home/hadoop/</strong>
</p>

<p>Now you have the 'etl.py' script on your EMR cluster master node and you have the necessary environment variables set, run the script using the command below: <br>
<strong>spark-submit etl.py</strong>
</p><br>

<p>Check your output S3 bucket to confirm parquet files have been written.</p>
