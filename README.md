Janrain Constant Contact Export App
===================================

Python app to export records from Capture to Constant Contact.


Local Setup
-----------


##### Setup virtual environment
    $ virtualenv venv
    $ source venv/bin/activate

##### Install requirements
    $ pip install -r requirements.txt

##### Configure environment
    $ cp env_example.sh env.sh
    $ source env.sh

##### Start the development webserver
    $ python constant_contact

##### Make it go!
    $ curl http://localhost:5000/sync -d ''


Configuration
-------------

These environment variables determine the app's configuration:

* `DEBUG`: set to TRUE to enable debug mode (default = FALSE)
* `APP_LOG_FILE`: location of the app's log file (required)
* `APP_LOG_FILESIZE`: maximum size the log file will grow to before being rotated (default = 10MB)
* `APP_LOG_NUM_BACKUPS`: number of backups of the log that will be kept (default = 20)
* `JANRAIN_URI`: URI of the Capture app to pull records from (required)
* `JANRAIN_CLIENT_ID`: client_id to use when pulling records (required)
* `JANRAIN_CLIENT_SECRET`: client_secret for the client_id (required)
* `JANRAIN_SCHEMA`: name of Capture schema (entity_type) containing the records (default = `user`)
* `JANRAIN_BATCH_SIZE`: maximum number of records to fetch at a time from Capture - higher = faster export, but if too large, the export may time out (default = 1000)
* `JANRAIN_ATTRIBUTES`: list of attributes (comma-separated) to fetch for each record, limit this to only the attributes needed for maximum efficiency (default = all attributes)
