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

Required:
* `APP_LOG_FILE`: location of the app's log file
* `JANRAIN_URI`: URI of the Capture app to pull records from 
* `JANRAIN_CLIENT_ID`: client_id to use when pulling records
* `JANRAIN_CLIENT_SECRET`: client_secret for the client_id 
* `JANRAIN_CC_ATTRIBUTE_MAPPING`: json blob representing the mapping from janrain attribute to constant contact attribute
example below.
```
'{"primaryAddress.country":"personal_address.country_code","givenName":"first_name","familyName":"last_name","primaryAddress.zip":"personal_address.postal_code","primaryAddress.stateAbbreviation":"personal_address.state_code","primaryAddress.zipPlus4":"personal_address.sub_postal_code","primaryAddress.mobile":"cell_phone","primaryAddress.city":"personal_address.city","primaryAddress.address2":"personal_address.line2","primaryAddress.address1":"personal_address.line1"}'
```
* `JANRAIN_CC_CUSTOM_FIELD_MAPPING`: json blob representing the mapping from janrain attribute to constant constact 
custome field. the custome field is indexed by 1-10. Example below
```
'{"customfield1":"1"}'
```
* `AWS_SQS_QUEUE_NAME`: queue name of aws sqs queue
* `AWS_REGION`: aws region for app to run in
* `CC_API_KEY`: api key for constant contact
* `CC_ACCESS_TOKEN`: access token for constant contact
* `CC_LIST_IDS`: comma separated list of cc list ids to which users will be pushed

Optional:

* `DEBUG`: set to TRUE to enable debug mode (default = FALSE)
* `APP_LOG_FILESIZE`: maximum size the log file will grow to before being rotated (default = 10MB)
* `APP_LOG_NUM_BACKUPS`: number of backups of the log that will be kept (default = 20)
* `APP_RUN_SYNCHRONOUS`: set to true to force app to run synchronously, used for debugging (default = false)
* `APP_QUEUE_MIN_RETRIES`: (default = 3)
* `APP_QUEUE_MAX_RETRIES`: (default = 15)
* `APP_QUEUE_RETRY_DELAY`: (default = 3)
* `APP_DEFAULT_UPDATE_DELTA_HOURS`: (default = 24)
* `JANRAIN_SCHEMA`: name of Capture schema (entity_type) containing the records (default = `user`)
* `JANRAIN_BATCH_SIZE`: maximum number of records to fetch at a time from Capture - higher = faster export, but if too large, the export may time out (default = 1000)
* `CC_CALL_TIMEOUT`: (default = 1)
* `CC_MAX_RETRIES`: Max number of times to retry when a rate limit error is received (default = 3)
* `CC_RETRY_TIMEOUT`: time in seconds to sleep between retries (default = 1)
* `CC_CALL_TIMEOUT`: time in seconds to sleep between calls to cc (default = 1)
