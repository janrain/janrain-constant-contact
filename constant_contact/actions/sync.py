"""Sync action."""
from flask import current_app as app
import boto3
import janrain_datalib
import json
import sys
import datetime
import collections
import traceback
from time import sleep
from .constant_contact_client import *

SDB_SYNC_IN_PROCESS_NAME = 'sync_in_process'
SDB_SYNC_IN_PROCESS_TRUE = 'true'
SDB_SYNC_IN_PROCESS_FALSE = 'false'
SDB_LAST_RUN_TIME_NAME = 'last_run_time'

class MaxRetriesError(Exception):
    """Thrown if max retries hit when calling constant contact"""
    def __init__(self, call_info):
        self.call_info = call_info
    def __str__(self):
        return 'max retries exceeded for call: ' + call_info

def sync():
    """The main sync method. 
        Spin up the queue_reader_process to process records from the An event 
        is passed to the process to enable this method to signal when all records 
        are loaded to the queue. All records that have been updated since the last time the sync was ran will 
        be pulled from capture and loaded into the queue. Once all records are loaded the queue reader 
        is signaled and the method waits for the reader to complete.
        """

    """SyncInfo object to pass references around"""    
    sync_info = init_sync()

    if not sync_info:
        app.logger.warning("Not able to initialize sync, shutting down")
        return "intialization error"

    app.logger.info("processing queue for retries")
    """messages encountered will not be retried if they fail"""
    process_queue(sync_info,False)

    """load updates into queue, if ccid attribute is missing add it"""
    try: 
        load_records(sync_info)

    except janrain_datalib.exceptions.ApiError as e:
        if e.message.find('subpath ' + sync_info['janrain_ccid_name'] + ' does not exist') > -1:
            sync_info['capture_schema'].add_attribute({'name': sync_info['janrain_ccid_name'], 'type': 'string'})
            load_records(sync_info)
        else:
            raise

    """messages will be retried if failed for retriable reason""" 
    process_queue(sync_info,True)

    """set the time of the completed run"""
    sdb_attributes = [{'Name':SDB_LAST_RUN_TIME_NAME,'Value':str(sync_info['now']),'Replace':True},
                      {'Name':SDB_SYNC_IN_PROCESS_NAME,'Value':SDB_SYNC_IN_PROCESS_FALSE,'Replace':True},]
    sync_info['sdb'].put_attributes(DomainName=sync_info['sdb_domain_name'],
                                 ItemName=sync_info['sdb_item_name'],Attributes=sdb_attributes)

    return "done"

def init_sync():
    """initialize all needed services and store references in dict with the
        the following values:

        capture_app
        capture_schema
        janrain_ccid_name
        janrain_attribute_map
        custom_field_map
        janrain_attributes
        queue
        sdb
        sdb_domain_name
        sdb_item_name
        cc_list_ids
        cc_client
        last_run_time
    """
    sync_info = {}
    sync_info['now'] = datetime.datetime.utcnow()

    sync_info = init_aws(sync_info)
    if not sync_info:
        return sync_info

    """check for info from last run. if there is a run in progress still aborting
        if there is no info to be found set the lock and set last_run_time to now - 15minutes
        """
    response = sync_info['sdb'].get_attributes(DomainName=sync_info['sdb_domain_name'],
                                            ItemName=sync_info['sdb_item_name'],
                                            AttributeNames=[SDB_SYNC_IN_PROCESS_NAME,SDB_LAST_RUN_TIME_NAME],
                                            ConsistentRead=True)
    if 'Attributes' in response:   
        for attribute in response['Attributes']:
            if attribute['Name'] == SDB_SYNC_IN_PROCESS_NAME:
                if attribute['Value'] == SDB_SYNC_IN_PROCESS_TRUE:
                    return log_and_return_warning("aborting: sync is already in process")
            if attribute['Name'] == SDB_LAST_RUN_TIME_NAME:
                sync_info['last_run_time'] = attribute['Value']

    if 'last_run_time' not in sync_info:
        sync_info['last_run_time'] = sync_info['now'] - datetime.timedelta(minutes=15)

    sdb_attributes = [{'Name':SDB_SYNC_IN_PROCESS_NAME,'Value':SDB_SYNC_IN_PROCESS_TRUE,'Replace':True}]
    sync_info['sdb'].put_attributes(DomainName=sync_info['sdb_domain_name'],
                                 ItemName=sync_info['sdb_item_name'],Attributes=sdb_attributes) 

    sync_info = init_janrain(sync_info)
    if not sync_info:
        return sync_info
    
    sync_info = init_cc(sync_info)
    if not sync_info:
        return sync_info
    
    return sync_info

def init_aws(sync_info):
    app.logger.debug("intializing aws sqs and sdb")

    aws_region = app.config['AWS_REGION']
    if not aws_region:
        return log_and_return_warning("aborting: AWS_REGION is not configured") 
    sdb_domain_name = app.config['AWS_SDB_DOMAIN_NAME']
    if not sdb_domain_name:
        return log_and_return_warning("aborting: AWS_SDB_DOMAIN_NAME is not configured") 
    sync_info['sdb_domain_name'] = sdb_domain_name
    try:        
        sdb = boto3.client('sdb', region_name=app.config['AWS_REGION'])
        # sdb.delete_domain(DomainName=sdb_domain_name)
        sdb.create_domain(DomainName=sdb_domain_name)
        sync_info['sdb'] = sdb
    except Exception as e:
        #specific exceptions
        log_and_return_warning("unable to connect to sdb: error detected: " + 
                                str(traceback.format_exception(*sys.exc_info())))
    
    sdb_item_name = app.config['AWS_SDB_ITEM_NAME']
    if not sdb_item_name:
        return log_and_return_warning("aborting: AWS_SDB_ITEM_NAME is not configured") 
    sync_info['sdb_item_name'] = sdb_item_name

    aws_sqs_queue_name = app.config['AWS_SQS_QUEUE_NAME']
    if not aws_sqs_queue_name:
        return log_and_return_warning("aborting: AWS_SQS_QUEUE_NAME is not configured")
    try: 
        sqs = boto3.resource('sqs', region_name=app.config['AWS_REGION'])
        sync_info['queue'] = sqs.create_queue(QueueName=app.config['AWS_SQS_QUEUE_NAME'])
    except Exception as e:
        #specific exceptions
        log_and_return_warning("unable to connect to sqs: error detected: " +
                                 str(traceback.format_exception(*sys.exc_info())))

    app.logger.debug("aws complete")
    return sync_info

def init_janrain(sync_info):
    app.logger.debug("intializing janrain")  

    """check that janrain info is configured and create janrain objects for sync"""
    janrain_uri = app.config['JANRAIN_URI']
    if not janrain_uri:
        return log_and_return_warning("aborting: JANRAIN_URI is not configured")    
    janrain_client_id = app.config['JANRAIN_CLIENT_ID']
    if not janrain_client_id:
        return log_and_return_warning("aborting: JANRAIN_CLIENT_ID is not configured")
    janrain_client_secret = app.config['JANRAIN_CLIENT_SECRET']
    if not janrain_client_secret:
        return log_and_return_warning("aborting: JANRAIN_CLIENT_SECRET is not configured")
    capture_app = janrain_datalib.get_app(janrain_uri,janrain_client_id,janrain_client_secret)
    sync_info['capture_app'] = capture_app
    janrain_schema_name = app.config['JANRAIN_SCHEMA_NAME']
    #should I check for existence (default is user)?
    sync_info['capture_schema'] = capture_app.get_schema(janrain_schema_name)

    """we will always sync email so create dictionary if not configured
        all attributes are optional but we will fail if the mapping is configured poorly
    """
    janrain_attribute_map = eval_mapping(app.config['JANRAIN_CC_ATTRIBUTE_MAPPING'],
                                                    'JANRAIN_CC_ATTRIBUTE_MAPPING')
    janrain_attribute_map.pop('email', None)
    janrain_attribute_map['email'] = 'email_addresses'
    sync_info['janrain_attribute_map'] = janrain_attribute_map

    """grab list of attributes from map for janrain filtering"""
    janrain_attributes = []
    janrain_attributes += list(janrain_attribute_map.keys())

    """custom fields are optional but we will fail if the mapping is configured poorly"""
    custom_field_map =  eval_mapping(app.config['JANRAIN_CC_CUSTOM_FIELD_MAPPING'],
                                                'JANRAIN_CC_CUSTOM_FIELD_MAPPING')
    if custom_field_map:
        janrain_attributes += list(custom_field_map.keys())
    sync_info['custom_field_map'] = custom_field_map
      
    """uuid must be added to list of attributes"""         
    try:
        janrain_attributes.remove('uuid')
    except ValueError:
        pass
    janrain_attributes.append('uuid')
    
    """default is 'constantContactId""" 
    janrain_ccid_name = app.config['JANRAIN_CCID_ATTR_NAME']
    try:
        janrain_attributes.remove(janrain_ccid_name)
    except ValueError:
        pass
    sync_info['janrain_ccid_name'] = janrain_ccid_name
    ##todo check if in schema
    janrain_attributes.append(janrain_ccid_name)
    sync_info['janrain_attributes'] = janrain_attributes

    app.logger.debug("janrain complete")
    return sync_info

def init_cc(sync_info):
    app.logger.debug ("intializing constant contact")

    list_ids = [x.strip() for x in app.config['CC_LIST_IDS'].split(',')]
    if not list_ids[0]:
        return log_and_return_warning("aborting: CC_LIST_IDS is not configured")
    sync_info['cc_list_ids'] = list_ids
    cc_api_key = app.config['CC_API_KEY']
    if not cc_api_key:
        return log_and_return_warning("aborting: CC_API_KEY is not configured") 
    cc_access_token = app.config['CC_ACCESS_TOKEN']
    if not cc_access_token:
        return log_and_return_warning("aborting: CC_ACCESS_TOKEN is not configured") 
    cc_client = ConstantContactClient(cc_api_key,cc_access_token)
    if not cc_client.health_check(list_ids[0]):
        app.logger.error("Can not connect to Constant Contact")
        return None
    sync_info['cc_client'] = cc_client

    app.logger.debug("constant contact complete")
    return sync_info

def load_records(sync_info):
    """grab records from capture and put them in sqs"""
    kwargs = {
        'batch_size': app.config['JANRAIN_BATCH_SIZE'],
        'attributes': sync_info['janrain_attributes'],
    }
    rf = record_filter(sync_info)
    if rf:
        kwargs['filtering'] = rf

    records_count = 0
    app.logger.info("starting export from capture with batch size %s...", app.config['JANRAIN_BATCH_SIZE'])

    for record_num, record in enumerate(sync_info['capture_schema'].records.iterator(**kwargs), start=1):
        app.logger.debug("fetched record: %d", record_num)
        # app.logger.info(record)
        app.logger.info("sent to queue: " + record['uuid'])
        sync_info['queue'].send_message(MessageBody=json.dumps(record))
        records_count += 1
    app.logger.info("total records fetched: %d", records_count)

def log_and_return_warning(message):
    app.logger.warning(message)
    return None

def eval_mapping(mapping_string,attribute_name):
    """eval mapping environment variables and make sure they produce dictionaries"""
    if mapping_string:
        mapping = {}
        try: 
            mapping = eval(mapping_string)
        except SyntaxError:
            app.logger.warning("aborting: unable to parse mapping: " + attribute_name)
            return None
        if not type(mapping) is dict:
            app.logger.warning("aborting: parsing mapping did not produce a dictionary: " + attribute_name)
            return None  
        return mapping
    else:
        return {}

def record_filter(sync_info):
    """Return the filter to use when fetching records from capture."""
    
    filter_string = "lastUpdated > '" + str(sync_info['last_run_time']) + "'"
    app.logger.info("app was last ran at: " + str(sync_info['last_run_time']) + " configuring filters")

    return filter_string

def process_queue(sync_info,retry):
    """spun up as a separate process from the main sync thread. Polls queue and dispatches messages untill 
       it determines all messages are processed. It will run until either:
        (the number of retries since the last message was found > APP_QUEUE_MIN_RETRIES 
        AND
        the job_complete_event is set from the parent thread)
        OR
        the number of retries since the last message was found > APP_QUEUE_MAX_RETRIES"""
    queue_has_messages = True
    retries = 0
    app.logger.info("processing queue")
    min_retries = app.config['APP_QUEUE_MIN_RETRIES']
    while True:
        messages = sync_info['queue'].receive_messages(MaxNumberOfMessages=5)
        if len(messages) == 0:
            retries += 1
            if retries > min_retries:
                app.logger.info('found empty queue ' + str(min_retries) + ' times, stopping process')
                break
            sleep(app.config['APP_QUEUE_RETRY_DELAY'])
            app.logger.info("returned: 0 messages from queue, retry: " + str(retries))
        else:
            retries = 0
            app.logger.info("returned: " + str(len(messages))+ " messages from queue")
            for m in messages:
                if process_message(m,sync_info) or not retry:
                    m.delete()
        

def process_message(message,sync_info):
    """process a message from the queue. contains the main business logic of how an update is made"""
    janrain_user_info = json.loads(message.body)
    email = janrain_user_info['email']
    if email is None:
        app.logger.info("Skipping record with no email: " + janrain_user_info['uuid'])
        return
    log_tag = email + ':' + janrain_user_info['uuid'] + ':'

    app.logger.info(log_tag + " processing message")
    ccid_name = sync_info['janrain_ccid_name']
    cc_client = sync_info['cc_client']
    cc_contact = None
    #app.logger.info(janrain_user_info)
    try:
        cc_contact = get_by_email_or_id(sync_info,log_tag,email)
        if cc_contact is None:
            app.logger.info(log_tag + " contact not found by email")
            if janrain_user_info[ccid_name] is None:
                app.logger.info(log_tag + " no info in " + ccid_name + ' creating new contact')
                """we have a new account or must treat as if we do"""
                create_or_update(sync_info, janrain_user_info, cc_contact, log_tag)
                app.logger.info(log_tag + " contact created")
            else:   
                """email address may have changed"""
                cc_contact = get_by_email_or_id(sync_info,log_tag,
                                            janrain_user_info[ccid_name],email_id=False)
                if cc_contact is None:
                    app.logger.info(log_tag + " contact not found by id, creating new contact")
                    """contactid is stale and we must create new account"""
                    create_or_update(sync_info, janrain_user_info, cc_contact, log_tag)
                else:
                    app.logger.info(log_tag + " contact found by id, syncing")
                    """email has changed"""
                    create_or_update(sync_info, janrain_user_info, cc_contact, log_tag)
                    app.logger.info(log_tag + " contact synced")

        else:
            app.logger.info(log_tag + " contact found by email, syncing")
            """email has changed"""
            create_or_update(sync_info, janrain_user_info, cc_contact, log_tag)
            app.logger.info(log_tag + " contact synced")
        return True
    except constant_contact_client.ConstantConctactServerError as e:
        app.logger.info(log_tag + e.message)
        """retry""" 
        return False
    except Exception as e:
        app.logger.exception('')
        return True

def get_by_email_or_id(sync_info,indentifier,tag,email_id=True):
    """defaut is to search by email, set email_id to false to search by cc id
        if rate limit errors recieved will retry up to the configed max before
        failing over"""
    tries = 0
    contact = -1
    while tries < app.config['APP_CC_CALL_MAX_RETRIES']:
        if email_id:
            contact = sync_info['cc_client'].get_contact_by_email(indentifier)
        else:
            contact = sync_info['cc_client'].get_contact_by_id(indentifier)
        if contact_id != -1:
            return contact
        tries += 1
        sleep(tries * app.config['APP_CC_CALL_RETRY_SLEEP_FACTOR'])
    raise MaxRetriesError('get by email or id for: ' + tag)

def create_or_update(sync_info,janrain_user_info,cc_contact,tag):
    """if contact passes is empty will route to create otherwise updates
       if rate limit errors recieved will retry up to the configed max before
        failing over """
    tries = 0
    MAX_TRIES = 3
    RETRY_SLEEP = 1
    contact_id = -1
    while tries < app.config['APP_CC_CALL_MAX_RETRIES']:
        if cc_contact is None:
            contact_id = create_contact(sync_info,janrain_user_info,tag)
        else:
            contact_id = update_contact(sync_info,janrain_user_info,cc_contact,tag)
        if contact_id  > -1:
            break;
        tries += 1
        sleep(tries * app.config['APP_CC_CALL_RETRY_SLEEP_FACTOR'])
    raise MaxRetriesError('create or update for: ' + tag)

def update_contact(sync_info,janrain_user_info,cc_contact,tag):
    """makes put call to contact id to update contact in cc and 
        updates ccid in janrain if out of sync"""
    cc_client = sync_info['cc_client']
    new_contact = cc_client.transform_contact(repack_user(sync_info,janrain_user_info),sync_info['janrain_attribute_map'])
    for attr in new_contact:
        cc_contact[attr] = new_contact[attr]
    ccid = cc_client.put_contact(cc_contact)
    app.logger.debug(tag + ' updated ' + ccid + ' in cc')
    if ccid != janrain_user_info[sync_info['janrain_ccid_name']]:
        """update janrain user if ccid is out of sync"""
        app.logger.info(tag + 'id out of sync in janrian, updating')
        sync_info['capture_schema'].records.get_record(janrain_user_info['uuid']).update({sync_info['janrain_ccid_name']: ccid}) 

def create_contact(sync_info,janrain_user_info,tag):  
    """makes post call to create contact in cc and 
        updates ccid in janrain."""  
    cc_client = sync_info['cc_client']
    cc_contact = {}
    """add the configured list(s) to the user object and a copy of our attribute mapping"""
    user = repack_user(sync_info,janrain_user_info)
    user['lists'] = sync_info['cc_list_ids']
    mapping = sync_info['janrain_attribute_map'].copy()
    mapping['lists'] = 'lists'
    cc_contact = cc_client.transform_contact(user,mapping)
    ccid = cc_client.post_contact(cc_contact)
    app.logger.debug(tag + ' created ' + ccid + ' in cc updating janain')
    sync_info['capture_schema'].records.get_record(janrain_user_info['uuid']).update({sync_info['janrain_ccid_name']: ccid}) 

def repack_user(sync_info,janrain_user_info):
    """takes standard janrain user object and flattens it.
        map and removes individual custom fields to a custom fields object
        removes uuid and ccid"""
    user_info = flatten(janrain_user_info)
    custom_field_map = sync_info['custom_field_map']
    if custom_field_map:
        custom_fields = {}
        for mapping in custom_field_map:
            if mapping in user_info:
                custom_fields[custom_field_map[mapping]] = user_info[mapping]
                user_info.pop(mapping,None)
        user_info['custom_fields'] = custom_fields
    user_info.pop('uuid',None)
    user_info.pop(sync_info['janrain_ccid_name'],None)
    return user_info

def flatten(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

