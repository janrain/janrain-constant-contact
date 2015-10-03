"""Sync action."""
from flask import current_app as app
import boto3
import janrain_datalib
import json
import multiprocessing as mp
import sys
import datetime
from time import sleep
from .constant_contact_client import ConstantContactClient

class SyncInfo(object):
    def __init__(self,capture_app,capture_schema,cc_client,ccid_name,
                 attribute_mapping,attributes,queue,list_ids,sdb):
        self.capture_app = capture_app
        self.capture_schema = capture_schema
        self.cc_client = cc_client
        self.ccid_name = ccid_name
        self.attribute_mapping = attribute_mapping
        self.queue = queue
        self.sdb = sdb
        self.attributes = attributes
        self.list_ids = list_ids

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

    now = datetime.datetime.now().isoformat(' ')

    kwargs = {
        'batch_size': app.config['JANRAIN_BATCH_SIZE'],
        'attributes': sync_info.attributes,
    }
    rf = record_filter(sync_info)
    if rf:
        kwargs['filtering'] = rf

    records_count = 0
    app.logger.info("starting export from capture with batch size %s...", app.config['JANRAIN_BATCH_SIZE'])

    """spin up queue reader process to start polling queue, event is to notify when all records are sent"""
    job_complete_event = mp.Event()
    queue_reader_process = mp.Process(target=process_queue, args=(sync_info, job_complete_event))
    queue_reader_process.start()

    """load updates into queue"""
    for record_num, record in enumerate(sync_info.capture_schema.records.iterator(**kwargs), start=1):
        app.logger.debug("fetched record: %d", record_num)
        # app.logger.info(record)
        app.logger.info("sent to queue: " + record['uuid'])
        sync_info.queue.send_message(MessageBody=json.dumps(record))
        records_count += 1
    app.logger.info("total records fetched: %d", records_count)

    """signal job is done and wait for reader to finish"""
    job_complete_event.set()
    queue_reader_process.join()

    """set the time of the completed run"""
    sdb_attributes = [{'Name':app.config['AWS_SDB_LAST_RUN_NAME'],'Value':str(now),'Replace':True}]
    sync_info.sdb.put_attributes(DomainName=app.config['AWS_SDB_DOMAIN_NAME'],
                                 ItemName=app.config['AWS_SDB_ITEM_NAME'],Attributes=sdb_attributes)

    return "done"

def init_sync():
    """initialize all needed services and store references in a SyncInfo object"""
    # todo error handling
    app.logger.debug("intializing queue: " + app.config['AWS_SQS_QUEUE_NAME'])
    sqs = boto3.resource('sqs', region_name=app.config['AWS_REGION'])
    queue = sqs.create_queue(QueueName=app.config['AWS_SQS_QUEUE_NAME'])

    sdb = boto3.client('sdb', region_name=app.config['AWS_REGION'])
    sdb.create_domain(DomainName=app.config['AWS_SDB_DOMAIN_NAME'])
    
    cc_client = ConstantContactClient(app.config['CC_API_KEY'], app.config['CC_ACCESS_TOKEN'])

    capture_app = janrain_datalib.get_app(
        app.config['JANRAIN_URI'],
        app.config['JANRAIN_CLIENT_ID'],
        app.config['JANRAIN_CLIENT_SECRET'])

    capture_schema = capture_app.get_schema(app.config['JANRAIN_SCHEMA_NAME'])

    attr_map = eval(app.config['JANRAIN_ATTRIBUTE_MAPPING'])
    attr_map.pop('email', None)
    attr_map['email'] = 'email_addresses'

    ccid_name = app.config['JANRAIN_CCID_ATTR_NAME']

    attributes = list(attr_map.keys())
    try:
        attributes.remove('uuid')
    except ValueError:
        pass
    attributes.append('uuid')
    try:
        attributes.remove(ccid_name)
    except ValueError:
        pass
    attributes.append(ccid_name)

    list_ids = [x.strip() for x in app.config['CC_LIST_IDS'].split(',')]

    return SyncInfo(capture_app, capture_schema,cc_client,ccid_name,attr_map,attributes,queue,list_ids,sdb)

def record_filter(sync_info):
    """Return the filter to use when fetching records from capture."""
    last_run = sync_info.sdb.get_attributes(DomainName=app.config['AWS_SDB_DOMAIN_NAME'],
                                            ItemName=app.config['AWS_SDB_ITEM_NAME'],
                                            AttributeNames=[app.config['AWS_SDB_LAST_RUN_NAME']],
                                            ConsistentRead=True)['Attributes'][0]['Value']
    filter_string = "lastUpdated > " + last_run 
    app.logger.info("app was last ran at: " + last_run + " configuring filter: " + filter_string)
    ##this is causing errors still. need to fix
    # return filter_string
    return ""

def process_queue(sync_info, job_complete_event):
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
    while ((retries < app.config['APP_QUEUE_MIN_RETRIES'] or not job_complete_event.is_set()) and \
            retries < app.config['APP_QUEUE_MAX_RETRIES'] ):
        ##todo catch MAX retries and log as error
        messages = sync_info.queue.receive_messages(MaxNumberOfMessages=5)
        if len(messages) == 0:
            retries += 1
            sleep(1)
        else:
            retries = 0
        app.logger.info("returned: " + str(len(messages))+ " messages from queue")
        for m in messages:
            process_message(m,sync_info)
            m.delete()

def process_message(message,sync_info):
    """process a message from the queue. contains the main business logic of how an update is made"""
    janrain_user_info = json.loads(message.body)
    email = janrain_user_info['email']
    log_tag = email + ':' + janrain_user_info['uuid'] + ':'

    app.logger.info(log_tag + " processing message")
    ccid_name = sync_info.ccid_name
    cc_client = sync_info.cc_client

    #app.logger.info(janrain_user_info)
    cc_contact = cc_client.get_contact_by_email(email)
    if cc_contact is None:
        app.logger.info(log_tag + " contact not found by email")
        if janrain_user_info[ccid_name] is None:
            app.logger.info(log_tag + " no info in " + ccid_name + ' creating new contact')
            """we have a new account or must treat as if we do"""
            create_contact(sync_info, janrain_user_info, log_tag)
            app.logger.info(log_tag + " contact created")
        else:   
            """email address may have changed"""
            cc_contact = cc_client.get_contact_by_id(janrain_user_info[ccid_name])
            if cc_contact is None:
                app.logger.info(log_tag + " contact not found by id, creating new contact")
                """contactid is stale and we must create new account"""
                create_contact(sync_info, janrain_user_info, log_tag)
            else:
                app.logger.info(log_tag + " contact found by id, syncing")
                """email has changed"""
                sync_contact(sync_info, janrain_user_info, cc_contact, log_tag)
                app.logger.info(log_tag + " contact synced")

    else:
        app.logger.info(log_tag + " contact found by email, syncing")
        """email has changed"""
        sync_contact(sync_info, janrain_user_info, cc_contact, log_tag)
        app.logger.info(log_tag + " contact synced")

def sync_contact(sync_info,janrain_user_info,cc_contact,tag):
    attribute_mapping = sync_info.attribute_mapping
    cc_client = sync_info.cc_client
    ccid_name = sync_info.ccid_name
    """map of cc attribute name and transformation function"""
    cc_transform_mapping = cc_client.get_transform_mapping()
    for janrain_attribute in attribute_mapping:
        cc_attribute = attribute_mapping[janrain_attribute]
        """transform mapping provides a dict of transform functions. 
           For each value we pass it to the function and receive the appropriate value back"""
        cc_contact[cc_attribute] = cc_transform_mapping[cc_attribute](janrain_user_info[janrain_attribute])
    ccid = cc_client.put_contact_by_id(cc_contact)
    app.logger.debug(tag + ' updated ' + ccid + ' in cc updating janain')
    if ccid != janrain_user_info[ccid_name]:
        """update janrain user if ccid is out of sync"""
        sync_info.capture_schema.records.get_record(janrain_user_info['uuid']).update({ccid_name: ccid}) 

def create_contact(sync_info,janrain_user_info,tag):
    attribute_mapping = sync_info.attribute_mapping
    cc_client = sync_info.cc_client
    cc_contact = {}
    for janrain_attribute in attribute_mapping:
        cc_attribute = attribute_mapping[janrain_attribute]
        """transform mapping provides a dict of transform functions. For each value we pass 
           it to the function and receive the appropriate value back"""
        cc_contact[cc_attribute] = cc_client.get_transform_mapping()[cc_attribute](janrain_user_info[janrain_attribute])
    cc_contact['lists'] = cc_client.transform_lists(sync_info.list_ids)
    ccid = cc_client.post_contact(cc_contact)
    app.logger.debug(tag + ' created ' + ccid + ' in cc updating janain')
    sync_info.capture_schema.records.get_record(janrain_user_info['uuid']).update({sync_info.ccid_name: ccid}) 

