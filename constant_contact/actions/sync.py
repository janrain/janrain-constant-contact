"""Sync action."""
from flask import current_app as app
import janrain_datalib

def sync():
    """Sync records."""
    capture_app = janrain_datalib.get_app(
        app.config['JANRAIN_URI'],
        app.config['JANRAIN_CLIENT_ID'],
        app.config['JANRAIN_CLIENT_SECRET'])
    capture_schema = capture_app.get_schema(app.config['JANRAIN_SCHEMA_NAME'])

    # params for records.iterator()
    attributes = [x.strip() for x in app.config['JANRAIN_ATTRIBUTES'].split(',')]
    kwargs = {
        'batch_size': app.config['JANRAIN_BATCH_SIZE'],
        'attributes': attributes,
    }
    rf = record_filter()
    if rf:
        kwargs['filtering'] = rf

    records_count = 0
    app.logger.info("starting export from capture with batch size %s...", app.config['JANRAIN_BATCH_SIZE'])
    for batch_num, batch in enumerate(capture_schema.records.iterator(**kwargs), start=1):
        app.logger.debug("fetched batch: %d", batch_num)
        process_batch(batch)
        records_count += len(batch)
    app.logger.info("total records fetched: %d", records_count)

    return "done"

def record_filter():
    """Return the filter to use when fetching records from capture."""
    # TODO
    return ""

def process_batch(batch):
    """Implementation TBD"""
    # TODO
    app.logger.info("processed batch...")
