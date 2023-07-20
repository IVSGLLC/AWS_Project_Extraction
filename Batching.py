from logging import Logger
import logging
import os
import time
from botocore.exceptions import ClientError
loglevel = int(os.environ['LOG_LEVEL'])
logger = logging.getLogger('Batching')
logger.setLevel(loglevel)

MAX_GET_SIZE = 100  # Amazon DynamoDB rejects a get batch larger than 100 items.
def do_batch_get(dynamodb,batch_keys):
    """
    Gets a batch of items from Amazon DynamoDB. Batches can contain keys from
    more than one table.

    When Amazon DynamoDB cannot process all items in a batch, a set of unprocessed
    keys is returned. This function uses an exponential backoff algorithm to retry
    getting the unprocessed keys until all are retrieved or the specified
    number of tries is reached.

    :param batch_keys: The set of keys to retrieve. A batch can contain at most 100
                       keys. Otherwise, Amazon DynamoDB returns an error.
    :return: The dictionary of retrieved items grouped under their respective
             table names.
    """
    tries = 0
    max_tries = 5
    sleepy_time = 1  # Start with 1 second of sleep, then exponentially increase.
    retrieved = {key: [] for key in batch_keys}
    while tries < max_tries:
        response = dynamodb.batch_get_item(RequestItems=batch_keys)
        # Collect any retrieved items and retry unprocessed keys.
        for key in response.get('Responses', []):
            retrieved[key] += response['Responses'][key]
        unprocessed = response['UnprocessedKeys']
        if len(unprocessed) > 0:
            batch_keys = unprocessed
            unprocessed_count = sum(
                [len(batch_key['Keys']) for batch_key in batch_keys.values()])
            Logger.info(
                "%s unprocessed keys returned. Sleep, then retry.",
                unprocessed_count)
            tries += 1
            if tries < max_tries:
                logger.info("Sleeping for %s seconds.", sleepy_time)
                time.sleep(sleepy_time)
                sleepy_time = min(sleepy_time * 2, 32)
        else:
            break

    return retrieved
def get_batch_data(dynamodb,tableName, item_list):
    """
    Gets data from the specified movie and actor tables. Data is retrieved in batches.

    :param movie_table: The table from which to retrieve movie data.
    :param movie_list: A list of keys that identify movies to retrieve.
    :param actor_table: The table from which to retrieve actor data.
    :param actor_list: A list of keys that identify actors to retrieve.
    :return: The dictionary of retrieved items grouped under the respective
             movie and actor table names.
    """
     
    batch_keys = {
        tableName: {
            'Keys': [{'document_type': item['document_type'], 'document_id': item['document_id']} for item in item_list]
        } 
    }
    try:
        retrieved = do_batch_get(dynamodb,batch_keys)
        for response_table, response_items in retrieved.items():
            logger.info("Got %s items from %s.", len(response_items), response_table)
    except ClientError:
        logger.error(
            "Couldn't get items from %s and %s.", tableName)
        raise
    else:
        return retrieved