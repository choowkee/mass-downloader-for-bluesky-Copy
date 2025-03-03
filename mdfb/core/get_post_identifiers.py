import json
from atproto_client.namespaces.sync_ns import ComAtprotoRepoNamespace
from atproto_client.models.com.atproto.repo.list_records import ParamsDict
from atproto import Client
from atproto.exceptions import AtProtocolError
import re, time, logging

from mdfb.utils.constants import DELAY


def get_post_identifiers(did: str, feed_type: str, limit: int = 0, archive: bool = False) -> list[str]:
    """
    get_post_identifiers: Gets the given amount AT-URIs of the posts wanted from the desired account 

    Args:
        did (str): DID of the target account
        feed_type (str): The type of post wanted from the account: like, repost and post
        limit (optional, default=0, int): The amount wanted to get
        archive (optional, default=False, bool): Will download all posts of the wanted type
    Raises:
        SystemExit: If there is a failure to retreive posts

    Returns:
        list[str]: A list of the desired AT-URIs
    """
    cursor = ""
    post_uris = []
    logger = logging.getLogger(__name__)
    client = Client()
    while limit > 0 or archive:
        fetch_amount = 100 if archive else min(100, limit)
        try:
            logger.info(f"Fetching up to {fetch_amount} posts for DID: {did}, feed_type: {feed_type}")
            res = ComAtprotoRepoNamespace(client).list_records(ParamsDict(
                collection=f"app.bsky.feed.{feed_type}",
                repo=did,
                limit=fetch_amount,
                cursor=cursor,
            ))  
            res = json.loads(res.model_dump_json())
        except AtProtocolError as e:
            logger.error(f"Failure to fetch posts: {e}", exc_info=True) 
            print("Failure to get fetch posts. See logs for details.")
            raise SystemExit(1) from e
        
        limit -= fetch_amount
        logger.info("Successful retrieved: %d posts, %d remaining", fetch_amount, limit)
        records = res.get("records", {})
        if not records:
            logger.info(f"No more records to fetch for DID: {did}, feed_type: {feed_type}")
            break
        last_record_cid = re.search(r"\w+$", records[-1]["uri"])[0]
        cursor = last_record_cid
        for record in records:
            if feed_type == "post":
                uri = record["uri"]
            else:
                uri = record["value"]["subject"]["uri"]
            post_uris.append(uri)
        time.sleep(DELAY)
    return post_uris