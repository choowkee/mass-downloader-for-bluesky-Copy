import json
from atproto_client.namespaces.sync_ns import ComAtprotoRepoNamespace
from atproto_client.models.com.atproto.repo.list_records import ParamsDict
from atproto import Client
from atproto.exceptions import AtProtocolError
import re, time, logging

from mdfb.utils.constants import DELAY

def get_all_post_identifiers(did: str, feed_type: str) -> list[str]:
    cursor = ""
    fetch_amount = 100
    post_uris = []
    logger = logging.getLogger(__name__)
    client = Client()
    while True:
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
        
        logger.info("Successful retrieved: %d posts", fetch_amount)
        records = res.get("records", {})
        if not records:
            logger.info(f"No more records to fetch for DID: {did}, feed_type: {feed_type}")
            break
        last_record_cid = re.search(r"\w+$", records[-1]["uri"])[0]
        if last_record_cid == cursor:
            break
        cursor = last_record_cid
        for record in records:
            if feed_type == "post":
                uri = record["uri"]
            else:
                uri = record["value"]["subject"]["uri"]
            post_uris.append(uri)
        time.sleep(DELAY)
    return post_uris

# posts = get_all_post_identifiers("did:plc:jqoadvytybij3aatl5tzqf2k", "repost")
# print(len(posts), posts)