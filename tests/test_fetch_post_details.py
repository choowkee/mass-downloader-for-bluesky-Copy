import json
from unittest.mock import MagicMock
import pytest
from tenacity import RetryError, stop_after_attempt, wait_none
from mdfb.core import fetch_post_details
from atproto.exceptions import AtProtocolError


def test_fetch_post_details(mocker):
    mock_json_response = {'posts': [{'author': {'did': 'did:plc:m453r6fotgsoegzxtxj7snwa', 'handle': 'nature-view.bsky.social', 'associated': None, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreif3z4vgsms2hoxcykl3qehk6uhsw5ubqgpzvbo6biitjrwjnvgyny@jpeg', 'created_at': '2024-11-21T10:09:10.071Z', 'display_name': "Nature's masterpiece", 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreidj6ievh3i7spm6fqnebhdpnmwj3c4iz6kfpqymrtvyeahvrqflj4', 'indexed_at': '2024-12-10T15:38:05.561Z', 'record': {'created_at': '2024-12-10T15:38:01.684Z', 'text': 'Full Moon 🌕🌠', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 230390, 'ref': {'link': 'bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 1010, 'width': 736, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:m453r6fotgsoegzxtxj7snwa/app.bsky.feed.post/3lcxl453gps2l', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa@jpeg', 'aspect_ratio': {'height': 1010, 'width': 736, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 11394, 'quote_count': 26, 'reply_count': 102, 'repost_count': 739, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, {'author': {'did': 'did:plc:4youk6koejgwe5m4lennnn4g', 'handle': 'deepgreens.bsky.social', 'associated': None, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreidij4dcdp6ntjtbhabjhghjkfosisih6lqbclbls7tnvtypz2j4ba@jpeg', 'created_at': '2024-10-19T02:57:03.115Z', 'display_name': 'Floridiana 🌺', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreih6opo7az6gk2gnb4stynbdli33anjkdl4al4rhrm5bov2dpramai', 'indexed_at': '2024-12-10T18:13:49.060Z', 'record': {'created_at': '2024-12-10T18:13:39.727Z', 'text': 'Sunset versus Storm', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 924046, 'ref': {'link': 'bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 2000, 'width': 1551, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.post/3lcxtsgk27s2o', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny@jpeg', 'aspect_ratio': {'height': 2000, 'width': 1551, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 5782, 'quote_count': 8, 'reply_count': 90, 'repost_count': 329, 'threadgate': {'cid': 'bafyreiandeby55ef3kzcck7ze52fbcplf5feoyqhgs4l4ns3eky3qrtiiu', 'lists': [], 'record': {'created_at': '2024-12-11T16:18:22.374Z', 'post': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.post/3lcxtsgk27s2o', 'allow': None, 'hidden_replies': ['at://did:plc:5l4lgpkjtfhucmsc6mun3vff/app.bsky.feed.post/3lcy34z33oc22', 'at://did:plc:qnuzhyazuau7jxta55h6xxg5/app.bsky.feed.post/3lcxwotjqa22o', 'at://did:plc:ybxmfg3we6idmvv6qdnwv6vo/app.bsky.feed.post/3lcy7v5vx3c2n', 'at://did:plc:xpfkkdwp63tlpqul4hga6zqh/app.bsky.feed.post/3lcyctij6wk2n', 'at://did:plc:jpfsjbtbkreqrf6455nkme72/app.bsky.feed.post/3lczmtyxvsk2o', 'at://did:plc:snqsncnjxkn3kjesfgndedpr/app.bsky.feed.post/3ld25qguabs2w'], 'py_type': 'app.bsky.feed.threadgate'}, 'uri': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.threadgate/3lcxtsgk27s2o', 'py_type': 'app.bsky.feed.defs#threadgateView'}, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    mock_response = MagicMock()
    mock_response.model_dump_json.return_value = json.dumps(mock_json_response)
    mock_request = mocker.patch("atproto_client.namespaces.sync_ns.AppBskyFeedNamespace.get_posts", return_value=mock_response)
    result = fetch_post_details.fetch_post_details(["example1", "example2"])
    print(result)
    assert result == [{'rkey': '3lcxl453gps2l', 'response': {'author': {'did': 'did:plc:m453r6fotgsoegzxtxj7snwa', 'handle': 'nature-view.bsky.social', 'associated': None, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreif3z4vgsms2hoxcykl3qehk6uhsw5ubqgpzvbo6biitjrwjnvgyny@jpeg', 'created_at': '2024-11-21T10:09:10.071Z', 'display_name': "Nature's masterpiece", 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreidj6ievh3i7spm6fqnebhdpnmwj3c4iz6kfpqymrtvyeahvrqflj4', 'indexed_at': '2024-12-10T15:38:05.561Z', 'record': {'created_at': '2024-12-10T15:38:01.684Z', 'text': 'Full Moon 🌕🌠', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 230390, 'ref': {'link': 'bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 1010, 'width': 736, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:m453r6fotgsoegzxtxj7snwa/app.bsky.feed.post/3lcxl453gps2l', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:m453r6fotgsoegzxtxj7snwa/bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa@jpeg', 'aspect_ratio': {'height': 1010, 'width': 736, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 11394, 'quote_count': 26, 'reply_count': 102, 'repost_count': 739, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'did': 'did:plc:m453r6fotgsoegzxtxj7snwa', 'handle': 'nature-view.bsky.social', 'display_name': "Nature's masterpiece", 'text': 'Full Moon 🌕🌠', 'images_cid': ['bafkreibogi25adwdzqtcmm3qxj533xon3lh7gt42pphgohjdwhmldz2iwa'], 'mime_type': 'image/jpeg'}, {'rkey': '3lcxtsgk27s2o', 'response': {'author': {'did': 'did:plc:4youk6koejgwe5m4lennnn4g', 'handle': 'deepgreens.bsky.social', 'associated': None, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreidij4dcdp6ntjtbhabjhghjkfosisih6lqbclbls7tnvtypz2j4ba@jpeg', 'created_at': '2024-10-19T02:57:03.115Z', 'display_name': 'Floridiana 🌺', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreih6opo7az6gk2gnb4stynbdli33anjkdl4al4rhrm5bov2dpramai', 'indexed_at': '2024-12-10T18:13:49.060Z', 'record': {'created_at': '2024-12-10T18:13:39.727Z', 'text': 'Sunset versus Storm', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 924046, 'ref': {'link': 'bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 2000, 'width': 1551, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.post/3lcxtsgk27s2o', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:4youk6koejgwe5m4lennnn4g/bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny@jpeg', 'aspect_ratio': {'height': 2000, 'width': 1551, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 5782, 'quote_count': 8, 'reply_count': 90, 'repost_count': 329, 'threadgate': {'cid': 'bafyreiandeby55ef3kzcck7ze52fbcplf5feoyqhgs4l4ns3eky3qrtiiu', 'lists': [], 'record': {'created_at': '2024-12-11T16:18:22.374Z', 'post': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.post/3lcxtsgk27s2o', 'allow': None, 'hidden_replies': ['at://did:plc:5l4lgpkjtfhucmsc6mun3vff/app.bsky.feed.post/3lcy34z33oc22', 'at://did:plc:qnuzhyazuau7jxta55h6xxg5/app.bsky.feed.post/3lcxwotjqa22o', 'at://did:plc:ybxmfg3we6idmvv6qdnwv6vo/app.bsky.feed.post/3lcy7v5vx3c2n', 'at://did:plc:xpfkkdwp63tlpqul4hga6zqh/app.bsky.feed.post/3lcyctij6wk2n', 'at://did:plc:jpfsjbtbkreqrf6455nkme72/app.bsky.feed.post/3lczmtyxvsk2o', 'at://did:plc:snqsncnjxkn3kjesfgndedpr/app.bsky.feed.post/3ld25qguabs2w'], 'py_type': 'app.bsky.feed.threadgate'}, 'uri': 'at://did:plc:4youk6koejgwe5m4lennnn4g/app.bsky.feed.threadgate/3lcxtsgk27s2o', 'py_type': 'app.bsky.feed.defs#threadgateView'}, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'did': 'did:plc:4youk6koejgwe5m4lennnn4g', 'handle': 'deepgreens.bsky.social', 'display_name': 'Floridiana 🌺', 'text': 'Sunset versus Storm', 'images_cid': ['bafkreif7wk76urqa2a7kusbq6yb4iv6fbcekwybnccqcs4bakw3tel7rny'], 'mime_type': 'image/jpeg'}]
    mock_request.assert_called_once_with({"uris": ["example1", "example2"]})
    
def test_fetch_post_details_no_uris(mocker):
    result = fetch_post_details.fetch_post_details([])
    print(result)
    assert result == []

def test_fetch_post_details_deleted_post(mocker):
    mock_json_response = {"posts": []}
    mock_response = MagicMock()
    mock_response.model_dump_json.return_value = json.dumps(mock_json_response)
    mock_request = mocker.patch("atproto_client.namespaces.sync_ns.AppBskyFeedNamespace.get_posts", return_value=mock_response)
    result = fetch_post_details.fetch_post_details(["at://did:example:1234"])
    print(result)
    assert result == []
    mock_request.assert_called_once_with({"uris": ["at://did:example:1234"]})

def test_extract_media_image(mocker):
    mock_input = {'posts': [{'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigp3wgf36lhnb73plsy4xyfpaub5xvrntmmh4lga2z75reld3qnna', 'indexed_at': '2024-11-18T02:51:34.738Z', 'record': {'created_at': '2024-11-18T02:51:26.625Z', 'text': "me watching Bluesky's user count grow by another million in a day\n\nhello and welcome to all 19M of you! 🥳", 'embed': {'images': [{'alt': 'Kris Jenner looking at a computer', 'image': {'mime_type': 'image/jpeg', 'size': 232606, 'ref': {'link': 'bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 487, 'width': 702, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lb6vz4ms6c25', 'embed': {'images': [{'alt': 'Kris Jenner looking at a computer', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq@jpeg', 'aspect_ratio': {'height': 487, 'width': 702, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 222800, 'quote_count': 1063, 'reply_count': 5182, 'repost_count': 11589, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    print(json.dumps(mock_input, indent=4))
    mock_input = mock_input["posts"][0]["record"]["embed"]
    result = fetch_post_details._extract_media(mock_input)
    assert result == {'images_cid': ['bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq'], 'mime_type': 'image/jpeg'}

def test_extract_media_video(mocker):
    mock_input = {'posts': [{'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigah7oevnzroay2ve56w7pd3atscamiekwqbmj3kl66mvqsuydtwe', 'indexed_at': '2024-11-15T00:53:48.256Z', 'record': {'created_at': '2024-11-15T00:53:46.785Z', 'text': "it's official — 1,000,000 people have joined Bluesky in just the last day!!! \n\nwelcome and thank you for being here 🥳", 'embed': {'video': {'mime_type': 'video/mp4', 'size': 1787532, 'ref': {'link': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq'}, 'py_type': 'blob'}, 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'captions': None, 'py_type': 'app.bsky.embed.video'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lax5zxh7bc2p', 'embed': {'cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'playlist': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/playlist.m3u8', 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'thumbnail': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/thumbnail.jpg', 'py_type': 'app.bsky.embed.video#view'}, 'labels': [], 'like_count': 88777, 'quote_count': 1724, 'reply_count': 2380, 'repost_count': 8951, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    mock_input = mock_input["posts"][0]["record"]["embed"]
    result = fetch_post_details._extract_media(mock_input)
    assert result == {'video_cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'mime_type': 'video/mp4'}

def test_get_rkey(mocker):
    mock_input = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lax5zxh7bc2p"
    result = fetch_post_details._get_rkey(mock_input)
    assert result == "3lax5zxh7bc2p"

def test_get_author_details(mocker):
    mock_input = {'posts': [{'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigah7oevnzroay2ve56w7pd3atscamiekwqbmj3kl66mvqsuydtwe', 'indexed_at': '2024-11-15T00:53:48.256Z', 'record': {'created_at': '2024-11-15T00:53:46.785Z', 'text': "it's official — 1,000,000 people have joined Bluesky in just the last day!!! \n\nwelcome and thank you for being here 🥳", 'embed': {'video': {'mime_type': 'video/mp4', 'size': 1787532, 'ref': {'link': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq'}, 'py_type': 'blob'}, 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'captions': None, 'py_type': 'app.bsky.embed.video'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lax5zxh7bc2p', 'embed': {'cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'playlist': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/playlist.m3u8', 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'thumbnail': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/thumbnail.jpg', 'py_type': 'app.bsky.embed.video#view'}, 'labels': [], 'like_count': 88777, 'quote_count': 1724, 'reply_count': 2380, 'repost_count': 8951, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    mock_input = mock_input["posts"][0]["author"]
    result = fetch_post_details._get_author_details(mock_input)
    assert result == {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'display_name': 'Bluesky'}

def test_get_post_details_retires_then_succeeds(monkeypatch: pytest.MonkeyPatch, mocker):
    mock_uri_chunk = ["example1", "example2"]
    mock_client = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_retires = 5

    mock_get_post_details = mocker.patch("atproto_client.namespaces.sync_ns.AppBskyFeedNamespace.get_posts")
    mock_get_post_details.side_effect = [
        AtProtocolError(),
        AtProtocolError(),
        AtProtocolError(),
        AtProtocolError(),
        {"posts": ["post1", "post2"]},
    ]

    monkeypatch.setattr(
        fetch_post_details._get_post_details.retry,
        "stop",
        stop_after_attempt(mock_retires)
    )
    monkeypatch.setattr(
        fetch_post_details._get_post_details.retry,
        "wait",
        wait_none()
    )

    response = fetch_post_details._get_post_details(mock_uri_chunk, mock_client, mock_logger)

    assert response == {"posts": ["post1", "post2"]}
    assert mock_get_post_details.call_count == mock_retires 

def test_get_post_details_exceeds_retries(monkeypatch: pytest.MonkeyPatch, mocker):
    mock_uri_chunk = ["example1", "example2"]
    mock_client = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_retires = 5

    mock_get_post_details = mocker.patch("atproto_client.namespaces.sync_ns.AppBskyFeedNamespace.get_posts")
    mock_get_post_details.side_effect = [
        AtProtocolError(),
        AtProtocolError(),
        AtProtocolError(),
        AtProtocolError(),
        AtProtocolError(),
        {"posts": ["post1", "post2"]},
    ]

    monkeypatch.setattr(
        fetch_post_details._get_post_details.retry,
        "stop",
        stop_after_attempt(mock_retires)
    )
    monkeypatch.setattr(
        fetch_post_details._get_post_details.retry,
        "wait",
        wait_none()
    )

    with pytest.raises(RetryError):
        fetch_post_details._get_post_details(mock_uri_chunk, mock_client, mock_logger)

    assert mock_get_post_details.call_count == mock_retires 

def test_get_post_details_with_retries_success(mocker):
    mock_uri_chunk = ["example1", "example2"]
    mock_client = mocker.Mock()
    mock_logger = mocker.Mock()
    mock_response =  {"posts": ["post1", "post2"]}
    mocker.patch("mdfb.core.fetch_post_details._get_post_details", return_value=mock_response)
    response = fetch_post_details._get_post_details_with_retries(mock_uri_chunk, mock_client, mock_logger)
    
    assert response == mock_response

def test_get_post_details_with_retries_failure(mocker):
    mock_uri_chunk = ["example1", "example2"]
    mock_client = mocker.Mock()
    mock_logger = mocker.Mock()
    mocker.patch("mdfb.core.fetch_post_details._get_post_details", side_effect=AtProtocolError())
    
    response = fetch_post_details._get_post_details_with_retries(mock_uri_chunk, mock_client, mock_logger)
    
    assert response == None
    
    
    mock_logger.error.assert_called_with(
        f"Failure to fetch records from the URIs: {mock_uri_chunk}",
        exc_info=True
    )