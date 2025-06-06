import json
from unittest.mock import MagicMock
import pytest
from tenacity import RetryError, stop_after_attempt, wait_none
from mdfb.core import fetch_post_details
from atproto.exceptions import AtProtocolError


def test_fetch_post_details(mocker):
    mock_json_response = [{'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.like/3lqxkvxe5mo2s'], 'feed_type': ['like'], 'poster_post_uri': 'at://did:plc:vpkdwwdia5etkdsuvsewtlws/app.bsky.feed.post/3lqx7wzy7c227', 'author': {'did': 'did:plc:vpkdwwdia5etkdsuvsewtlws', 'handle': 'inkpixels.bsky.social', 'associated': {'chat': {'allow_incoming': 'following', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreiauvzci74nrfoxhkfgd3mo6h75ejmzpxpyxlkbg2d5shflhy2ugmu@jpeg', 'created_at': '2023-08-20T15:22:50.198Z', 'display_name': 'ink', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreidhuc5hv3iiq26e6citq2d77xl5gwsf22mp6qpwtl73hdkjqy3jsa', 'indexed_at': '2025-06-06T16:11:06.061Z', 'record': {'created_at': '2025-06-06T16:11:02.304Z', 'text': '11 colors, pixelart', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 631615, 'ref': {'link': 'bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 570, 'width': 960, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:vpkdwwdia5etkdsuvsewtlws/app.bsky.feed.post/3lqx7wzy7c227', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei@jpeg', 'aspect_ratio': {'height': 570, 'width': 960, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 1342, 'quote_count': 6, 'reply_count': 12, 'repost_count': 303, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]
    mock_response = MagicMock()
    mock_response.model_dump_json.return_value = json.dumps(mock_json_response)
    mock_request = mocker.patch("atproto_client.namespaces.sync_ns.AppBskyFeedNamespace.get_posts", return_value=mock_response)
    mocker.patch("mdfb.core.fetch_post_details._merge_uri_chunk_to_records", return_value=mock_json_response)
    result = fetch_post_details.fetch_post_details([{"poster_post_uri" : "example1"}, {"poster_post_uri" : "example2"}])
    # print(result)
    assert result == [{'rkey': '3lqx7wzy7c227', 'text': '11 colors, pixelart', 'response': {'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.like/3lqxkvxe5mo2s'], 'feed_type': ['like'], 'poster_post_uri': 'at://did:plc:vpkdwwdia5etkdsuvsewtlws/app.bsky.feed.post/3lqx7wzy7c227', 'author': {'did': 'did:plc:vpkdwwdia5etkdsuvsewtlws', 'handle': 'inkpixels.bsky.social', 'associated': {'chat': {'allow_incoming': 'following', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreiauvzci74nrfoxhkfgd3mo6h75ejmzpxpyxlkbg2d5shflhy2ugmu@jpeg', 'created_at': '2023-08-20T15:22:50.198Z', 'display_name': 'ink', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreidhuc5hv3iiq26e6citq2d77xl5gwsf22mp6qpwtl73hdkjqy3jsa', 'indexed_at': '2025-06-06T16:11:06.061Z', 'record': {'created_at': '2025-06-06T16:11:02.304Z', 'text': '11 colors, pixelart', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 631615, 'ref': {'link': 'bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 570, 'width': 960, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:vpkdwwdia5etkdsuvsewtlws/app.bsky.feed.post/3lqx7wzy7c227', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:vpkdwwdia5etkdsuvsewtlws/bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei@jpeg', 'aspect_ratio': {'height': 570, 'width': 960, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 1342, 'quote_count': 6, 'reply_count': 12, 'repost_count': 303, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.like/3lqxkvxe5mo2s'], 'poster_post_uri': 'at://did:plc:vpkdwwdia5etkdsuvsewtlws/app.bsky.feed.post/3lqx7wzy7c227', 'feed_type': ['like'], 'did': 'did:plc:vpkdwwdia5etkdsuvsewtlws', 'handle': 'inkpixels.bsky.social', 'display_name': 'ink', 'media_type': ['image'], 'images_cid': ['bafkreienkhaa7gtudrria37tznq4bvwtnwmuavtizqoqlq5dxesbo4yhei'], 'mime_type': 'image/jpeg'}]
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
    result = fetch_post_details.fetch_post_details([{"poster_post_uri" : "at://did:example:1234"}])
    print(result)
    assert result == []
    mock_request.assert_called_once_with({"uris": ["at://did:example:1234"]})

def test_extract_media_image(mocker):
    mock_input = {'posts': [{'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigp3wgf36lhnb73plsy4xyfpaub5xvrntmmh4lga2z75reld3qnna', 'indexed_at': '2024-11-18T02:51:34.738Z', 'record': {'created_at': '2024-11-18T02:51:26.625Z', 'text': "me watching Bluesky's user count grow by another million in a day\n\nhello and welcome to all 19M of you! 🥳", 'embed': {'images': [{'alt': 'Kris Jenner looking at a computer', 'image': {'mime_type': 'image/jpeg', 'size': 232606, 'ref': {'link': 'bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 487, 'width': 702, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lb6vz4ms6c25', 'embed': {'images': [{'alt': 'Kris Jenner looking at a computer', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq@jpeg', 'aspect_ratio': {'height': 487, 'width': 702, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 222800, 'quote_count': 1063, 'reply_count': 5182, 'repost_count': 11589, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    print(json.dumps(mock_input, indent=4))
    mock_input = mock_input["posts"][0]["record"]["embed"]
    result = fetch_post_details._extract_media(mock_input)
    assert result == {'media_type': ['image'], 'images_cid': ['bafkreiddd7cus33yyfwxcjrzk77ka3zldjzt22wbvoimduwn2viuf2qdtq'], 'mime_type': 'image/jpeg'}

def test_extract_media_video(mocker):
    mock_input = {'posts': [{'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigah7oevnzroay2ve56w7pd3atscamiekwqbmj3kl66mvqsuydtwe', 'indexed_at': '2024-11-15T00:53:48.256Z', 'record': {'created_at': '2024-11-15T00:53:46.785Z', 'text': "it's official — 1,000,000 people have joined Bluesky in just the last day!!! \n\nwelcome and thank you for being here 🥳", 'embed': {'video': {'mime_type': 'video/mp4', 'size': 1787532, 'ref': {'link': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq'}, 'py_type': 'blob'}, 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'captions': None, 'py_type': 'app.bsky.embed.video'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lax5zxh7bc2p', 'embed': {'cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'playlist': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/playlist.m3u8', 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'thumbnail': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/thumbnail.jpg', 'py_type': 'app.bsky.embed.video#view'}, 'labels': [], 'like_count': 88777, 'quote_count': 1724, 'reply_count': 2380, 'repost_count': 8951, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}]}
    mock_input = mock_input["posts"][0]["record"]["embed"]
    result = fetch_post_details._extract_media(mock_input)
    assert result == {'media_type': ['video'], 'video_cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'mime_type': 'video/mp4'}

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
    mock_uri_chunk = [{"poster_post_uri" : "example1"}, {"poster_post_uri" : "example2"}]
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
    mock_uri_chunk = [{"poster_post_uri" : "example1"}, {"poster_post_uri" : "example2"}]
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