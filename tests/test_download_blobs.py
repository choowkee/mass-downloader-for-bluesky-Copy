import os
import pytest
from tenacity import stop_after_attempt, wait_none
from mdfb.core import download_blobs 
from mdfb.utils import constants


def test_truncate_filename(mocker):
    # this filename is more than 256 bytes
    filename = "投稿（まだプロフィールではない）内の日本語テキストを検索するためにいくつかの変更を加えました。 あなたがどう思うか興味があります！\n\n[この投稿は機械翻訳を使用しました][この投稿は機械翻訳を使用しました][この投稿は機械翻訳を使用しました]"
    result = download_blobs._truncate_filename(filename, 256)
    def is_valid_utf8(string: str) -> bool:
        try:
            string.encode('utf-8')
            return True
        except (UnicodeEncodeError, UnicodeDecodeError):
            return False
        
    assert len(result.encode("utf-8")) <= 256 
    assert is_valid_utf8(result)

def test_make_base_filename(mocker):
    filename_options = {
        "RKEY": "3213213mkmlk",
        "TEXT": "example_filename",
        "HANDLE": "handle_example"
    }
    filename_format_string = "{RKEY}_{TEXT}_{HANDLE}"
    result = download_blobs._make_base_filename(filename_options, filename_format_string)
    assert result == f"{filename_options["RKEY"]}_{filename_options["TEXT"]}_{filename_options["HANDLE"]}"

def test_append_extension_mime_type(mocker):
    mime_type = "image/jpeg"
    file_type = "jpeg"
    filename = "filename_example"
    result = download_blobs._append_extension(filename, mime_type=mime_type)
    assert result == f"{filename}.{file_type}"

def test_append_extension_i(mocker):
    i = 2
    filename = "filename_example"
    result = download_blobs._append_extension(filename, i=2) 
    assert result == f"{filename}_{i}"

def test_append_extension_full(mocker):
    mime_type = "image/jpeg"
    file_type = "jpeg"
    i = 2
    filename = "filename_example"
    result = download_blobs._append_extension(filename, mime_type=mime_type, i=i)
    assert result == f"{filename}_{i}.{file_type}"

def test_get_blob(mocker):
    mock_did = "did:example:1234"
    mock_cid = "example_1234"
    mock_filename = "example_filename"
    mock_filepath = "exmaple_filepath"
    mock_blob_data = b"0x3eb"
    mock_logger = mocker.patch("logging.Logger")
    mock_file_download = mocker.patch(
        "atproto_client.namespaces.sync_ns.ComAtprotoSyncNamespace.get_blob", 
        return_value = mock_blob_data
    )

    mocked_open = mocker.patch("builtins.open", mocker.mock_open())

    download_blobs._get_blob(mock_did, mock_cid, mock_filename, mock_filepath, mock_logger)
    mock_file_download.assert_called_once_with({
        "did": mock_did, 
        "cid": mock_cid, 
    })
    mocked_open.assert_called_once_with(os.path.join(mock_filepath, mock_filename), "wb")
    mocked_open().write.assert_called_once_with(mock_blob_data)

def test_get_blob_exceed_retries(monkeypatch: pytest.MonkeyPatch, mocker):
    mock_did = "did:example:1234"
    mock_cid = "example_1234"
    mock_filename = "example_filename"
    mock_filepath = "exmaple_filepath"
    mock_logger = mocker.patch("logging.Logger")
    mock_retires = 5
    
    mock_get_blob = mocker.patch("atproto_client.namespaces.sync_ns.ComAtprotoSyncNamespace.get_blob")
    mock_get_blob.side_effect = [
        Exception(),
        Exception(),
        Exception(),
        Exception(),
        Exception(),
        Exception(),
    ]
    
    mocker.patch("builtins.open", mocker.mock_open())
    
    monkeypatch.setattr(
        download_blobs._get_blob.retry,
        "stop",
        stop_after_attempt(mock_retires)
    )
    monkeypatch.setattr(
        download_blobs._get_blob.retry,
        "wait",
        wait_none()
    )
    with pytest.raises(Exception):
        download_blobs._get_blob(mock_did, mock_cid, mock_filename, mock_filepath, mock_logger)
    
    assert mock_get_blob.call_count == mock_retires
    
    mock_logger.error.assert_called_with(
        f"Error occured for downloading this file, DID: {mock_did}, CID: {mock_cid}", exc_info=True,
    )
    
    
def test_get_blob_success_with_some_retries(monkeypatch: pytest.MonkeyPatch, mocker):
    mock_did = "did:example:1234"
    mock_cid = "example_1234"
    mock_filename = "example_filename"
    mock_filepath = "exmaple_filepath"
    mock_logger = mocker.patch("logging.Logger")
    mock_retires = 5
    
    mock_get_blob = mocker.patch("atproto_client.namespaces.sync_ns.ComAtprotoSyncNamespace.get_blob")
    mock_get_blob.side_effect = [
        Exception(),
        Exception(),
        Exception(),
        Exception(),
        True
    ]
    
    mocker.patch("builtins.open", mocker.mock_open())
    
    monkeypatch.setattr(
        download_blobs._get_blob.retry,
        "stop",
        stop_after_attempt(mock_retires)
    )
    monkeypatch.setattr(
        download_blobs._get_blob.retry,
        "wait",
        wait_none()
    )

    response = download_blobs._get_blob(mock_did, mock_cid, mock_filename, mock_filepath, mock_logger)
    
    assert response == None 
    assert mock_get_blob.call_count == mock_retires

def test_get_blob_with_retries_failure(monkeypatch, mocker):
    mock_did = "did:example:1234"
    mock_cid = "example_1234"
    mock_filename = "example_filename"
    mock_filepath = "exmaple_filepath"
    mock_logger = mocker.patch("logging.Logger")
    mock_retries = 5
    
    mocker.patch("mdfb.core.download_blobs._get_blob", side_effect=Exception)
    
    monkeypatch.setattr(constants, "RETRIES", mock_retries)

    response = download_blobs._get_blob_with_retries(mock_did, mock_cid, mock_filename, mock_filepath, mock_logger)
    
    assert response == False
    mock_logger.error.assert_called_with(
        f"Error occured for downloading this file, DID: {mock_did}, CID: {mock_cid}, after {mock_retries} retires",
        exc_info=True
    )

def test_get_blob_with_retries_success(mocker):
    mock_did = "did:example:1234"
    mock_cid = "example_1234"
    mock_filename = "example_filename"
    mock_filepath = "exmaple_filepath"
    mock_logger = mocker.patch("logging.Logger")
    
    mocker.patch("mdfb.core.download_blobs._get_blob")
    
    response = download_blobs._get_blob_with_retries(mock_did, mock_cid, mock_filename, mock_filepath, mock_logger)
    
    assert response == True


    
def test_download_blob(mocker):
    mock_post = [{'rkey': '3lqwz2kuzg22s', 'text': '', 'response': {'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.repost/3lqx3fnspgj2s'], 'feed_type': ['repost'], 'poster_post_uri': 'at://did:plc:3eatnvb2dim4l7fiwln5wow6/app.bsky.feed.post/3lqwz2kuzg22s', 'author': {'did': 'did:plc:3eatnvb2dim4l7fiwln5wow6', 'handle': 'dailybunnies.bsky.social', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:3eatnvb2dim4l7fiwln5wow6/bafkreifz2jzlibiyldp75uhdi4xhlpww6khmkt5brpj2jif274kocrv3xq@jpeg', 'created_at': '2024-10-20T14:50:10.507Z', 'display_name': 'daily bunnies 🐇✨', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreib3qtxun7gvvpjaxk666xxvkltu2oyyghekziswhwtmutz6xz2vh4', 'indexed_at': '2025-06-06T14:07:49.256Z', 'record': {'created_at': '2025-06-06T14:07:44.486Z', 'text': '', 'embed': {'images': [{'alt': 'A small, white bunny stands on its hind legs, peering out a window. Lush green trees are visible outside.', 'image': {'mime_type': 'image/jpeg', 'size': 659942, 'ref': {'link': 'bafkreiamy7yinrdcqrqtka4xwhhkbcblv4zxudjlupslnuamzm6tbxofze'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 1440, 'width': 1080, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:3eatnvb2dim4l7fiwln5wow6/app.bsky.feed.post/3lqwz2kuzg22s', 'embed': {'images': [{'alt': 'A small, white bunny stands on its hind legs, peering out a window. Lush green trees are visible outside.', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:3eatnvb2dim4l7fiwln5wow6/bafkreiamy7yinrdcqrqtka4xwhhkbcblv4zxudjlupslnuamzm6tbxofze@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:3eatnvb2dim4l7fiwln5wow6/bafkreiamy7yinrdcqrqtka4xwhhkbcblv4zxudjlupslnuamzm6tbxofze@jpeg', 'aspect_ratio': {'height': 1440, 'width': 1080, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 4187, 'quote_count': 35, 'reply_count': 68, 'repost_count': 386, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.repost/3lqx3fnspgj2s'], 'poster_post_uri': 'at://did:plc:3eatnvb2dim4l7fiwln5wow6/app.bsky.feed.post/3lqwz2kuzg22s', 'feed_type': ['repost'], 'did': 'did:plc:3eatnvb2dim4l7fiwln5wow6', 'handle': 'dailybunnies.bsky.social', 'display_name': 'daily bunnies 🐇✨', 'media_type': ['image'], 'images_cid': ['bafkreiamy7yinrdcqrqtka4xwhhkbcblv4zxudjlupslnuamzm6tbxofze'], 'mime_type': 'image/jpeg'}, {'rkey': '3lqdrbvwmg22g', 'text': '', 'response': {'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.repost/3lqg4s7fcmd2b'], 'feed_type': ['repost'], 'poster_post_uri': 'at://did:plc:crks6dwob3t4sxgqcqldeh3u/app.bsky.feed.post/3lqdrbvwmg22g', 'author': {'did': 'did:plc:crks6dwob3t4sxgqcqldeh3u', 'handle': 'catwomanresists.bsky.social', 'associated': {'chat': {'allow_incoming': 'all', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:crks6dwob3t4sxgqcqldeh3u/bafkreia5bxhaukvsgjjgidllsxxnhozvjz2in2vbteuph5jsjd3nw346dm@jpeg', 'created_at': '2024-10-20T19:06:59.707Z', 'display_name': 'CatWoman', 'labels': [{'cts': '2024-10-20T19:06:58.620Z', 'src': 'did:plc:crks6dwob3t4sxgqcqldeh3u', 'uri': 'at://did:plc:crks6dwob3t4sxgqcqldeh3u/app.bsky.actor.profile/self', 'val': '!no-unauthenticated', 'cid': 'bafyreifif3zhzqu4c34f7fy3hrmhggugr2r3edo6pmynfuz7b6s5cukdgq', 'exp': None, 'neg': None, 'sig': None, 'ver': None, 'py_type': 'com.atproto.label.defs#label'}], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreidkaqm5afzmds4feiyde6mblrnv3j2arsyzw26d5i45omybeabz5m', 'indexed_at': '2025-05-29T22:28:09.449Z', 'record': {'created_at': '2025-05-29T22:28:05.992Z', 'text': '', 'embed': {'images': [{'alt': '', 'image': {'mime_type': 'image/jpeg', 'size': 506632, 'ref': {'link': 'bafkreibzwwd2a57di3i4rx6xcz52cebiw6whh6bc2d42666v7fgamkcwl4'}, 'py_type': 'blob'}, 'aspect_ratio': {'height': 697, 'width': 527, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#image'}], 'py_type': 'app.bsky.embed.images'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:crks6dwob3t4sxgqcqldeh3u/app.bsky.feed.post/3lqdrbvwmg22g', 'embed': {'images': [{'alt': '', 'fullsize': 'https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:crks6dwob3t4sxgqcqldeh3u/bafkreibzwwd2a57di3i4rx6xcz52cebiw6whh6bc2d42666v7fgamkcwl4@jpeg', 'thumb': 'https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:crks6dwob3t4sxgqcqldeh3u/bafkreibzwwd2a57di3i4rx6xcz52cebiw6whh6bc2d42666v7fgamkcwl4@jpeg', 'aspect_ratio': {'height': 697, 'width': 527, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'py_type': 'app.bsky.embed.images#viewImage'}], 'py_type': 'app.bsky.embed.images#view'}, 'labels': [], 'like_count': 18709, 'quote_count': 201, 'reply_count': 542, 'repost_count': 4668, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'user_did': 'did:plc:u6iyyil77bqv5fknwauj3tfk', 'user_post_uri': ['at://did:plc:u6iyyil77bqv5fknwauj3tfk/app.bsky.feed.repost/3lqg4s7fcmd2b'], 'poster_post_uri': 'at://did:plc:crks6dwob3t4sxgqcqldeh3u/app.bsky.feed.post/3lqdrbvwmg22g', 'feed_type': ['repost'], 'did': 'did:plc:crks6dwob3t4sxgqcqldeh3u', 'handle': 'catwomanresists.bsky.social', 'display_name': 'CatWoman', 'media_type': ['image'], 'images_cid': ['bafkreibzwwd2a57di3i4rx6xcz52cebiw6whh6bc2d42666v7fgamkcwl4'], 'mime_type': 'image/jpeg'}]
    post_response = mock_post[1]["response"]
    mock_filepath = "example"
    mock_filename = "example_filename_1"

    mock_progress_bar = mocker.patch(
        "tqdm.tqdm"
    ) 

    mocker.patch(
        "mdfb.core.download_blobs._make_base_filename",
        return_value=mock_filename
    )
    mocker.patch(
        "mdfb.core.download_blobs._get_blob"
    )

    mocked_open = mocker.patch("builtins.open", mocker.mock_open())
    mock_json_dump = mocker.patch("json.dump") 
    
    download_blobs.download_blobs(mock_post, mock_filepath, mock_progress_bar)
    
    mocked_open.assert_any_call(f"{os.path.join(mock_filepath, mock_filename)}.json", "wt")
    dump_call_args = mock_json_dump.call_args
    data_written = dump_call_args[0][0]
    file_object = dump_call_args[0][1]
    indent_value = dump_call_args[1]["indent"]
    assert data_written == post_response
    assert file_object == mocked_open()
    assert indent_value == 4