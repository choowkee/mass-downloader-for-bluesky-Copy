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
    assert is_valid_utf8(result) == True

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
        f"Error occured for downloading this file, DID: {mock_did}, CID: {mock_cid}",
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
    mock_post = [{'rkey': '3lax5zxh7bc2p', 'response': {'author': {'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'associated': {'chat': {'allow_incoming': 'none', 'py_type': 'app.bsky.actor.defs#profileAssociatedChat'}, 'feedgens': None, 'labeler': None, 'lists': None, 'starter_packs': None, 'py_type': 'app.bsky.actor.defs#profileAssociated'}, 'avatar': 'https://cdn.bsky.app/img/avatar/plain/did:plc:z72i7hdynmk6r22z27h6tvur/bafkreihagr2cmvl2jt4mgx3sppwe2it3fwolkrbtjrhcnwjk4jdijhsoze@jpeg', 'created_at': '2023-04-12T04:53:57.057Z', 'display_name': 'Bluesky', 'labels': [], 'viewer': None, 'py_type': 'app.bsky.actor.defs#profileViewBasic'}, 'cid': 'bafyreigah7oevnzroay2ve56w7pd3atscamiekwqbmj3kl66mvqsuydtwe', 'indexed_at': '2024-11-15T00:53:48.256Z', 'record': {'created_at': '2024-11-15T00:53:46.785Z', 'text': "it's official — 1,000,000 people have joined Bluesky in just the last day!!! \n\nwelcome and thank you for being here 🥳", 'embed': {'video': {'mime_type': 'video/mp4', 'size': 1787532, 'ref': {'link': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq'}, 'py_type': 'blob'}, 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'captions': None, 'py_type': 'app.bsky.embed.video'}, 'entities': None, 'facets': None, 'labels': None, 'langs': ['en'], 'reply': None, 'tags': None, 'py_type': 'app.bsky.feed.post'}, 'uri': 'at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.post/3lax5zxh7bc2p', 'embed': {'cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'playlist': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/playlist.m3u8', 'alt': 'Barbie smiling, clapping, and jumping up and down', 'aspect_ratio': {'height': 640, 'width': 1280, 'py_type': 'app.bsky.embed.defs#aspectRatio'}, 'thumbnail': 'https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq/thumbnail.jpg', 'py_type': 'app.bsky.embed.video#view'}, 'labels': [], 'like_count': 88769, 'quote_count': 1724, 'reply_count': 2382, 'repost_count': 8949, 'threadgate': None, 'viewer': None, 'py_type': 'app.bsky.feed.defs#postView'}, 'did': 'did:plc:z72i7hdynmk6r22z27h6tvur', 'handle': 'bsky.app', 'display_name': 'Bluesky', 'text': "it's official — 1,000,000 people have joined Bluesky in just the last day!!! \n\nwelcome and thank you for being here 🥳", 'video_cid': 'bafkreic5qzdlpdt6gqakxzx27lsp6phmltk2fv6bpyfleoumo4nxytvleq', 'mime_type': 'video/mp4'}]
    post_response = mock_post[0]["response"]
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
    
    mocked_open.assert_called_once_with(f"{os.path.join(mock_filepath, mock_filename)}.json", "wt")
    mock_json_dump.assert_called_once()
    dump_call_args = mock_json_dump.call_args
    data_written = dump_call_args[0][0]
    file_object = dump_call_args[0][1]
    indent_value = dump_call_args[1]["indent"]
    assert data_written == post_response
    assert file_object == mocked_open()
    assert indent_value == 4