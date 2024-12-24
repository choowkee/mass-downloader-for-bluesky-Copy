import json
from unittest.mock import MagicMock
import pytest
from mdfb.core.get_post_identifiers import get_post_identifiers

def test_get_post_identifiers_liked(mocker):
    mock_json_response = {
        "records": [
            {
                "uri": "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.like/3ld7z46debo2g",
                "cid": "bafyreic5s6gfkaogfwljvhxnzxer4xslfopwf26f5qtwakluypzrppamye",
                "value": {
                    "$type": "app.bsky.feed.like",
                    "subject": {
                        "cid": "bafyreifp4vomoqhxritmilksydl4iixlnqnmwhau4nnprs7dvl4xy6gmqi",
                        "uri": "at://did:plc:xlqcxpk53spbhlypj6wmvvke/app.bsky.feed.post/3ld6bzuenjs2a"
                    },
                    "createdAt": "2024-12-14T00:09:53.185Z"
                }
            },
            {
                "uri": "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.like/3lbxh76jfuq2y",
                "cid": "bafyreicksfgvc25tkcobipwirruxyumk2ichbx6fd2zllcki7z6bdzhs3e",
                "value": {
                    "$type": "app.bsky.feed.like",
                    "subject": {
                        "cid": "bafyreiabo7kerzlewo33l4y6qqdwgmpjhrpi6yagf7fvzn2tepnstn2lie",
                        "uri": "at://did:plc:vc7f4oafdgxsihk4cry2xpze/app.bsky.feed.post/3lbxe3z66hk2e"
                    },
                    "createdAt": "2024-11-27T21:02:56.996Z"
                }
            }
        ],
        "cursor": "3lbxh76jfuq2y"
    }
    mock_response = MagicMock()
    mock_response.model_dump_json.return_value = json.dumps(mock_json_response)
    mock_request = mocker.patch("atproto_client.namespaces.sync_ns.ComAtprotoRepoNamespace.list_records", return_value=mock_response)
    result = get_post_identifiers("did:example:1234", 2, "like")
    assert result == ['at://did:plc:xlqcxpk53spbhlypj6wmvvke/app.bsky.feed.post/3ld6bzuenjs2a', 'at://did:plc:vc7f4oafdgxsihk4cry2xpze/app.bsky.feed.post/3lbxe3z66hk2e']
    mock_request.assert_called_once_with({'collection': 'app.bsky.feed.like', 'repo': 'did:example:1234', 'limit': 2, 'cursor': ''})

def test_get_post_identifiers_no_likes(mocker):
    mock_json_response = {
                        "records": []
                    }
    mock_response = MagicMock()
    mock_response.model_dump_json.return_value = json.dumps(mock_json_response)
    mocker.patch("atproto_client.namespaces.sync_ns.ComAtprotoRepoNamespace.list_records", return_value=mock_response)
    result = get_post_identifiers("did:example:1234", 2, "like")
    assert result == []
