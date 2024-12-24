# mass-downloader-for-bluesky

mass-downloader-for-bluesky (mdfb) is a Python cli application that can download large amounts of posts from bluesky from any given account.

## Installation

You will need [Python](https://www.python.org/downloads/) to be installed to use this CLI.

You can install via pip by:
```bash
pip install mdfb
```

### Manual

Have [Poetry](https://python-poetry.org/) installed. 

Then clone the project, open a poetry shell and then install all dependencies.


```bash
git clone git@github.com:IbrahimHajiAbdi/mass-downloader-for-bluesky.git
cd mdfb
poetry shell
poetry install
```

## Usage
``mdfb`` works by using the public API offered by bluesky to retrieve posts liked, reposted or posted by the desired account. 

``mdfb`` will download the information for a post and the accompanying media, video or image(s). If there is no image(s) or video, it will just download the information of the post. The information of the post will be a JSON file and have lots of accompanying data, such as the text in the post, creation time of the post and author details. Currently, the retrieved posts start from the latest post to the oldest.

You will need to be inside a poetry shell to use ``mdfb`` if installed manually

### Examples

Some example commands would be:

#### Linux
```bash
mdfb --handle bsky.app -l 10 --like ./media/
```

#### Windows
```bash
mdfb --handle bsky.app -l 100 --like --repost --post ./media/
```

### Naming Convention
``mdfb``'s naming convention is: ``"{rkey}_{handle}_{text}"``, if it is downloading a post with multiple images then the naming will be: ``"{rkey}_{handle}_{text}_{i}"``, where "i" represents the order of the images in the post ranging from 1 - 4. In addition, the filenames are limited to 256 bytes and will be truncated down to that size. 

### Download Amount
When specifying the limit, this will be true for all types of post downloaded. For example: 
```bash
mdfb --handle bsky.app -l 100 --like --repost --post ./media/
```
This would download 100 likes, reposts and post, totalling 300 posts downloaded.

### Note
The maximum number of threads is currently 3, that can be changed in the ``mdfb/utils/constants.py`` file. Furthermore, there are more constants that can be changed in that file, such as delay between each request and the number of retires before marking that post as a failure and continuing.

## Options
- ``--handle``
  - The handle of the target account.
- ``--did, -d``
  - The DID of the target account. 
- ``--limit, -l``
  - The amount of posts that want to be downloaded. **Required**.
- ``directory``
  - Positional argument, where all the downloaded files are to be located. **Required**.
- ``--threads``
  - The amount of threads wanted to download posts more efficiently, maximum number of threads is 3.
- ``--like``
  - To retrieved liked posts
- ``--repost``
  - To retrieved reposts
- ``--post``
  - To retrieved posts
### Note
At least one of the flags: ``--like``, ``--repost``, ``--post`` is **required**.
In addition, ``--did, -d`` and ``--handle`` are mutually exclusive, and at least one of them is **required** as well. 