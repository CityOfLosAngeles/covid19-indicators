"""
To upload first version of HTML.

Put this into terminal.
"""

import os

TOKEN = os.environ["GITHUB_TOKEN_PASSWORD"]

curl -i -X PUT -H f'Authorization: token {TOKEN}' -d '{"path": "us-county-trends.html", "message": "test", "content": "asdfasdf", "branch": "gh-pages", "sha\": $(curl -X GET https://api.github.com/repos/CityofLosAngeles/covid19-indicators/contents/us-county-trends.html)}' https://api.github.com/repos/CityofLosAngeles/covid19-indicators/contents/us-county-trends.html