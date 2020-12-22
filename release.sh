#!/bin/bash
if [ -z "$1" ]
then
    echo usage: $0 version
    exit 1
fi

set -euxo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
version="$1"

echo "checking for version ($version) in datawelder/version.py"
grep -F "__version__ = '$version'" datawelder/version.py

#
# https://stackoverflow.com/questions/3878624/how-do-i-programmatically-determine-if-there-are-uncommited-changes
#
echo checking for uncommitted changes...
git diff-index HEAD --quiet --

tag="v$version"
echo checking for existing tag...  you must run \"git tag -d $tag\" if it already exists
git tag "$tag"

git push origin master --tags
