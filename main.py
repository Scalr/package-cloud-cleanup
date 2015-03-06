#coding:utf-8
import os
import re
import sys
import json
import itertools
import posixpath
import logging

import requests
from StringIO import StringIO
from requests.auth import HTTPBasicAuth

from debian import deb822, debfile
from repodataParser.RepoParser import Parser

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(asctime)s %(name)s %(message)s")
logging.getLogger('requests').setLevel(logging.WARNING)

HIGHEST_CHAR = chr(127)
LOWEST_CHAR = chr(0)


class ParserWithRequests(Parser):
    def __init__(self, session, url):
        self.session = session
        self.url = url
        Parser.__init__(self, url=url)

    # Library does name mangling.
    def _Parser__open(self):
        self.res = self.session.get(self.url, headers={"User-Agent": "curl/7.37.1"}).content

def get_vesion_tuple(version, iteration):
    v_list = []

    def str_processor(s):
        if not s.endswith('~'):
            s += HIGHEST_CHAR
        return s.replace('~', LOWEST_CHAR)

    for processor, matcher in itertools.cycle([
        (str_processor, re.compile('^(\D*)(.*)$')),
        (int, re.compile('^(\d*)(.*)$'))
    ]):

        if not version:
            break

        bit, version = matcher.match(version).groups()

        if bit:
            v_list.append(processor(bit))

    v_list.append(iteration)
    return tuple(v_list)


PACKAGE_CLOUD_CONFIG = os.path.expanduser('~/.packagecloud')
API_URL = "https://packagecloud.io/api/v1/"

USER_NAME = "scalr"
PKG_NAMES = ("scalr-manage", "scalr-server")
KEEP_PKGS = 2

REPOS = ("scalr-manage", "scalr-manage-a", "scalr-server-oss", "scalr-server-ee")

DEB_PLATFORMS = ("ubuntu/precise", "ubuntu/trusty", "debian/wheezy", "debian/jessie")
DEB_ARCHS = ("binary-amd64",)
DEB_PKG_TPL = "https://packagecloud.io/scalr/{repo}/{os}/dists/{release}/main/{arch}/Packages"
# ^^^^^^^^^^^^ TODO - use USER_NAME here


def deb_extract_orderable_version(deb):
    deb_version = deb["Version"].decode('utf-8')
    deb_version.replace('~', HIGHEST_CHAR)

    if "-" in deb_version:
        version, iteration = deb_version.split('-')
    else:
        version, iteration = deb_version, '1'
    return get_vesion_tuple(version, iteration)

def deb_pretty_name(deb):
    return posixpath.basename(deb["Filename"])


RPM_PLATFORMS = ("el/6", "el/7", "ol/6", "ol/7")
RPM_ARCHS = ("x86_64",)
RPM_PRIMARY_TPL = "https://packagecloud.io/scalr/{repo}/{platform}/{arch}/repodata/primary.xml.gz"
# ^^^^^^^^^^^^ TODO - use USER_NAME here

def rpm_extract_orderable_version(rpm):
    version = rpm["version"][1]
    ver, rel = version["ver"], version["rel"]
    return get_vesion_tuple(version["ver"], version["rel"])


def rpm_pretty_name(rpm):
    return posixpath.basename(rpm["location"][1]["href"])


def main(api_session, client_session):
    # Start with Debs
    # TODO - Abstract this!
    for repo, platform, arch in itertools.product(REPOS, DEB_PLATFORMS, DEB_ARCHS):
        logger = logging.getLogger(".".join([repo, platform, arch]))
        logger.debug("Process: %s/%s/%s", repo, platform, arch)

        os, release = platform.split("/")
        res = client_session.get(DEB_PKG_TPL.format(repo=repo, os=os, release=release, arch=arch), stream=True)
        all_pkgs = list(deb822.Packages.iter_paragraphs(res.iter_lines()))

        for pkg_name in PKG_NAMES:
            pkgs = [pkg for pkg in all_pkgs if pkg["Package"] == pkg_name]
            pkgs.sort(key=deb_extract_orderable_version, reverse=True)

            logger.info("%s: found %s package(s)", pkg_name, len(pkgs))
            if len(pkgs) <= KEEP_PKGS:
                continue

            for pkg in pkgs[KEEP_PKGS:]:
                logger.warning("%s: deleting %s", pkg_name, deb_pretty_name(pkg))
                del_file = posixpath.basename(pkg["Filename"])
                res = api_session.delete("/".join([API_URL, "repos", USER_NAME, repo, platform, del_file]))
                res.raise_for_status()
                if "error" in res.json():
                    logger.error("%s: failed to delete %s (%s)", pkg, del_file, res.text)

    # Now, do EL
    for repo, platform, arch in itertools.product(REPOS, RPM_PLATFORMS, RPM_ARCHS):
        logger = logging.getLogger(".".join([repo, platform, arch]))
        logger.debug("Process: %s/%s/%s", repo, platform, arch)

        repodata = ParserWithRequests(client_session, RPM_PRIMARY_TPL.format(repo=repo, platform=platform, arch=arch))
        all_pkgs = list(repodata.getList())

        for pkg_name in PKG_NAMES:
            pkgs = [pkg for pkg in all_pkgs if pkg["name"][0] == pkg_name]
            pkgs.sort(key=rpm_extract_orderable_version, reverse=True)

            logger.info("%s: found %s package(s)", pkg_name, len(pkgs))
            if len(pkgs) <= KEEP_PKGS:
                continue

            for pkg in pkgs[KEEP_PKGS:]:
                logger.warning("%s: deleting %s", pkg_name, rpm_pretty_name(pkg))
                del_file = posixpath.basename(pkg["location"][1]["href"])
                res = api_session.delete("/".join([API_URL, "repos", USER_NAME, repo, platform, del_file]))
                res.raise_for_status()
                if "error" in res.json():
                    logger.error("%s: failed to delete %s (%s)", pkg, del_file, res.text)

if __name__ == "__main__":
    api_token = os.environ["API_TOKEN"]
    api_session = requests.Session()
    api_session.auth = HTTPBasicAuth(api_token, "")

    client_token = os.environ["CLIENT_TOKEN"]
    client_session = requests.Session()
    client_session.auth = HTTPBasicAuth(client_token, "")

    main(api_session, client_session)

