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

REPOS = ("scalr-manage", "scalr-manage-a", "scalr-server", "scalr-server-ee")

UBUNTU_RELEASES = ("precise", "trusty")
UBUNTU_ARCHS = ("binary-amd64",)
UBUNTU_PKG_TPL = "https://packagecloud.io/scalr/{repo}/ubuntu/dists/{release}/main/{arch}/Packages"
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


EL_RELEASES = ("6", "7")
EL_ARCHS = ("x86_64",)
EL_PRIMARY_TPL = "https://packagecloud.io/scalr/{repo}/el/{release}/{arch}/repodata/primary.xml.gz"
# ^^^^^^^^^^^^ TODO - use USER_NAME here

def rpm_extract_orderable_version(rpm):
    version = rpm["version"][1]
    ver, rel = version["ver"], version["rel"]
    return get_vesion_tuple(version["ver"], version["rel"])


def rpm_pretty_name(rpm):
    return posixpath.basename(rpm["location"][1]["href"])


def main(api_session, client_session):
    # Start with Ubuntu
    # TODO - Abstract this!
    for repo, release, arch in itertools.product(REPOS, UBUNTU_RELEASES, UBUNTU_ARCHS):
        logger = logging.getLogger(".".join([repo, "ubuntu", release, arch]))
        logger.debug("Process: %s/ubuntu/%s/%s", repo, release, arch)

        res = client_session.get(UBUNTU_PKG_TPL.format(repo=repo, release=release, arch=arch), stream=True)
        pkgs = deb822.Packages.iter_paragraphs(res.iter_lines())

        for pkg_name in PKG_NAMES:
            pkgs = [pkg for pkg in pkgs if pkg["Package"] == pkg_name]
            pkgs.sort(key=deb_extract_orderable_version, reverse=True)

            logger.info("%s: found %s package(s)", pkg_name, len(pkgs))
            if len(pkgs) <= KEEP_PKGS:
                continue

            del_pkgs = pkgs[KEEP_PKGS:]

            for pkg in del_pkgs:
                del_file = posixpath.basename(pkg["Filename"])
                logger.warning("%s: deleting %s", pkg, del_file)
                res = api_session.delete("/".join([API_URL, "repos", USER_NAME, repo, "ubuntu", release, del_file]))
                res.raise_for_status()
                if "error" in res.json():
                    logger.error("%s: failed to delete %s (%s)", pkg, del_file, res.text)

    # Now, do EL
    for repo, release, arch in itertools.product(REPOS, EL_RELEASES, EL_ARCHS):
        logger = logging.getLogger(".".join([repo, "ubuntu", release, arch]))
        logger.debug("Process: %s/el/%s/%s", repo, release, arch)
        repodata = ParserWithRequests(client_session, EL_PRIMARY_TPL.format(repo=repo, release=release, arch=arch))

        for pkg_name in PKG_NAMES:
            pkgs = [pkg for pkg in repodata.getList() if pkg["name"][0] == pkg_name]
            pkgs.sort(key=rpm_extract_orderable_version, reverse=True)

            logger.info("%s: found %s package(s)", pkg_name, len(pkgs))
            if len(pkgs) <= KEEP_PKGS:
                continue

            for pkg in del_pkgs:
                del_file = posixpath.basename(pkg["location"][1]["href"])
                logger.warning("%s: deleting %s", pkg, del_file)
                res = api_session.delete("/".join([API_URL, "repos", USER_NAME, repo, "el", release, del_file]))
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

