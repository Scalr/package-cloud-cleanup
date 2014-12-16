#coding:utf-8
import os
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


logger = logging.getLogger(__name__)

HIGHEST_NUMBER = float("inf")
HIGHEST_CHAR = "\xff"

def get_vesion_tuple(version, iteration):
    for pre_release_sep in ("~", "-"):
        if pre_release_sep in version:
            final, test = version.split(pre_release_sep, 1)
            special, index = test.split('.', 1)
            extra = (special, int(index))
            break
    else:
        # Ensure that pre-releases always rank lower than actual releases
        final, extra = version, (HIGHEST_NUMBER, HIGHEST_CHAR)

    return tuple([int(v) for v in final.split('.', 3)]) + extra + (iteration,)


PACKAGECLOUD_CONFIG = os.path.expanduser('~/.packagecloud')
API_URL = "https://packagecloud.io/api/v1/"

USER_NAME = "scalr"
PKG_NAME = "scalr-manage"
KEEP_PKGS = 2

REPOS = ("scalr-manage", "scalr-manage-a", "scalr-manage-b",)

UBUNTU_RELEASES = ("precise", "trusty")
UBUNTU_ARCHS = ("binary-amd64",)
UBUNTU_PKG_TPL = "https://packagecloud.io/scalr/{repo}/ubuntu/dists/{release}/main/{arch}/Packages"
# ^^^^^^^^^^^^ TODO - use USER_NAME here


def deb_extract_orderable_version(deb):
    deb_version = deb["Version"]
    if "-" in deb_version:
        if "~" in deb_version:
            # New format for pre-releases
            version, iteration = deb_version.rsplit('-')
        else:
            head, tail = deb_version.rsplit('-')
            if tail.isdigit():
                # New format for actual releases
                version, iteration = head, tail
            else:
                # Old format for pre-release
                version, iteration = deb_version, '1'
    else:
        # Old format for actual releases
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
    return get_vesion_tuple(version["ver"], version["rel"])


def rpm_pretty_name(rpm):
    return posixpath.basename(rpm["location"][1]["href"])


def main(session):
    # Start with Ubuntu
    for repo, release, arch in itertools.product(REPOS, UBUNTU_RELEASES, UBUNTU_ARCHS):
        print
        print "PROCESSING {0}/{1}/{2}".format(repo, release, arch)
        res = session.get(UBUNTU_PKG_TPL.format(repo=repo, release=release, arch=arch), stream=True)
        pkgs = deb822.Packages.iter_paragraphs(res.iter_lines())

        scalr_manage_pkgs = [pkg for pkg in pkgs if pkg["Package"] == PKG_NAME]
        scalr_manage_pkgs.sort(key=deb_extract_orderable_version, reverse=True)

        # TODO - Abstract this!

        if len(scalr_manage_pkgs) <= KEEP_PKGS:
            print "Only {0} packages in {1}/{2}/{3}".format(len(scalr_manage_pkgs), repo, release, arch)
            continue

        del_pkgs = scalr_manage_pkgs[KEEP_PKGS:]

        for pkg in scalr_manage_pkgs:
            print "CDEL" if pkg in del_pkgs else "KEEP", deb_pretty_name(pkg)

        raw_input("Proceed? (CTRL+C to abort)")

        for pkg in del_pkgs:
            del_file = posixpath.basename(pkg["Filename"])
            print "XDEL", deb_pretty_name(pkg)
            res = session.delete("/".join([API_URL, "repos", USER_NAME, repo, "ubuntu", release, del_file]))
            res.raise_for_status()
            if "error" in res.json():
                print "FAILED TO DELETE", res.text

    # Now, do EL
    for repo, release, arch in itertools.product(REPOS, EL_RELEASES, EL_ARCHS):
        print
        print "PROCESSING {0}/{1}/{2}".format(repo, release, arch)
        repodata = Parser(url=EL_PRIMARY_TPL.format(repo=repo, release=release, arch=arch))

        scalr_manage_pkgs = [pkg for pkg in repodata.getList() if pkg["name"][0] == PKG_NAME]
        scalr_manage_pkgs.sort(key=rpm_extract_orderable_version, reverse=True)

        if len(scalr_manage_pkgs) <= KEEP_PKGS:
            print "Only {0} packages in {1}/{2}/{3}".format(len(scalr_manage_pkgs), repo, release, arch)
            continue

        del_pkgs = scalr_manage_pkgs[KEEP_PKGS:]

        for pkg in scalr_manage_pkgs:
            print "CDEL" if pkg in del_pkgs else "KEEP", rpm_pretty_name(pkg)

        raw_input("Proceed? (CTRL+C to abort)")

        for pkg in del_pkgs:
            del_file = posixpath.basename(pkg["location"][1]["href"])
            print "XDEL", rpm_pretty_name(pkg)
            res = session.delete("/".join([API_URL, "repos", USER_NAME, repo, "el", release, del_file]))
            res.raise_for_status()
            if "error" in res.json():
                print "FAILED TO DELETE", res.text

if __name__ == "__main__":
    try:
        with open(PACKAGECLOUD_CONFIG) as f:
                token = json.load(f)["token"]
    except (IOError, KeyError, ValueError):
        logging.exception("Failed to open or parse packagecloud config!")
    else:
        session = requests.Session()
        session.auth = HTTPBasicAuth(token, "")
        main(session)

