#coding:utf-8
import itertools
import posixpath

import requests
from StringIO import StringIO
from requests.auth import HTTPBasicAuth

from debian import deb822, debfile
from repodataParser.RepoParser import Parser



API_URL = "https://packagecloud.io/api/v1/"

USER_NAME = "scalr"
PKG_NAME = "scalr-manage"
KEEP_PKGS = 2

UBUNTU_RELEASES = ("precise", "trusty")
UBUNTU_ARCHS = ("binary-amd64",)
UBUNTU_PKG_TPL = "https://packagecloud.io/scalr/scalr-manage/ubuntu/dists/{release}/main/{arch}/Packages"
# ^^^^^^^^^^^^ TODO - use USER_NAME and PKG_NAME here

EL_RELEASES = ("6", "7")
EL_ARCHS = ("x86_64",)
EL_PRIMARY_TPL = "https://packagecloud.io/scalr/scalr-manage/el/{release}/{arch}/repodata/primary.xml.gz"
# ^^^^^^^^^^^^ TODO - use USER_NAME and PKG_NAME here


def main(session):
    # Start with Ubuntu
    for release, arch in itertools.product(UBUNTU_RELEASES, UBUNTU_ARCHS):
        print "PROCESSING {0}/{1}".format(release, arch)
        res = session.get(UBUNTU_PKG_TPL.format(release=release, arch=arch), stream=True)
        pkgs = deb822.Packages.iter_paragraphs(res.iter_lines())

        scalr_manage_pkgs = [pkg for pkg in pkgs if pkg["Package"] == PKG_NAME]
        scalr_manage_pkgs.sort(key=lambda pkg: map(int, pkg["Version"].split(".")), reverse=True)

        # TODO - Abstract this!

        if len(scalr_manage_pkgs) <= KEEP_PKGS:
            print "Only {0} packages in {1} / {2}".format(len(scalr_manage_pkgs), release, arch)
            continue

        del_pkgs = scalr_manage_pkgs[KEEP_PKGS:]

        for pkg in scalr_manage_pkgs:
            print "CDEL" if pkg in del_pkgs else "KEEP", pkg["Package"], pkg["Version"]

        raw_input("Proceed? (CTRL+C to abort)")

        for pkg in del_pkgs:
            del_file = posixpath.basename(pkg["Filename"])
            print "XDEL", pkg["Package"], pkg["Version"], del_file
            res = session.delete("/".join([API_URL, "repos", USER_NAME, PKG_NAME, "ubuntu", release, del_file]))
            res.raise_for_status()
            if "error" in res.json():
                print "FAILED TO DELETE", res.text

    # Now, do EL
    for release, arch in itertools.product(EL_RELEASES, EL_ARCHS):
        print "PROCESSING {0}/{1}".format(release, arch)
        repodata = Parser(url=EL_PRIMARY_TPL.format(release=release, arch=arch))

        scalr_manage_pkgs = [pkg for pkg in repodata.getList() if pkg["name"][0] == PKG_NAME]
        scalr_manage_pkgs.sort(key=lambda pkg: (map(int, pkg["version"][1]["ver"].split(".")), int(pkg["version"][1]["epoch"])), reverse=True)

        # TODO - Abstract this!

        if len(scalr_manage_pkgs) <= KEEP_PKGS:
            print "Only {0} packages in {1} / {2}".format(len(scalr_manage_pkgs), release, arch)
            continue

        del_pkgs = scalr_manage_pkgs[KEEP_PKGS:]

        for pkg in scalr_manage_pkgs:
            print "CDEL" if pkg in del_pkgs else "KEEP", pkg["name"][0], pkg["version"][1]["ver"], pkg["version"][1]["epoch"]

        raw_input("Proceed? (CTRL+C to abort)")

        for pkg in del_pkgs:
            del_file = posixpath.basename(pkg["location"][1]["href"])
            print "XDEL", pkg["name"][0], pkg["version"][1]["ver"], pkg["version"][1]["epoch"], del_file
            res = session.delete("/".join([API_URL, "repos", USER_NAME, PKG_NAME, "el", release, del_file]))
            res.raise_for_status()
            if "error" in res.json():
                print "FAILED TO DELETE", res.text

if __name__ == "__main__":
    TOKEN = ""
    session = requests.Session()
    session.auth = HTTPBasicAuth(TOKEN, "")
    main(session)
