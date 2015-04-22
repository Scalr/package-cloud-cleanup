# coding:utf-8
from repodataParser.RepoParser import Parser

from ._base import BaseRepoAdapter
from ._util import get_version_tuple


RPM_PRIMARY_TPL = "https://packagecloud.io/{user}/{repo}/{platform}/{arch}/repodata/primary.xml.gz"


class ParserWithRequests(Parser):
    def __init__(self, session, url):
        self.session = session
        self.url = url
        Parser.__init__(self, url=url)

    # Library does name mangling.
    def _Parser__open(self):
        r = self.session.get(self.url, headers={"User-Agent": "curl/7.37.1"})
        r.raise_for_status()
        self.res = r.content


class RpmRepoAdapter(BaseRepoAdapter):
    def _get_platforms(self):
        return ["el/6", "el/7", "ol/6", "ol/7"]

    def _get_archs(self):
        return ["x86_64",]

    def _extract_file_name(self, pkg):
        return pkg["location"][1]["href"]

    def _extract_pkg_name(self, pkg):
        return pkg["name"][0]

    def _extract_orderable_version(self, pkg):
        version = pkg["version"][1]
        ver, rel = version["ver"], version["rel"]
        return get_version_tuple(ver, rel)

    def _fetch_package_list(self, platform, arch):
        repodata = ParserWithRequests(self.client_session, RPM_PRIMARY_TPL.format(user=self.user, repo=self.repo, platform=platform, arch=arch))
        return  list(repodata.getList())

