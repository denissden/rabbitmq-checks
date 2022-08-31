import itertools
import requests
from requests.auth import HTTPBasicAuth
import urllib.parse
import logging

logger = logging.getLogger(__name__)

class Api:
    def __init__(self, address, management_login, management_password) -> None:
        self.address = address
        self.authentication = HTTPBasicAuth(management_login, management_password)

        self.refresh()
    
    def refresh(self):
        logger.info('Refreshing info from management api')
        self.vhosts = self._get('/api/vhosts')
        self.exchanges = self._get('/api/exchanges')
        self.queues = self._get('/api/queues')
        self.bindings = self._get('/api/bindings')
        self.users = self._get('/api/users')
        self.policies = self._get('/api/policies')
    
    def vhost_names(self):
        return self._extract('name', self.vhosts)
    
    def queue_info(self, vhost=None):
        queues_and_vhosts = zip(
                self._extract('name', self.queues),
                self._extract('vhost', self.queues)
            )
        if vhost is None:
            return list(queues_and_vhosts)
        else:
            return list(filter(
                lambda qv: qv[1] == vhost, 
                queues_and_vhosts))

    @staticmethod
    def _extract(prop_name, collection):
        return list(map(lambda i: i[prop_name], collection))

    def _get(self, url: str):
        url = urllib.parse.urljoin(self.address, url)
        res = requests.get(url, auth=self.authentication)
        return res.json()




if __name__ == '__main__':
    from pprint import pprint
    api = Api('http://localhost:15672', 'guest', 'guest')
    pprint(api.__dict__)