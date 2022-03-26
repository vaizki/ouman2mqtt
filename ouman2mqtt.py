
from typing import Optional, List, Dict

import asyncio
import hashlib
import logging

from ouman_eh800 import OumanEH800
from asyncio2mqtt import Asyncio2Mqtt, BaseConfig

LOG = logging.getLogger('ouman2mqtt')


class EH800(Asyncio2Mqtt):
    def __init__(self, ouman_url: str, ouman_name: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.ouman_url = ouman_url
        self.ouman_name = ouman_name
        self.ouman = OumanEH800(self.ouman_url)
        self.ha_device_info = {
            'connections': [['url_hash',hashlib.sha1(bytes(self.ouman_url,'ascii')).hexdigest() ],],
            'manufacturer': 'Ouman Oy',
            'model': 'EH-800',
            'name': self.ouman_name,
            'suggested_area': 'Boiler Room',
            'configuration_url': self.ouman_url
            }

    async def poll_data(self) -> Dict:
        return await self.ouman.get_params()

    def get_ha_configuration(self) -> Dict:
        state_topic = f'{self.mqtt_topic}/state'
        conf = {}
        for pn,pc in self.ouman.params.items():
            ha_uid = f'{self.ha_instance}_{pc["key"]}'
            cl = pc.get('class','raw')
            if cl == 'raw':
                ha_t = 'sensor'
                dc = {}
            elif cl == 'temperature':
                ha_t = 'sensor'
                dc = { 'device_class': 'temperature',
                       'unit_of_measurement': '°C',
                       }
            elif cl == 'gauge':
                ha_t = 'sensor'
                dc = { 
                       'unit_of_measurement': pc['unit'],
                       }
            else:
                LOG.error('Unknown type for %s, skipping', pn)
                continue
            hac = {
                'name': self.ouman_name + ' ' + pc['name'],
                'state_topic': state_topic,
                'value_template': '{{value_json.%s}}' % pc['key'],
                'state_class': 'measurement',
                'unique_id': ha_uid,
                'object_id': ha_uid,
                'device': self.ha_device_info,
            }
            hac.update(dc)
            hac.update(pc.get('ha_cfg', {}))
            conf[f'{ha_t}/{ha_uid}'] = hac
        return conf


class EH800Config(BaseConfig):
    def configure(self, p):
        p.add('--ouman-url', required=True,
              help='HTTP URL for Ouman web interface')
        p.add('--ouman-name', default='Ouman EH-800')
        
    def defaults(self):
        return {
            '--mqtt-topic': 'ouman2mqtt/ouman',
            '--ha-instance': 'ouman'
        }

cfg = EH800Config('ouman_')
    
eh = EH800(**cfg.as_dict())
asyncio.run(eh.poll_and_publish())
