

import logging

import aiohttp

LOG = logging.getLogger(__name__)


OUMAN_EH800_PARAMS = {
    # S_54_85 L1 min temp 
    # S_55_85 L1 max temp
    'S_59_85': {
        'key': 'L1_control',
        'name': 'L1 control setting',
        'type': 'select',
        'select_map': {
            '0': 'automatic',
            '1': 'forced_small_drop',
            '2': 'forced_large_drop',
            '3': 'forced_normal',
            '5': 'shutdown',
            '6': 'manual'
            },
        'ha_cfg': { 'icon': 'mdi:car-cruise-control' },
    },
    # S_67_85 L1 -20
    # S_69_85 L1 -10
    # S_71_85 L1 0 
    # S_73_85 L1 +10 
    # S_75_85 L1 +20
    # S_89_85 Lammonpudotus setting
    # S_90_85 Suuri lammonpudotus setting
    'S_92_85': {
        'key': 'L1_valve_manual_pct',
        'name': 'L1 manual valve setting',
        'class': 'gauge',
        'unit': '%',
        'ha_cfg': { 'icon': 'mdi:valve' },
    },
    'S_134_85': {
        'key': 'room_finetune_t',
        'name': 'Room temperature fine tuning',
        'class': 'temperature',
    },
    'S_222_85': {
        'key': 'at_home_control',
        'name': 'Home/Away control mode',
        'type': 'select',
        'select_map': {
            '0': 'home',
            '1': 'away',
            '2': 'disabled'
            },
        'ha_cfg': { 'icon': 'mdi:home-switch-outline' },
        },
    'S_227_85': {
        'key': 'outside_t',
        'name': 'Outside temperature',
        'class': 'temperature',
    },
    'S_234_85': {
        'key': 'ambient_t',
        'name': 'ambient temperature',
        'class': 'temperature'
    },
    # S_258_85 Ulkolämpötilan hid. vaik.
    'S_259_85': {
        'key': 'L1_measured_t',
        'name': 'L1 measured temperature',
        'class': 'temperature',
    },
    'S_272_85': {
        'key': 'L1_valve_pct',
        'name': 'L1 valve current position',
        'class': 'gauge',
        'unit': '%',
        'ha_cfg': { 'icon': 'mdi:valve' },
    },
    'S_275_85': {
        'key': 'L1_target_t',
        'name': 'L1 target temperature',
        'class': 'temperature',
        'ha_cfg': { 'icon': 'mdi:gauge' },
    },
    # S_286_85 Hienosäädön vaikutus
    # S_1000_0 = 'L1 Normaalilampo'
}


class OumanEH800():
    def __init__(self, base_url):
        self.url = base_url + ('' if base_url.endswith('/') else '/') + 'request'
        self.params = OUMAN_EH800_PARAMS
        fetch = self.params.keys()
        self.request_url = self.url + '?' + ';'.join(fetch)
        LOG.info('Ouman URL is: %s', self.request_url)
        self.data = None

        
    async def get_params(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.request_url) as resp:
                if resp.status != 200:
                    LOG.warning('Got response code %d, exiting update', resp.status)
                    self.data = None
                    return None
                rtext = (await resp.text(encoding='ascii')).split('?',1)[1]
        self.data = {}
        for param in rtext.split(';'):
            try:
                ok, ov = param.split('=')
            except ValueError:
                continue
            cfg = self.params.get(ok, None)
            if not cfg:
                continue
            ot = cfg.get('type', 'float')
            k = cfg['key']
            if ot == 'float':
                v = float(ov)
            elif ot == 'select':
                v = cfg['select_map'].get(ov,'invalid_%s' % ov)
            self.data[k] = v
        LOG.debug("Ouman data: %s", self.data)        
        return self.data

    
