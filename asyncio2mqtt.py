
from typing import Union, List, Optional, Any 

import asyncio
import json
import signal
import logging

from asyncio_mqtt import Client, Will, MqttError
import configargparse

LOG = logging.getLogger(__name__)

            
class Asyncio2Mqtt:
    def __init__(self,
                 mqtt_topic: str = 'asyncio2mqtt/test',
                 mqtt_broker: str = '127.0.0.1',
                 mqtt_will: bool = True, 
                 mqtt_publish_state: bool = True,
                 mqtt_publish_values: bool = False,
                 mqtt_publish_interval: int = 15,
                 mqtt_retain_state = False,
                 ha_support: bool = False,
                 ha_instance: str = 'default',
                 ha_expire_after: Union[int,None] = None,
                 ha_assume_online: str = False,
                 ha_status_topic: str = 'homeassistant/status',
                 **kwargs
                 ):
        self.mqtt_client = None
        self.mqtt_topic = mqtt_topic
        self.mqtt_broker = mqtt_broker
        self.mqtt_will = mqtt_will
        self.mqtt_publish_interval = mqtt_publish_interval
        self.mqtt_publish_state = mqtt_publish_state
        self.mqtt_publish_values = mqtt_publish_values
        self.mqtt_retain_state = mqtt_retain_state
        self.ha_support = ha_support
        self.ha_status_topic = ha_status_topic
        self.ha_instance = ha_instance
        if ha_expire_after == -1:
            ha_expire_after = (self.mqtt_publish_interval*3)+1
        self.ha_expire_after = ha_expire_after
        self.ha_assume_online = ha_assume_online
        self._ha_online = ha_assume_online
        self.online = False
        self._broker_ok = None
        
        if mqtt_will:
            self._will = Will(mqtt_topic + '/status', 'offline', qos=1, retain=True)
        else:
            self._will = None

            
    async def connect(self):
        self._broker_ok = asyncio.Event()
        asyncio.create_task(self._reconnect_task())

        
    async def _reconnect_task(self):
        while True:
            LOG.info('Connecting to MQTT broker at %s', self.mqtt_broker) 
            self.mqtt_client = Client(self.mqtt_broker, will=self._will, client_id=self.mqtt_topic)
            try: 
                await self.mqtt_client.connect()
                LOG.info('Connected to MQTT broker')
            except MqttError as exc:
                LOG.error('MQTT broker connect failed: %s', exc)
                await asyncio.sleep(1)
                continue
            # Connect OK
            self._broker_ok.set()
            try:
                if self.ha_support and self.ha_assume_online:
                    await self._configure_ha()
                async with self.mqtt_client.unfiltered_messages() as messages:
                    LOG.debug('MQTT-SUB: $SYS/broker/uptime')
                    await self.mqtt_client.subscribe("$SYS/broker/uptime")
                    if self.ha_support:
                        LOG.debug('MQTT-SUB: %s', self.ha_status_topic)
                        await self.mqtt_client.subscribe(self.ha_status_topic)
                    async for message in messages:
                        msg = message.payload.decode()
                        LOG.debug('MQTT-MSG: [%s]  %s', message.topic, msg)
                        if self.ha_support and message.topic == self.ha_status_topic:
                            if msg == 'online':
                                LOG.info('Home Assistant is online')
                                self._ha_online = True
                                await self._configure_ha()
                                if self.online:
                                    # Send the online message to make sure HA accepts state
                                    await self.go_online(True)
                            if msg == 'offline':
                                LOG.info('Home Assistant is offline')
                                self._ha_online = False
            except MqttError as exc:
                LOG.warning('MQTT broker reconnect loop error: %s', exc)
                await self.disconnect()
                await asyncio.sleep(1)
            
            
    async def disconnect(self):
        if not self._broker_ok.is_set():
            return
        LOG.debug('Disconnecting MQTT broker')
        self._broker_ok.clear()
        try:
            await self.mqtt_client.disconnect()
        except MqttError:
            pass

        
        
    async def publish(self, topic: str, payload: Union[str,dict],
                      full_topic: bool = False, **kwargs):
        if not self.mqtt_client:
            log.error('MQTT-ERR: Publishing to an uninitialized broker connection')
            return
        try:
            if isinstance(payload, dict):
                payload = json.dumps(payload)
        except Exception as exc:
            LOG.exception('Cannot convert payload to string')
            return
        
        if not full_topic:
            topic = f'{self.mqtt_topic}/{topic}'
        LOG.debug('MQTT-PUB: [%s]  %s', topic, payload)
        return await self.mqtt_client.publish(topic, payload, **kwargs) 

    
    async def mlog(self, log_msg, *args):
        if args:
            log_msg = log_msg % args
        self.publish('log', log_msg)

        
    async def go_online(self, force: bool = False):
        if not self.online or force:
            LOG.info('Going online, publishing status to MQTT')
            await self.publish('status', 'online', retain=True)
        self.online = True
        
    async def go_offline(self, force: bool = False):
        if self.online or force:
            LOG.info('Going offline, publishing status to MQTT')
            await self.publish('status', 'offline', retain=True)
        self.online = False
            
        
    async def poll_and_publish(self):
        self._exit_ev = asyncio.Event()

        async def shutdown():
            LOG.info('Signal received - shutdown')
            self._exit_ev.set()

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

        await self.connect()
        try:
            while not self._exit_ev.is_set():
                if not self._broker_ok.is_set():
                    await asyncio.sleep(1)
                    continue
                data = await self.poll_data()
                if not data:
                    LOG.debug('No data from poll(), skipping this round and going offline') 
                    await self.go_offline()
                else:
                    await self.go_online()
                    if (not self.ha_support) or self._ha_online or self.ha_assume_online:
                        if self.mqtt_publish_state:
                            await self.publish('state', json.dumps(data), retain=self.mqtt_retain_state)
                        if self.mqtt_publish_values:
                            for dk,dv in data.items():
                                await self.mqtt_client.publish(f'values/{dk}', dv)
                    else:
                        LOG.debug('HA not online, not publishing')
                try:
                    await asyncio.wait_for(self._exit_ev.wait(), self.mqtt_publish_interval)
                except asyncio.exceptions.TimeoutError:
                    continue
        except Exception as exc:
            LOG.exception('Exception in main loop')
        await self.go_offline()
        await self.disconnect()
        LOG.info('Exiting main()')


    async def _publish_ha_configs(self):
        hac = {}
        hac.update(self.get_ha_configuration())
        if not hac:
            return
        LOG.info('Publishing HA discovery messages for %d entities', len(hac))
        for topic,conf in hac.items():
            conf.update(
                {
                    'availability_topic': f'{self.mqtt_topic}/status',
                    'expire_after': self.ha_expire_after,
                 })
            LOG.debug('Configure: %s = %s', topic, repr(conf))
            await self.publish(f'homeassistant/{topic}/config',
                               json.dumps(conf), full_topic=True)
            
                
    async def _configure_ha(self):
        LOG.info('Configuring Home Assistent support')
        await self._publish_ha_configs()
        
        

class BaseConfig:
    def __init__(self, cfg_prefix):
        self.cfg_prefix = cfg_prefix
        self.cfg = None
        logging.basicConfig(format='%(asctime)s %(levelname)s [%(name)s]: %(message)s'
)
        logging.getLogger().setLevel(logging.DEBUG)
        self._configure()

    def _configure(self):
        p = configargparse.ArgParser(add_env_var_help=True,
                                     auto_env_var_prefix=self.cfg_prefix)
        defs = self.defaults()
        def add(ck, *args, **kwargs):
            if ck:
                cd = defs.get(ck, None)
                if cd:
                    kwargs['default'] = cd
                p.add(ck, *args, **kwargs)
            else:
                p.add(*args, **kwargs)
        
        add(None, '-c', '--config', is_config_file=True,
              help='config file location')
        add(None, '-d', '--debug', action='store_true',
              help='Debug mode / messages')
        add('--mqtt-broker', default='127.0.0.1')
        add('--mqtt-topic', default='asyncio2mqtt/test')
        add('--mqtt-publish-values', action='store_true',
            help='Publish raw values to per-item topics under #/values/+')
        add('--mqtt-publish-interval', '-i', default=15)
        add('--mqtt-retain-state', action='store_true',
            help='Publish #/state objects with retain') 
        add('--ha-support', action='store_true',
            help='Home Assistant MQTT discovery support')
        add('--ha-instance', default='default',
            help='Prefix for HA entity object ids')
        add('--ha-expire-after', type=int, default=-1,
            help='How many seconds without updates before HA considers state unavailable')
        add('--ha-assume-online', action='store_true', default=False,
            help='Publish state/values even if HA state is unknown or offline')
        add('--ha-status-topic', default='homeassistant/status',
            help='Topic where HA publishes offline/online status')
        self.configure(p)
        self.cfg = p.parse_args()
        if self.cfg.debug:
            LOG.debug('CONFIG: %s', self.cfg)
        else:
            logging.getLogger().setLevel(logging.INFO)
    
    def configure(self, p):
        pass

    def defaults(self):
        return {}

    @property
    def config(self):
        return self.cfg
    
    def as_dict(self):
        return vars(self.cfg)
