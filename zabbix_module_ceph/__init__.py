from __future__ import absolute_import

import json
import logging
import pprint
import threading
import time

import six

import rados

import zabbix_module.simple as simple
import zabbix_module.types as types


_CEPH_CONNECT_TIMEOUT = 10  # seconds
_CEPH_COMMAND_TIMEOUT = 10  # seconds

_DATA_FETCH_INTERVAL = 10.0  # seconds
_MAX_TIME_DIFF = 60.0  # seconds

_DEFAULT_CONF = {
    'ceph_conf_files': {
        'main': '/etc/ceph/ceph.conf',
    },
}

_MAP_STATUS = {
    'fsid': {
        'path': ('fsid', ),
    },
    'health.overall': {
        'path': ('health', 'overall_status'),
    },
    'mds.in': {
        'path': ('mdsmap', 'in'),
    },
    'mds.max': {
        'path': ('mdsmap', 'max'),
    },
    'mds.up': {
        'path': ('mdsmap', 'up'),
    },
    'mds.up_standby': {
        'path': ('mdsmap', 'up:standby'),
    },
    'mon.num': {
        'path': ('monmap', 'mons'),
        'conv': len,
    },
    'mon.quorum': {
        'path': ('quorum', ),
        'conv': len,
    },
    'osd.num': {
        'path': ('osdmap', 'osdmap', 'num_osds'),
    },
    'osd.in': {
        'path': ('osdmap', 'osdmap', 'num_in_osds'),
    },
    'osd.up': {
        'path': ('osdmap', 'osdmap', 'num_up_osds'),
    },
    'pg.bytes_avail': {
        'path': ('pgmap', 'bytes_avail'),
    },
    'pg.bytes_total': {
        'path': ('pgmap', 'bytes_total'),
    },
    'pg.bytes_used': {
        'path': ('pgmap', 'bytes_used'),
    },
    'pg.data_bytes': {
        'path': ('pgmap', 'data_bytes'),
    },
    'pg.num_pgs': {
        'path': ('pgmap', 'num_pgs'),
    },
}


_log = logging.getLogger(__name__)


def _get_value_from_tree(tree_dict, path):
    if path:
        return _get_value_from_tree(tree_dict[path[0]], path[1:])
    else:
        return tree_dict


def _conv_dict(from_dict, dict_map, args):
    to_dict = {}

    for map_key, map_val in six.iteritems(dict_map):
        value = _get_value_from_tree(from_dict, map_val['path'])

        conv = map_val.get('conv')
        if conv is not None:
            value = conv(value)

        to_dict[map_key] = {
            args: value,
        }

    return to_dict


class _CephConnection(threading.Thread):
    def __init__(self, name, conf_file, commands):
        super(_CephConnection, self).__init__()

        self._running = True

        self._name = name
        self._conf_file = conf_file
        self._commands = commands

        self._cluster = None

    def _touch_connection(self):
        if self._cluster is not None:
            return

        try:
            self._cluster = rados.Rados(conffile=self._conf_file)
            self._cluster.connect(_CEPH_CONNECT_TIMEOUT)
        except:
            self._cluster = None
            raise

    def _disconnect(self):
        if self._cluster is None:
            return

        try:
            self._cluster.shutdown()
        finally:
            self._cluster = None

    def _ceph_mon_command(self, command):
        _log.debug('Executing ceph mon command "%s" on cluster "%s"...',
                   command, self._name)

        command_json = json.dumps(dict(
            prefix=command,
            format='json'))
        ret_code, ret_buf, ret_msg = self._cluster.mon_command(
                command_json, '', timeout=_CEPH_COMMAND_TIMEOUT)

        if ret_code != 0:
            raise RuntimeError('Ceph command "%s" failed: %r, %r, %r' % (
                command, ret_code, ret_buf, ret_msg))

        result = json.loads(ret_buf)
        _log.debug('Result of ceph mon command "%s" on cluster "%s":\n%s',
                   command, self._name, pprint.pformat(result))
        return result

    def _update(self):
        for command in self._commands:
            mon_data = self._ceph_mon_command(command['mon_command'])
            command['submodule'].update(mon_data, self._name)

    def run(self):
        _log.info('Started thread for cluster "%s"', self._name)

        while self._running:
            try:
                self._touch_connection()
                self._update()
            except:
                self._disconnect()
                _log.exception(
                        'Unable to fetch data for cluster "%s", will try '
                        'again after sleep',
                        self._name)

            time.sleep(_DATA_FETCH_INTERVAL)

        _log.info('Stopped thread for cluster "%s"', self._name)

    def stop(self):
        self._running = False


class _Status(simple.Simple):
    items_prefix = 'status.'

    def __init__(self, *args, **kwargs):
        super(_Status, self).__init__(*args, **kwargs)

        self.add_asynchronous_items(dict(
                (item_name, {
                    'max_time_diff': _MAX_TIME_DIFF,
                    'have_params': True,
                }) for item_name in six.iterkeys(_MAP_STATUS)))
        self.add_asynchronous_items({
            'pg.pfree': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
            'pg.pused': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
        })

    def update(self, data, cluster_name):
        self.update_asynchronous_items(
                _conv_dict(data, _MAP_STATUS, (cluster_name, )))

        bytes_avail = _get_value_from_tree(data, ('pgmap', 'bytes_avail'))
        bytes_used = _get_value_from_tree(data, ('pgmap', 'bytes_used'))
        bytes_total = _get_value_from_tree(data, ('pgmap', 'bytes_total'))
        self.update_asynchronous_items({
            'pg.pfree': {
                (cluster_name, ): 100.0 * bytes_avail / bytes_total,
            },
            'pg.pused': {
                (cluster_name, ): 100.0 * bytes_used / bytes_total,
            },
        })


class _PG(simple.Simple):
    items_prefix = 'pg.'

    def __init__(self, *args, **kwargs):
        super(_PG, self).__init__(*args, **kwargs)

        self.add_asynchronous_items({
            'pool.read': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
            'pool.read_kb': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
            'pool.write': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
            'pool.write_kb': {
                'max_time_diff': _MAX_TIME_DIFF,
                'have_params': True,
            },
        })

    def update(self, data, cluster_name):
        read = 0
        read_kb = 0
        write = 0
        write_kb = 0

        for pool in data:
            read += pool['stat_sum']['num_read']
            read_kb += pool['stat_sum']['num_read_kb']
            write += pool['stat_sum']['num_write']
            write_kb += pool['stat_sum']['num_write_kb']

        self.update_asynchronous_items({
            'pool.read': {
                (cluster_name, ): read,
            },
            'pool.read_kb': {
                (cluster_name, ): read_kb,
            },
            'pool.write': {
                (cluster_name, ): write,
            },
            'pool.write_kb': {
                (cluster_name, ): write_kb,
            },
        })


class Main(simple.Simple):
    items_prefix = 'zpm.ceph.'

    def __init__(self, module_type, module_name, module_conf):
        super(Main, self).__init__(module_type, module_name, module_conf)

        self._conf = dict(_DEFAULT_CONF)
        self._conf.update(module_conf)

        commands = (
            {
                'mon_command': 'status',
                'submodule': _Status(module_type, module_name, module_conf),
            },
            {
                'mon_command': 'pg dump_pools_json',
                'submodule': _PG(module_type, module_name, module_conf),
            },
        )
        for submodule in map(lambda cmd: cmd['submodule'], commands):
            self.add_submodule(submodule)

        self._threads = []
        for cluster_name, conf_file in six.iteritems(
                self._conf.get('ceph_conf_files', {})):
            connection_thread = _CephConnection(
                    cluster_name, conf_file, commands)
            connection_thread.start()
            self._threads.append(connection_thread)

    @simple.item()
    def get_discovery(self):
        return types.Discovery({
            'ZPM_CEPH_CLUSTER': cluster_name,
        } for cluster_name in six.iterkeys(
                self._conf.get('ceph_conf_files', {})))

    def on_module_terminate(self):
        for thread in self._threads:
            thread.stop()
        for thread in self._threads:
            thread.join()
