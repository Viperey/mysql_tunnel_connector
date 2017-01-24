import logging
from configparser import ConfigParser

import sshtunnel
import MySQLdb


class MysqlTunnelConnector:
    host = None
    port = None
    user = None
    password = None
    database = None

    tunnel_enabled = False
    ssh_forwarder_host = None
    ssh_forwarder_port = None
    ssh_forwarder_user = None
    private_key_path = None
    private_key_password = None

    __settings_path__ = 'settings/properties.ini'
    __settings_section__ = 'mysql'
    __ssh_tunnel_connection__ = None
    __mysql_connection__ = None

    logger = logging.getLogger(__name__)

    def __init__(self):
        self.__load_mysql_credentials__()

    def __load_mysql_credentials__(self):
        def __get_value_from_config(config_in, key):
            return config_in.get(self.__settings_section__, key)

        config = ConfigParser()
        config.read(self.__settings_path__)

        self.host = __get_value_from_config(config_in=config, key='host')
        self.port = int(__get_value_from_config(config_in=config, key='port'))
        self.user = __get_value_from_config(config_in=config, key='user')
        self.password = __get_value_from_config(config_in=config, key='password')
        self.database = __get_value_from_config(config_in=config, key='database')

        self.ssh_forwarder_host = __get_value_from_config(config_in=config, key='ssh_forwarder_host')
        self.ssh_forwarder_port = int(__get_value_from_config(config_in=config, key='ssh_forwarder_port'))
        self.ssh_forwarder_user = __get_value_from_config(config_in=config, key='ssh_forwarder_user')
        self.private_key_path = __get_value_from_config(config_in=config, key='private_key_path')
        self.private_key_password = __get_value_from_config(config_in=config, key='private_key_password')
        self.tunnel_enabled = __get_value_from_config(config_in=config, key='tunnel_enabled')

    def connect(self):
        return self.__connectWithTunnel__() if self.tunnel_enabled else self.__connectWithoutTunnel__()

    def __connectWithTunnel__(self):
        try:
            self.__ssh_tunnel_connection__ = sshtunnel.SSHTunnelForwarder(
                (self.ssh_forwarder_host, self.ssh_forwarder_port), ssh_username=self.ssh_forwarder_user,
                ssh_pkey=self.private_key_path, ssh_private_key_password=self.private_key_password,
                remote_bind_address=(self.host, self.port), )
            self.__ssh_tunnel_connection__.start()

            self.__mysql_connection__ = MySQLdb.connect(host='127.0.0.1', user=self.user, passwd=self.password,
                                                        db=self.database,
                                                        port=self.__ssh_tunnel_connection__.local_bind_port)
        except Exception as e:
            self.close()

    def __connectWithoutTunnel__(self):
        try:
            self.__mysql_connection__ = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password,
                                                        db=self.database, port=self.port)
        except Exception as e:
            self.close()

    def close(self):
        if self.tunnel_enabled and self.__ssh_tunnel_connection__ is not None:
            self.__ssh_tunnel_connection__.close()

        if self.__mysql_connection__ is not None:
            self.__mysql_connection__.close()

    def get_connection(self):
        return self.__mysql_connection__

    def execute(self, query):

        def is_select(query_in):
            return query_in.upper().startswith('SELECT ')

        try:
            self.connect()
            with self.__mysql_connection__:
                cursor = self.__mysql_connection__.cursor()
                if is_select(query):
                    cursor.execute(query)
                    fields = map(lambda x: x[0], cursor.description)
                    return [dict(zip(fields, row)) for row in cursor.fetchall()]
                else:
                    return cursor.execute(query)
        finally:
            self.close()
