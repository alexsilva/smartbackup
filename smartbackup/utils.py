import socket

__author__ = 'alex'


def server_name_with(conf, key_name):
    """ Add server name to the key name """
    server_name = conf.get("server_name", socket.gethostname())
    return server_name + "_" + key_name
