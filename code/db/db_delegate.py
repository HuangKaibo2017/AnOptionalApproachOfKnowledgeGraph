#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-
__author__ = 'Huang Kaibo <kamp_kbh@hotmail.com>'
# The following code, derived from the bulbs project, carries this
# license:
"""
Copyright (c) 2017 Huang Kaibo (kamp_kbh@hotmail.com)
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
3. The name of the author may not be used to endorse or promote products
   derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from pyorient import OrientDB, DB_TYPE_GRAPH

# from pyorient.exceptions import (PyOrientConnectionException,
#                                  PyOrientCommandException,
#                                  PyOrientSecurityAccessException,
#                                  PyOrientDatabaseException)

# TODO: add helper functions for:
#       1. SQL injection protection: Validate input value is valid or not.
class DbDelegate(object):
    """
    Simple wrapper of pyorient. The caller doesn't need to know it is orientdb client detail.
    """
    def __init__(self, host_name:str, db_user_name:str, db_user_password:str, db_name:str, db_port:int=2424):
        """
        Constructor of DbDelegate.
        :param host_name: the dns name or ip address of the db located. For example, "locahost", "127.0.0.1"
        :param db_user_name: login user name.
        :param db_user_password: login user password.
        :param db_name: database name.
        :param db_port: connection port, by default, it is 2424.
        """
        self.host_name = host_name
        self.db_user_name = db_user_name
        self.db_user_password = db_user_password
        self.db_name = db_name
        self.db_port = db_port
        self._cn:OrientDB = OrientDB(self.host_name, self.db_port)
        self._section_token = None

    def _init_connection(self):
        """
        Make sure connection is on.
        """
        if self._section_token:
            self._cn.set_session_token(self._section_token)
        else:
            self._cn.db_open(self.db_name, self.db_user_name, self.db_user_password, DB_TYPE_GRAPH, '')
            # self._cn.set_session_token(True)
            # self._section_token = self._cn.get_session_token()

    def command(self, *args) -> any:
        self._init_connection()
        return self._cn.command(*args)

    def batch(self, *args) -> any:
        self._init_connection()
        return self._cn.batch(*args)
