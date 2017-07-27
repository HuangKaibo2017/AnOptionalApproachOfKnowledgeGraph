#!/usr/bin/python3.6
# -*- coding: UTF-8 -*-
__author__ = 'Huang Kaibo <kamp_kbh@hotmail.com>'
# The following code, derived from the bulbs project, carries this
# license:
"""
Copyright (c) 2012 James Thornton (http://jamesthornton.com)
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

import sys, re, os
sys.path.insert(0, "..")

from .db_delegate import DbDelegate

def comment_out(text):
    def replacer(match):
        s = match.group(0)
        if s.startswith('/'):
            return r' ' # note: a space and not an empty string
        else:
            return s
    pattern = re.compile(r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"', re.DOTALL | re.MULTILINE)
    return re.sub(pattern, replacer, text)

# TODO: db creation.

class DbBuilder(object):
    def __init__(self, host_name:str, db_user_name:str, db_user_password:str, db_name:str, db_port:int
                 , clear_record:bool=False, drop_schema:bool=True):
        self.host_name = host_name
        self.db_user_name = db_user_name
        self.db_user_password = db_user_password
        self.db_name = db_name
        self.db_port = db_port
        self._cn = DbDelegate(driver_name, host_name, db_user_name, db_user_password, db_name, db_port)
        self.clear_record = clear_record
        self.drop_schema = drop_schema

    def cleanup(self, drop_schema:bool=False, clear_record:bool=True):
        """
        A trick here is the 'abstract'. which projecting it, the abstract class would be in the last position by
        'order by'. So it would be deleted after its dependence. v_region is the sample here.
        :param drop_schema: If False, doesn't delete class of vertex nor edge.
        :param clear_record: delete record or not.
        :return:
        """
        batch_sql = ""
        del_record_sql = "delete EDGE where @class like 'e_%';delete Vertex from (select from V where @class like 'v_%');"
        if clear_record:
            batch_sql += del_record_sql

		if drop_schema:
			rs = self._cn.command(
				"SELECT name,abstract FROM (SELECT expand(classes) FROM metadata:schema) where name like 'v_%' or name like 'e_%' order by abstract")
			if rs is not None:
				for itemOfRs in rs:
					batch_sql += "DROP CLASS {} UNSAFE;".format(itemOfRs.name)
		self._cn.batch(batch_sql)

    def create(self, db_script_file:str, db_name:str):
        if not db_name and self.db_name != db_name and len(db_name) > 0:
            self.db_name = db_name
        batch_sql = ''
        db_file = os.path.join(config.root_path, 'db', db_script_file)
        with open(db_file, 'r', -1,encoding='utf-8-sig') as f:
            batch_sql = comment_out(f.read()).replace('\n','')
            print(batch_sql)
        self._cn.batch(batch_sql)

    def init(self, init_db_script:str, db_name:str):
        init_db_file = os.path.join(config.root_path, 'db', init_db_script)
        with open(init_db_file,'r', -1,encoding='utf-8-sig') as f:
            init_sql = comment_out(f.read()).replace('\n','')
            print(init_sql)
        self._cn.batch(init_sql)

if __name__ == '__main__':
    db_builder = DbBuilder('host_name', 'db_user_name', 'db_user_password', 'db_name', 'db_port', True, True)
    db_builder.cleanup(True, True)
    db_builder.create('orientdb-script.txt', 'db_name')
    db_builder.init('initialization_data.txt', 'db_name')