"""Utility functions for Schema Sync"""

import re
import os
import datetime
import glob
# import cStringIO
import io
import operator
import json
import requests

# REGEX_NO_TICKS = re.compile('`')
# REGEX_INT_SIZE = re.compile('int\(\d+\)')
REGEX_MULTI_SPACE = re.compile(r'\s\s+')
REGEX_DISTANT_SEMICOLIN = re.compile(r'(\s+;)$')
REGEX_FILE_COUNTER = re.compile(r"_(?P<i>[0-9]+)\.(?:[^.]+)$")
REGEX_TABLE_COMMENT = re.compile(r"COMMENT(?:(?:\s*=\s*)|\s*)'(.*?)'", re.I)
REGEX_TABLE_AUTO_INC = re.compile(r"AUTO_INCREMENT(?:(?:\s*=\s*)|\s*)(\d+)", re.I)
REGEX_SEMICOLON_EXPLODE_TO_NEWLINE = re.compile(r';\s+')


def versioned(filename):
    """Return the versioned name for a file.
       If filename exists, the next available sequence # will be added to it.
       file.txt => file_1.txt => file_2.txt => ...
       If filename does not exist the original filename is returned.

       Args:
            filename: the filename to version (including path to file)

       Returns:
            String, New filename.
    """
    name, ext = os.path.splitext(filename)
    files = glob.glob(name + '*' + ext)
    if not files:
        return filename

    files = map(lambda x: REGEX_FILE_COUNTER.search(x, re.I), files)
    file_counters = [i.group('i') for i in files if i]

    if file_counters:
        i = int(max(file_counters)) + 1
    else:
        i = 1

    return name + ('_%d' % i) + ext


def create_pnames(db, tag=None, date_format="%Y%m%d", no_date=False):
    """Returns a tuple of the filenames to use to create the migration scripts.
       Filename format: <db>[_<tag>].<date=DATE_FORMAT>.(patch|revert).sql

        Args:
            db: string, database name
            tag: string, optional, tag for the filenames
            date_format: string, the current date format
                         Default Format: 21092009
            no_date: bool

        Returns:
            tuple of strings (patch_filename, revert_filename)
    """
    d = datetime.datetime.now().strftime(date_format)
    if tag:
        tag = re.sub('[^A-Za-z0-9_-]', '_', tag)
        if '__' in tag:
            tag = re.sub('_+', '_', tag)
        basename = "%s_%s.%s" % (db, tag, d)
    elif no_date:
        basename = "%s" % (db)
    else:
        basename = "%s.%s" % (db, d)

    return ("%s.%s" % (basename, "patch.sql"),
            "%s.%s" % (basename, "revert.sql"))


def send_msg_2_fei_shu(url, msg, title='', at_user_id_list: list = None, is_at_all: bool = False):
    """
    发送消息到飞书群
    :param url: 飞书 Web Hook 地址
    :param msg: 信息内容
    :param title: 消息标题（可选）
    :param at_user_id_list: At某些人（需要提供对应人员的user_id）
    :param is_at_all: 是否 At 所有人（可选，若为True，则不再单独At某些人）
    :return:
    """

    def send_msg(msg_content, msg_title):
        content[0]['text'] = msg_content
        msg_json['content']['post']['zh_cn']['title'] = msg_title
        msg_json['content']['post']['zh_cn']['content'] = [content]
        value = json.dumps(msg_json)
        resp = requests.post(url, data=value, headers=headers)
        print('[SEND_MSG_TO_FEI_SHU_RESULT] ' + resp.text)

    if is_at_all:
        content = [{
            "tag": "text",
            "text": ''
        },
            {
                "tag": "at",
                "user_id": "all"
            }]
    elif at_user_id_list:
        content = [{
            "tag": "text",
            "text": ''
        }]
        for user_id in at_user_id_list:
            at_tag = {
                "tag": "at",
                "user_id": user_id
            }
            content.append(at_tag)
    else:
        content = [{
            "tag": "text",
            "text": ''
        }]

    msg_json = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": '',
                    "content": []
                }
            }
        }
    }
    headers = {"Content-Type": "application/json;charset=utf-8"}

    msg = str(msg)
    title = str(title)
    msg_len = len(msg)
    sep = 20000
    if msg_len > sep:
        i = 1
        begin = 0
        end = begin + sep
        last_new_line_idx = 0

        while end < msg_len:
            if i != 1:
                end = begin + sep
            new_line_idx = msg[:end].rfind('\n')
            end = new_line_idx + 1 if new_line_idx != -1 and new_line_idx != last_new_line_idx else end + 1
            send_msg(msg[begin:end], title + '【数据长度超出限制，分批次发送】【第 %s 批】' % i)
            begin = end
            last_new_line_idx = new_line_idx
            i += 1
    else:
        send_msg(msg, title)


def compare_version(x, y, separator=r'[.-]'):
    """Return negative if version x<y, zero if x==y, positive if x>y.

        Args:
            x: string, version x to compare
            y: string, version y to compare
            separator: regex

        Returns:
            integer representing the compare result of version x and y.
    """
    x_array = re.split(separator, x)
    y_array = re.split(separator, y)
    for index in range(min(len(x_array), len(y_array))):
        if x_array[index] != y_array[index]:
            try:
                # return cmp(int(x_array[index]), int(y_array[index]))
                return operator.eq(int(x_array[index]), int(y_array[index]))
            except ValueError:
                return 0
    return 0


class PatchBuffer(object):
    """Class for creating patch files

        Attributes:
            name: String, filename to use when saving the patch
            filters: List of functions to map to the patch data
            tpl: The patch template where the data will be written
                 All data written to the PatchBuffer is placed in the
                template variable %(data)s.
            ctx: Dictionary of values to be put replaced in the template.
            version_filename: Bool, version the filename if it already exists?
            modified: Bool (default=False), flag to check if the
                      PatchBuffer has been written to.
    """

    def __init__(self, name, filters, tpl, ctx, version_filename=False):
        """Inits the PatchBuffer class"""
        # self._buffer = cStringIO.StringIO()
        self._buffer = io.StringIO()
        self.name = name
        self.filters = filters
        self.tpl = tpl
        self.ctx = ctx
        self.version_filename = version_filename
        self.modified = False

    def write(self, data):
        """Write data to the buffer."""
        self.modified = True
        self._buffer.write(data)

    def save(self):
        """Apply filters, template transformations and write buffer to disk"""
        data = self._buffer.getvalue()
        if not data:
            return False

        if self.version_filename:
            self.name = versioned(self.name)
        fh = open(self.name, 'w')

        for f in self.filters:
            data = f(data)

        self.ctx['data'] = data

        fh.write(self.tpl % self.ctx)
        fh.close()

        return True

    def delete(self):
        """Delete the patch once it has been writen to disk"""
        if os.path.isfile(self.name):
            os.unlink(self.name)

    def __del__(self):
        self._buffer.close()
