#!/usr/bin/env python
#
# Got Your Back
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""\n%s\n\nGot Your Back (GYB) is a command line tool which allows users to backup and restore their Gmail.

For more information, see http://code.google.com/p/got-your-back/
"""

global __name__, __author__, __email__, __version__, __license__
__program_name__ = 'Got Your Back: Gmail Backup'
__author__ = 'Jay Lee'
__email__ = 'jay0lee@gmail.com'
__version__ = '0.20 Alpha'
__license__ = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)'
__db_schema_version__ = '6'
__db_schema_min_version__ = '2'        #Minimum for restore

import imaplib
from optparse import OptionParser, SUPPRESS_HELP
import sys
import os
import os.path
import time
import random
import struct
import platform
import StringIO
import socket
import datetime
import sqlite3
import email
import email.parser
import mimetypes
import re
import shlex
from itertools import islice, chain
import math
import urllib
import urlparse
import shutil
import zipfile

import pywintypes
import win32file
import win32con

try:
    import json as simplejson
except ImportError:
    import simplejson

import httplib2
import oauth2client.client
import oauth2client.file
import oauth2client.tools
import gflags
import apiclient
import apiclient.discovery
import apiclient.errors
import gimaplib

def SetupOptionParser():
    # Usage message is the module's docstring.
    parser = OptionParser(usage=__doc__ % getGYBVersion(), add_help_option=False)
    parser.add_option('--email',
        dest='email',
        help='Full email address of user or group to act against')
    action_choices = ['backup','restore', 'attach2drive', 'restore-group', 'count', 'purge', 'estimate', 'reindex', 'refresh-time']
    parser.add_option('--action',
        type='choice',
        choices=action_choices,
        dest='action',
        default='backup',
        help='Action to perform - %s. Default is backup.' % ', '.join(action_choices))
    parser.add_option('--search',
        dest='gmail_search',
        default='in:anywhere',
        help='Optional: On backup, estimate, count and purge, Gmail search to scope operation against')
    parser.add_option('--local-folder',
        dest='local_folder',
        help='Optional: On backup, restore, estimate, local folder to use. Default is GYB-GMail-Backup-<email>',
        default='XXXuse-email-addressXXX')
    parser.add_option('--use-imap-folder',
        dest='use_folder',
        help='Optional: IMAP folder to act against. Default is "All Mail" label. You can run "--use_folder [Gmail]/Chats" to backup chat.')
    parser.add_option('--label-restored',
        dest='label_restored',
        help='Optional: On restore, all messages will additionally receive this label. For example, "--label_restored gyb-restored" will label all uploaded messages with a gyb-restored label.')
    parser.add_option('--service-account',
        dest='service_account',
        help='Google Apps Business and Education only. Use OAuth 2.0 Service Account to authenticate.')
    parser.add_option('--use-admin',
        dest='use_admin',
        help='Optional. On restore-group, authenticate as this admin user.')
    parser.add_option('--batch-size',
        dest='batch_size',
        type='int',
        default=100,
        help='Optional: On backup, sets the number of messages to batch download.')
    parser.add_option('--noresume', 
        action='store_true', 
        default=False,
        help='Optional: On restores, start from beginning. Default is to resume where last restore left off.')
    parser.add_option('--fast-incremental',
        dest='refresh',
        action='store_false',
        default=True,
        help='Optional: On backup, skips refreshing labels for existing message and marking deleted messages')
    parser.add_option('--compress',
        dest='compress',
        type='int',
        default=0,
        help='Optional: network compression amount [0-9], default is %default (off).')
    parser.add_option('--zip',
        dest='zip',
        action='store_true',
        default=False,
        help='Optional: On backup, create zip archives of each year and month group of emails (changed months only)')
    parser.add_option('--debug',
        action='store_true',
        dest='debug',
        help='Turn on verbose debugging and connection information (troubleshooting)')
    parser.add_option('-v', '--version',
        action='store_true',
        dest='version',
        help='print GYB version and quit')
    parser.add_option('--help',
        action='help',
        help='Display this message.')
    return parser

def getProgPath():
        return os.path.dirname(os.path.realpath(sys.argv[0]))

def batch(iterable, size):
        sourceiter = iter(iterable)
        while True:
                batchiter = islice(sourceiter, size)
                yield chain([batchiter.next()], batchiter)

def requestOAuthAccess(email, options):
    scopes = ['https://mail.google.com/',                        # IMAP/SMTP client access
                        'https://www.googleapis.com/auth/userinfo#email',
                        'https://www.googleapis.com/auth/apps.groups.migration']
    CLIENT_SECRETS = os.path.join(getProgPath(), 'client_secrets.json')
    MISSING_CLIENT_SECRETS_MESSAGE = """
WARNING: Please configure OAuth 2.0

To make GYB run you will need to populate the client_secrets.json file
found at:

     %s

with information from the APIs Console <https://code.google.com/apis/console>.

""" % (CLIENT_SECRETS)
    FLOW = oauth2client.client.flow_from_clientsecrets(CLIENT_SECRETS, scope=scopes, message=MISSING_CLIENT_SECRETS_MESSAGE)
    cfgFile = os.path.join(options.local_folder, email + '.cfg')
    storage = oauth2client.file.Storage(cfgFile)
    credentials = storage.get()
    if os.path.isfile(os.path.join(getProgPath(), 'nobrowser.txt')):
        gflags.FLAGS.auth_local_webserver = False
    if credentials is None or credentials.invalid:
        certFile = os.path.join(getProgPath(), 'cacert.pem')
        disable_ssl_certificate_validation = False
        if os.path.isfile(os.path.join(getProgPath(), 'noverifyssl.txt')):
            disable_ssl_certificate_validation = True
        http = httplib2.Http(ca_certs=certFile, disable_ssl_certificate_validation=disable_ssl_certificate_validation)
        credentials = oauth2client.tools.run(FLOW, storage, short_url=True, http=http)

def generateXOAuthString(options):
    email = options.email
    service_account = options.service_account
    debug = options.debug
    if debug:
        httplib2.debuglevel = 4
    if service_account:
        f = file(os.path.join(getProgPath(), 'privatekey.p12'), 'rb')
        key = f.read()
        f.close()
        scope = 'https://mail.google.com/'
        credentials = oauth2client.client.SignedJwtAssertionCredentials(service_account_name=service_account, private_key=key, scope=scope, user_agent=getGYBVersion(' / '), prn=email)
        disable_ssl_certificate_validation = False
        if os.path.isfile(os.path.join(getProgPath(), 'noverifyssl.txt')):
            disable_ssl_certificate_validation = True
        http = httplib2.Http(ca_certs=os.path.join(getProgPath(), 'cacert.pem'), disable_ssl_certificate_validation=disable_ssl_certificate_validation)
        if debug:
            httplib2.debuglevel = 4
        http = credentials.authorize(http)
        service = apiclient.discovery.build('oauth2', 'v2', http=http)
    else:
        cfgFile = os.path.join(options.local_folder, email + '.cfg')
        storage = oauth2client.file.Storage(cfgFile)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            requestOAuthAccess(email, options)
            credentials = storage.get()
    if credentials.access_token_expired:
        disable_ssl_certificate_validation = False
        if os.path.isfile(os.path.join(getProgPath(), 'noverifyssl.txt')):
            disable_ssl_certificate_validation = True
        credentials.refresh(httplib2.Http(ca_certs=os.path.join(getProgPath(), 'cacert.pem'), disable_ssl_certificate_validation=disable_ssl_certificate_validation))
    return "user=%s\001auth=OAuth %s\001\001" % (email, credentials.access_token)

def callGAPI(service, function, soft_errors=False, throw_reasons=[], **kwargs):
    method = getattr(service, function)
    retries = 3
    for n in range(1, retries + 1):
        try:
            return method(**kwargs).execute()
        except apiclient.errors.HttpError, e:
            error = simplejson.loads(e.content)
            try:
                reason = error['error']['errors'][0]['reason']
                http_status = error['error']['code']
                message = error['error']['errors'][0]['message']
                if reason in throw_reasons:
                    raise
                if n != retries and reason in ['rateLimitExceeded', 'userRateLimitExceeded', 'backendError']:
                    wait_on_fail = (2 ** n) if (2 ** n) < 60 else 60
                    randomness = float(random.randint(1,1000)) / 1000
                    wait_on_fail = wait_on_fail + randomness
                    if n > 3: sys.stderr.write('\nTemp error %s. Backing off %s seconds...' % (reason, int(wait_on_fail)))
                    time.sleep(wait_on_fail)
                    if n > 3: sys.stderr.write('attempt %s/%s\n' % (n + 1, retries))
                    continue
                sys.stderr.write('\n%s: %s - %s\n' % (http_status, message, reason))
                if soft_errors:
                    sys.stderr.write(' - Giving up.\n')
                    return
                else:
                    sys.exit(int(http_status))
            except KeyError:
                sys.stderr.write('Unknown Error: %s' % e)
                sys.exit(1)
        except oauth2client.client.AccessTokenRefreshError, e:
            sys.stderr.write('Error: Authentication Token Error - %s' % e)
            sys.exit(403)

def message_is_backed_up(message_uid, sqlcur, sqlconn, backup_folder):
    try:
        sqlcur.execute('''
                SELECT message_filename FROM uids NATURAL JOIN messages
                            where uid = ?''', (message_uid,))
    except sqlite3.OperationalError, e:
        if e.message == 'no such table: messages':
            print "\n\nError: your backup database file appears to be corrupted."
        else:
            print "SQL error:%s" % e
        sys.exit(8)
    sqlresults = sqlcur.fetchall()
    for x in sqlresults:
        filename = x[0]
        if os.path.isfile(os.path.join(backup_folder, filename)):
            return True
    return False

def mark_removed_messages_deleted(sqlcur, sqlconn):
    """
    Soft delete messages that are referenced in the messages table but no longer 
    have a uid, probably because they have been deleted (or moved to trash/spam 
    folders).
    """
    try:
        # messages deleted from GMail no longer appear in the uids table if the
        # db is reindexed
        sqlcur.execute('''
                SELECT m.message_num
                FROM messages m LEFT NATURAL JOIN uids u
                WHERE (is_deleted = 0 OR is_deleted IS NULL) AND u.uid IS NULL''')
    except sqlite3.OperationalError, e:
        if e.message == 'no such table: messages':
            print "\n\nError: your backup database file appears to be corrupted."
        else:
            print "SQL error:%s" % e
        sys.exit(8)
    sqlresults = sqlcur.fetchall()
    for x in sqlresults:
        soft_delete_message(x[0], sqlcur, sqlconn)

def archive_deleted_messages(sqlcur, sqlconn, backup_folder):
    deleted_folder = os.path.join(backup_folder, "deleted")
    if not os.path.isdir(deleted_folder):
            os.mkdir(deleted_folder)

    try:
        sqlcur.execute("""
            SELECT message_num, message_filename
            FROM messages
            WHERE is_deleted = 1 AND message_filename NOT LIKE ?""", ("deleted%",))
    except sqlite3.OperationalError, e:
        if e.message == 'no such table: messages':
            print "\n\nError: your backup database file appears to be corrupted."
        else:
            print "SQL error:%s" % e
        sys.exit(8)
    sqlresults = sqlcur.fetchall()
    deleted_count = len(sqlresults)

    if deleted_count > 0:
        print "GYB needs to move %s messages to deleted" % deleted_count
        print ""

        for x in sqlresults:
            message_num = x[0]
            filename = x[1]
            full_filename = os.path.join(backup_folder, filename)
            deleted_filename = os.path.join(deleted_folder, filename)
            if os.path.isfile(full_filename):
                deleted_path = os.path.dirname(deleted_filename)
                if not os.path.isdir(deleted_path):
                    os.makedirs(deleted_path)
                shutil.move(full_filename, deleted_filename)
            else:
                print 'WARNING! file %s does not exist for message %s' % (full_filename, message_num)

            if os.path.isfile(deleted_filename) or not os.path.isfile(full_filename):
                sqlcur.execute("""
                    UPDATE messages
                    SET message_filename = ?
                    WHERE message_num = ?""", (os.path.join("deleted", filename), message_num))
                sqlconn.commit()

def get_backed_up_message_ids(sqlcur, sqlconn, backup_folder):
    try:
        sqlcur.execute("""
            SELECT u.uid, m.message_filename, m.message_num
            FROM messages m NATURAL JOIN uids u
            WHERE (is_deleted = 0 OR is_deleted IS NULL)""")
    except sqlite3.OperationalError, e:
        if e.message == 'no such table: messages':
            print "\n\nError: your backup database file appears to be corrupted."
        else:
            print "SQL error:%s" % e
        sys.exit(8)
    uids = {}
    sqlresults = sqlcur.fetchall()
    for x in sqlresults:
        uid = str(x[0])
        filename = x[1]
        message_num = x[2]
        full_filename = os.path.join(backup_folder, filename)
        if not os.path.isfile(full_filename):
            print 'WARNING! file %s does not exist for message %s' % (full_filename, uid)
        uids[uid] = message_num
    return uids

def mark_messages_deleted(deleted_uids, backed_up_message_ids, sqlcur, sqlconn, backup_folder):
    deleted_count = len(deleted_uids)
    if deleted_count > 0:
        print "GYB needs to mark %s messages as deleted" % deleted_count

        for uid in deleted_uids:
            soft_delete_message(backed_up_message_ids[uid], sqlcur, sqlconn)

        print "marked %s messages as deleted" % (len(deleted_uids))

def soft_delete_message(message_num, sqlcur, sqlconn):
    sqlcur.execute("UPDATE messages SET is_deleted = 1 WHERE message_num = ?", (message_num,))
    #sqlcur.execute("DELETE FROM uids where message_num = ?", (message_num,))
    sqlcur.execute("DELETE FROM labels where message_num = ?", (message_num,))
    sqlcur.execute("DELETE FROM flags where message_num = ?", (message_num,))
    sqlconn.commit()

def change_file_times(fname, mtime=None, atime=None, ctime=None):
    winmtime = None if mtime == None else pywintypes.Time(mtime)
    winatime = None if atime == None else pywintypes.Time(atime)
    winctime = None if ctime == None else pywintypes.Time(ctime)

    winfile = win32file.CreateFile(fname, win32con.GENERIC_WRITE,
        win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
        None, win32con.OPEN_EXISTING,
        win32con.FILE_ATTRIBUTE_NORMAL, None)
    win32file.SetFileTime(winfile, winctime, winatime, winmtime, True)
    winfile.close()

def set_message_file_date(message_full_filename, message_time):
    mtime = os.path.getmtime(message_full_filename)
    #print message_full_filename, " last modified: %s" % time.ctime(mtime)
    if mtime != message_time:
        #os.utime(message_full_filename, (time.time(), message_time))
        change_file_times(message_full_filename, message_time, time.time(), message_time)
        #print " updated to: %s" %
        #time.ctime(os.path.getmtime(message_full_filename))

def set_message_file_dates(backup_folder, message_details):
    message_count = len(message_details)
    if message_count > 0:
        print "Refreshing modified timestamp on %s messages" % message_count
        message_position = 0
        for message in message_details:
            full_filename = os.path.join(backup_folder, message[0])
            if os.path.isfile(full_filename):
                set_message_file_date(full_filename, time.mktime(message[1].timetuple()))
                message_position += 1
            else:
                print '\nWARNING! message file %s does not exist' % (full_filename,)

            # if message_position % 100:
                # restart_line()
                # sys.stdout.write("refreshed modified timestamp on %s of %s messages" % (message_position, message_count))
                # sys.stdout.flush()
        restart_line()
        sys.stdout.write("refreshed modified timestamp on %s of %s messages" % (message_position, message_count))
        sys.stdout.flush()
        print "\n"

def create_compressed_archives(backup_folder):
    if not os.path.isabs(backup_folder):
        backup_folder = os.path.abspath(backup_folder)
    (root_path, backup_folder_name) = os.path.split(backup_folder)
    archive_folder = os.path.join(root_path, "Archives")

    print "Compressing monthly archives for: ", backup_folder
    print "Archive_folder: ", archive_folder

    if not os.path.isdir(archive_folder):
        os.mkdir(archive_folder)

    for year in os.listdir(backup_folder):
        if year.isdigit():
            for month in os.listdir(os.path.join(backup_folder, year)):
                if month.isdigit():
                    eml_files = set()
                    path_to_archive = os.path.join(backup_folder, year, month)
                    for walk_root, dirs, files in os.walk(path_to_archive):
                        if len(files) > 0:
                            relpath = os.path.relpath(walk_root, root_path)
                            for file_name in files:
                                (name, ext) = os.path.splitext(file_name)
                                if ext == ".eml":
                                    full_file_path = os.path.join(walk_root, file_name)
                                    modified_datetime = datetime.datetime.fromtimestamp(os.path.getmtime(full_file_path))
                                    # remove seconds and milliseconds as zip format does not
                                    # store enough time resolution for comparison
                                    modified_datetime = modified_datetime.replace(second=0, microsecond=0)
                                    eml_files.add((os.path.normcase(os.path.join(relpath, file_name)), modified_datetime, os.path.getsize(full_file_path)))

                    zip_name = "%s_%04d_%02d" % (backup_folder_name, int(year), int(month))
                    zip_path = os.path.join(archive_folder, zip_name)

                    zip_files = set()
                    full_zip_name = zip_path + '.zip'
                    if os.path.isfile(full_zip_name):
                        with zipfile.ZipFile(full_zip_name, 'r') as zip:
                            for zip_file in zip.infolist():
                                modified_datetime = datetime.datetime(*zip_file.date_time)
                                # remove seconds and milliseconds as zip format does not store
                                # enough time resolution for comparison
                                modified_datetime = modified_datetime.replace(second=0, microsecond=0)
                                zip_files.add((os.path.normcase(zip_file.filename), modified_datetime, long(zip_file.file_size)))

                    has_differences = len(zip_files) == 0 or len(eml_files ^ zip_files) > 0

                    if has_differences:
                        base_path = os.path.join(backup_folder_name, year, month)
                        print "Creating archive: %s.zip" % (zip_path,)
                        shutil.make_archive(zip_path, "zip", root_path, base_path)
    print ""

def get_db_settings(sqlcur):
    try:
        sqlcur.execute('SELECT name, value FROM settings')
        db_settings = dict(sqlcur)
        return db_settings
    except sqlite3.OperationalError, e:
        if e.message == 'no such table: settings':
            print "\n\nSorry, this version of GYB requires version %s of the database schema. Your backup folder database does not have a version." % (__db_schema_version__)
            sys.exit(6)
        else:
            print "%s" % e

def check_db_settings(db_settings, action, user_email_address):
    if (db_settings['db_version'] < __db_schema_min_version__ or db_settings['db_version'] > __db_schema_version__):
        print "\n\nSorry, this backup folder was created with version %s of the database schema while GYB %s requires version %s - %s for restores" % (db_settings['db_version'], __version__, __db_schema_min_version__, __db_schema_version__)
        sys.exit(4)

    # Only restores are allowed to use a backup folder started with another
    # account (can't allow 2 Google Accounts to backup/estimate from same folder)
    if action not in ['restore', 'restore-group']:
        if user_email_address.lower() != db_settings['email_address'].lower():
            print "\n\nSorry, this backup folder should only be used with the %s account that it was created with for incremental backups. You specified the %s account" % (db_settings['email_address'], user_email_address)
            sys.exit(5)

def convertDB(sqlconn, uidvalidity, oldversion):
    print "Converting database"
    try:
        with sqlconn:
            if oldversion < '3':
                # Convert to schema 3
                sqlconn.executescript('''
                    BEGIN;
                    CREATE TABLE uids
                            (message_num INTEGER, uid INTEGER PRIMARY KEY);
                    INSERT INTO uids (uid, message_num)
                             SELECT message_num as uid, message_num FROM messages;
                    CREATE INDEX labelidx ON labels (message_num);
                    CREATE INDEX flagidx ON flags (message_num);
                ''')
            if oldversion < '4':
                # Convert to schema 4
                sqlconn.execute('''
                    ALTER TABLE messages ADD COLUMN rfc822_msgid TEXT;
                ''')
            if oldversion < '5':
                # Convert to schema 5
                sqlconn.executescript('''
                    DROP INDEX labelidx;
                    DROP INDEX flagidx;
                    CREATE UNIQUE INDEX labelidx ON labels (message_num, label);
                    CREATE UNIQUE INDEX flagidx ON flags (message_num, flag);
                ''')
            if oldversion < '6':
                # Convert to schema 6
                sqlconn.executescript('''
                    ALTER TABLE messages ADD COLUMN is_deleted BOOLEAN DEFAULT 0;
                    UPDATE messages SET is_deleted = 0;
                    CREATE UNIQUE INDEX uididx ON uids (message_num);
                    CREATE UNIQUE INDEX messageidx ON messages (message_num, is_deleted);
                ''')
            sqlconn.executemany('REPLACE INTO settings (name, value) VALUES (?,?)',
                                                (('uidvalidity',uidvalidity),
                                                 ('db_version', __db_schema_version__)))
            sqlconn.commit()
    except sqlite3.OperationalError, e:
            print "Conversion error: %s" % e.message

    print "GYB database converted to version %s" % __db_schema_version__

def getMessageIDs(sqlconn, backup_folder):
    sqlcur = sqlconn.cursor()
    header_parser = email.parser.HeaderParser()
    for message_num, filename in sqlconn.execute('''
                             SELECT message_num, message_filename FROM messages
                             WHERE rfc822_msgid IS NULL'''):
        message_full_filename = os.path.join(backup_folder, filename)
        if os.path.isfile(message_full_filename):
            f = open(message_full_filename, 'rb')
            msgid = header_parser.parse(f, True).get('message-id') or '<DummyMsgID>'
            f.close()
            sqlcur.execute('UPDATE messages SET rfc822_msgid = ? WHERE message_num = ?',
                           (msgid, message_num))
    sqlconn.commit()

def rebuildUIDTable(imapconn, sqlconn):
    sqlcur = sqlconn.cursor()
    header_parser = email.parser.HeaderParser()
    sqlcur.execute('DELETE FROM uids')
    # Create an index on the Message ID to speed up the process
    sqlcur.execute('CREATE INDEX IF NOT EXISTS msgidx on messages(rfc822_msgid)')
    exists = imapconn.response('exists')
    exists = int(exists[1][0])
    batch_size = 1000
    for batch_start in xrange(1, exists, batch_size):
        batch_end = min(exists, batch_start + batch_size - 1)
        t, d = imapconn.fetch('%d:%d' % (batch_start, batch_end),
                              '(UID INTERNALDATE BODY.PEEK[HEADER.FIELDS '
                              '(FROM TO SUBJECT MESSAGE-ID)])')
        if t != 'OK':
            print "\nError: failed to retrieve messages."
            print "%s %s" % (t, d)
            sys.exit(5)
        for extras, header in (x for x in d if x != ')'):
            uid, message_date = re.search('UID ([0-9]*) (INTERNALDATE \".*\")',
                                          extras).groups()
            time_seconds = time.mktime(imaplib.Internaldate2tuple(message_date))
            message_internaldate = datetime.datetime.fromtimestamp(time_seconds)
            m = header_parser.parsestr(header, True)
            msgid = m.get('message-id') or '<DummyMsgID>'
            message_to = m.get('to')
            message_from = m.get('from')
            message_subject = m.get('subject')
            try:
                sqlcur.execute('''
                    INSERT INTO uids (uid, message_num)
                        SELECT ?, message_num FROM messages WHERE
                                     rfc822_msgid = ? AND
                                     message_internaldate = ?
                                     GROUP BY rfc822_msgid
                                     HAVING count(*) = 1''',
                                     (uid,
                                      msgid,
                                      message_internaldate))
            except Exception, e:
                print e
                print e.message
                print uid, msgid
            if sqlcur.lastrowid is None:
                print uid, msgid
        print "\b.",
        sys.stdout.flush()
    # There is no need to maintain the Index for normal operations
    sqlcur.execute('DROP INDEX msgidx')
    sqlconn.commit()

def doesTokenMatchEmail(cli_email, options):
    cfgFile = os.path.join(options.local_folder, cli_email + '.cfg')
    storage = oauth2client.file.Storage(cfgFile)
    credentials = storage.get()
    disable_ssl_certificate_validation = False
    if os.path.isfile(os.path.join(getProgPath(), 'noverifyssl.txt')):
        disable_ssl_certificate_validation = True
    http = httplib2.Http(ca_certs=os.path.join(getProgPath(), 'cacert.pem'), disable_ssl_certificate_validation=disable_ssl_certificate_validation)
    if options.debug:
        httplib2.debuglevel = 4
    if credentials.access_token_expired:
        credentials.refresh(http)
    oa2 = apiclient.discovery.build('oauth2', 'v2', http=http)
    token_info = callGAPI(service=oa2, function='tokeninfo', access_token=credentials.access_token)
    if token_info['email'].lower() == cli_email.lower():
        return True
    return False

def restart_line():
    sys.stdout.write('\r')
    sys.stdout.flush()

def initializeDB(sqlcur, sqlconn, email, uidvalidity):
    sqlcur.executescript('''
     CREATE TABLE messages(message_num INTEGER PRIMARY KEY,
                           message_filename TEXT,
                           message_to TEXT,
                           message_from TEXT,
                           message_subject TEXT,
                           message_internaldate TIMESTAMP,
                           rfc822_msgid TEXT,
                           is_deleted BOOLEAN DEFAULT 0);
     CREATE TABLE labels (message_num INTEGER, label TEXT);
     CREATE TABLE flags (message_num INTEGER, flag TEXT);
     CREATE TABLE uids (message_num INTEGER, uid INTEGER PRIMARY KEY);
     CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT);
     CREATE UNIQUE INDEX labelidx ON labels (message_num, label);
     CREATE UNIQUE INDEX flagidx ON flags (message_num, flag);
     CREATE UNIQUE INDEX uididx ON uids (message_num);
     CREATE UNIQUE INDEX messageidx ON messages (message_num, is_deleted);
    ''')
    sqlcur.executemany('INSERT INTO settings (name, value) VALUES (?, ?)',
                 (('email_address', email),
                  ('db_version', __db_schema_version__),
                  ('uidvalidity', uidvalidity)))
    sqlconn.commit()

def get_message_size(imapconn, uids):
    if type(uids) == type(int()):
        uid_string == str(uids)
    else:
        uid_string = ','.join(uids)
    t, d = imapconn.uid('FETCH', uid_string, '(RFC822.SIZE)')
    if t != 'OK':
        print "Failed to retrieve size for message %s" % uid_string
        print "%s %s" % (t, d)
        exit(9)
    total_size = 0
    for x in d:
        message_size = int(re.search('^[0-9]* \(UID [0-9]* RFC822.SIZE ([0-9]*)\)$', x).group(1))
        total_size = total_size + message_size
    return total_size

def getGYBVersion(divider="\n"):
    return ('Got Your Back %s~DIV~%s - %s~DIV~Python %s.%s.%s %s-bit %s~DIV~%s %s' % (__version__, __author__, __email__,
            sys.version_info[0], sys.version_info[1], sys.version_info[2], struct.calcsize('P') * 8,
            sys.version_info[3], platform.platform(), platform.machine())).replace('~DIV~', divider)

def main(argv):
    options_parser = SetupOptionParser()
    (options, args) = options_parser.parse_args(args=argv)
    if options.version:
        print getGYBVersion()
        sys.exit(0)
    if not options.email:
        options_parser.print_help()
        print "\nERROR: --email is required."
        return
    if options.local_folder == 'XXXuse-email-addressXXX':
        options.local_folder = os.path.join(os.getcwd(), "GYB-GMail-Backup-%s" % options.email)

    if not os.path.isdir(options.local_folder):
        os.mkdir(options.local_folder)

    if options.service_account: # Service Account OAuth
        if not os.path.isfile(os.path.join(getProgPath(), 'privatekey.p12')):
            print 'Error: you must have a privatekey.p12 file downloaded from the Google API Console and saved to the same path as GAM to use a service account.'
            sys.exit(1)
    else:    # 3-Legged OAuth
        if options.use_admin:
            auth_as = options.use_admin
        else:
            auth_as = options.email
        requestOAuthAccess(auth_as, options)
        if not doesTokenMatchEmail(auth_as, options):
            print "Error: you did not authorize the OAuth token in the browser with the %s Google Account. Please make sure you are logged in to the correct account when authorizing the token in the browser." % auth_as
            cfgFile = os.path.join(options.local_folder, auth_as + '.cfg')
            os.remove(cfgFile)
            sys.exit(9)

    if not os.path.isdir(options.local_folder):
        if options.action == 'backup':
            os.mkdir(options.local_folder)
        elif options.action == 'restore' or options.action == 'refresh-time':
            print 'Error: Folder %s does not exist. Cannot restore.' % options.local_folder
            sys.exit(3)

    if options.action not in ['restore-group']:
        imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
        global ALL_MAIL, TRASH, SPAM
        ALL_MAIL = gimaplib.GImapGetFolder(imapconn)
        TRASH = gimaplib.GImapGetFolder(imapconn, foldertype='\Trash')
        SPAM = gimaplib.GImapGetFolder(imapconn, foldertype='\Spam')
        if ALL_MAIL == None:
            # Last ditched best guess but All Mail is probably hidden from IMAP...
            ALL_MAIL = '[Gmail]/All Mail'
        r, d = imapconn.select(ALL_MAIL, readonly=True)
        if r == 'NO':
            print "Error: Cannot select the Gmail \"All Mail\" folder. Please make sure it is not hidden from IMAP."
            sys.exit(3)
        uidvalidity = imapconn.response('UIDVALIDITY')[1][0]

    sqldbfile = os.path.join(options.local_folder, 'msg-db.sqlite')
    # Do we need to initialize a new database?
    newDB = (not os.path.isfile(sqldbfile)) and (options.action in ['backup', 'attach2drive'])

    #If we're not doing a estimate or if the db file actually exists we open it
    #(creates db if it doesn't exist)
    if options.action not in ['estimate', 'count', 'purge'] or os.path.isfile(sqldbfile):
        print "\nUsing backup folder %s" % options.local_folder
        global sqlconn
        global sqlcur
        sqlconn = sqlite3.connect(sqldbfile, detect_types=sqlite3.PARSE_DECLTYPES)
        sqlconn.text_factory = str
        sqlcur = sqlconn.cursor()
        if newDB:
            initializeDB(sqlcur, sqlconn, options.email, uidvalidity)
        db_settings = get_db_settings(sqlcur)
        check_db_settings(db_settings, options.action, options.email)
        if options.action not in ['restore', 'restore-group']:
            if ('uidvalidity' not in db_settings or db_settings['db_version'] < __db_schema_version__):
                # backup the sqlite db file before converting it's schema
                shutil.copy(sqldbfile, sqldbfile + '.bak')
                convertDB(sqlconn, uidvalidity, db_settings['db_version'])
                db_settings = get_db_settings(sqlcur)

            # REINDEX #

            if options.action == 'reindex':
                getMessageIDs(sqlconn, options.local_folder)
                rebuildUIDTable(imapconn, sqlconn)
                # messages deleted from GMail no longer appear in the uids table
                mark_removed_messages_deleted(sqlcur, sqlconn)
                sqlconn.execute('''
                    UPDATE settings SET value = ? where name = 'uidvalidity'
                ''', ((uidvalidity),))
                sqlconn.commit()
                sys.exit(0)

            if db_settings['uidvalidity'] != uidvalidity:
                print "Because of changes on the Gmail server, this folder cannot be used for incremental backups."
                print "Run GYB with reindex action to update the unique IDs (could take a long time)."
                sys.exit(3)

    # ATTACH2DRIVE
    if options.action == 'attach2drive':
        if options.use_folder:
            print 'Using folder %s' % options.use_folder
            imapconn.select(options.use_folder, readonly=True)
        else:
            imapconn.select(ALL_MAIL, readonly=True)
        messages_to_process = gimaplib.GImapSearch(imapconn, options.gmail_search)
        backup_path = options.local_folder
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)
        messages_to_backup = []
        messages_to_refresh = []
        #Determine which messages from the search we haven't processed before.
        print "GYB needs to examine %s messages" % len(messages_to_process)
        for message_num in messages_to_process:
            if not newDB and message_is_backed_up(message_num, sqlcur, sqlconn, options.local_folder):
                messages_to_refresh.append(message_num)
            else:
                messages_to_backup.append(message_num)
        print "GYB already has a backup of %s messages" % (len(messages_to_process) - len(messages_to_backup))
        backup_count = len(messages_to_backup)
        print "GYB needs to backup %s messages" % backup_count
        messages_at_once = options.batch_size
        backed_up_messages = 0
        header_parser = email.parser.HeaderParser()
        for working_messages in batch(messages_to_backup, messages_at_once):
            #Save message content
            batch_string = ','.join(working_messages)
            bad_count = 0
            while True:
                try:
                    r, d = imapconn.uid('FETCH', batch_string, '(X-GM-LABELS INTERNALDATE FLAGS BODY.PEEK[])')
                    if r != 'OK':
                        bad_count = bad_count + 1
                        if bad_count > 7:
                            print "\nError: failed to retrieve messages."
                            print "%s %s" % (r, d)
                            sys.exit(5)
                        sleep_time = math.pow(2, bad_count)
                        sys.stdout.write("\nServer responded with %s %s, will retry in %s seconds" % (r, d, str(sleep_time)))
                        time.sleep(sleep_time) # sleep 2 seconds, then 4, 8, 16, 32, 64, 128
                        imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug)
                        imapconn.select(ALL_MAIL, readonly=True)
                        continue
                    break
                except imaplib.IMAP4.abort, e:
                    print 'imaplib.abort error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug)
                    imapconn.select(ALL_MAIL, readonly=True)
                except socket.error, e:
                    print 'socket.error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug)
                    imapconn.select(ALL_MAIL, readonly=True)
            for everything_else_string, full_message in (x for x in d if x != ')'):
                msg = email.message_from_string(full_message)
                for part in msg.walk():
                    # multipart/* are just containers
                    if part.get_content_maintype() == 'multipart':
                        continue
                    # Applications should really sanitize the given filename so that an
                    # email message can't be used to overwrite important files
                    filename = part.get_filename()
                    if not filename or filename[-4:].lower() != '.pdf':
                        continue
                    filename = filename.replace('\n', '').replace('\r', '').replace('\\', '-').replace('/', '-')
                    fp = open(os.path.join(options.local_folder, filename), 'wb')
                    fp.write(part.get_payload(decode=True))
                    fp.close()

    # BACKUP #
    if options.action == 'backup':
        if options.use_folder:
            print 'Using folder %s' % options.use_folder
            imapconn.select(options.use_folder, readonly=True)
        else:
            imapconn.select(ALL_MAIL, readonly=True)
        messages_to_process = gimaplib.GImapSearch(imapconn, options.gmail_search)
        backup_path = options.local_folder
        if not os.path.isdir(backup_path):
            os.mkdir(backup_path)
        messages_to_backup = []
        messages_to_refresh = []
        #Determine which messages from the search we haven't processed before.
        messages_count = len(messages_to_process)
        print "GYB needs to examine %s messages" % messages_count
        for message_uid in messages_to_process:
            if not newDB and message_is_backed_up(message_uid, sqlcur, sqlconn, options.local_folder):
                messages_to_refresh.append(message_uid)
            else:
                messages_to_backup.append(message_uid)
        backup_count = len(messages_to_backup)
        print "GYB already has a backup of %s messages" % (messages_count - backup_count)
        print "GYB needs to backup %s messages" % backup_count
        message_nums = []
        messages_at_once = options.batch_size
        backed_up_messages = 0
        header_parser = email.parser.HeaderParser()
        for working_messages in batch(messages_to_backup, messages_at_once):
            #Save message content
            batch_string = ','.join(working_messages)
            bad_count = 0
            while True:
                try:
                    r, d = imapconn.uid('FETCH', batch_string, '(X-GM-LABELS INTERNALDATE FLAGS BODY.PEEK[])')
                    if r != 'OK':
                        bad_count = bad_count + 1
                        if bad_count > 7:
                            print "\nError: failed to retrieve messages."
                            print "%s %s" % (r, d)
                            sys.exit(5)
                        sleep_time = math.pow(2, bad_count)
                        sys.stdout.write("\nServer responded with %s %s, will retry in %s seconds" % (r, d, str(sleep_time)))
                        time.sleep(sleep_time) # sleep 2 seconds, then 4, 8, 16, 32, 64, 128
                        imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                        imapconn.select(ALL_MAIL, readonly=True)
                        continue
                    break
                except imaplib.IMAP4.abort, e:
                    print 'imaplib.abort error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                    imapconn.select(ALL_MAIL, readonly=True)
                except socket.error, e:
                    print 'socket.error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                    imapconn.select(ALL_MAIL, readonly=True)
            for everything_else_string, full_message in (x for x in d if x != ')'):
                search_results = re.search('X-GM-LABELS \((.*)\) UID ([0-9]*) (INTERNALDATE \".*\") (FLAGS \(.*\))', everything_else_string)
                labels = shlex.split(search_results.group(1))
                uid = search_results.group(2)
                message_date_string = search_results.group(3)
                message_flags_string = search_results.group(4)
                message_date = imaplib.Internaldate2tuple(message_date_string)
                time_seconds_since_epoch = time.mktime(message_date)
                message_internal_datetime = datetime.datetime.fromtimestamp(time_seconds_since_epoch)
                message_flags = imaplib.ParseFlags(message_flags_string)
                message_file_name = "%s-%s.eml" % (uidvalidity, uid)
                message_rel_path = os.path.join(str(message_date.tm_year),
                                                str(message_date.tm_mon),
                                                str(message_date.tm_mday))
                message_rel_filename = os.path.join(message_rel_path,
                                                    message_file_name)
                message_full_path = os.path.join(options.local_folder,
                                                 message_rel_path)
                message_full_filename = os.path.join(options.local_folder,
                                                     message_rel_filename)
                if not os.path.isdir(message_full_path):
                    os.makedirs(message_full_path)
                f = open(message_full_filename, 'wb')
                f.write(full_message)
                f.close()

                m = header_parser.parsestr(full_message, True)
                message_from = m.get('from')
                message_to = m.get('to')
                message_subj = m.get('subject')
                message_id = m.get('message-id')
                sqlcur.execute("""
                                INSERT INTO messages (
                                message_filename,
                                message_to,
                                message_from,
                                message_subject,
                                message_internaldate,
                                rfc822_msgid,
                                is_deleted) VALUES (?, ?, ?, ?, ?, ?, 0)""",
                            (message_rel_filename,
                                message_to,
                                message_from,
                                message_subj,
                                message_internal_datetime,
                                message_id))
                message_num = sqlcur.lastrowid
                sqlcur.execute("""
                         REPLACE INTO uids (message_num, uid) VALUES (?, ?)""",
                                 (message_num, uid))
                for label in labels:
                    sqlcur.execute("""
                         INSERT INTO labels (message_num, label) VALUES (?, ?)""",
                                  (message_num, label))
                for flag in message_flags:
                    sqlcur.execute("""
                         INSERT INTO flags (message_num, flag) VALUES (?, ?)""",
                                   (message_num, flag))

                message_nums.append(message_num)
                backed_up_messages += 1

            sqlconn.commit()
            restart_line()
            sys.stdout.write("backed up %s of %s messages" % (backed_up_messages, backup_count))
            sys.stdout.flush()
        print ""

        for working_messages in batch(message_nums, 100):
            batch_messages = list(working_messages)
            try:
                sql = """SELECT message_filename, message_internaldate FROM messages WHERE message_num IN (?%s)""" % (',?' * (len(batch_messages) - 1),)
                sqlcur.execute(sql, tuple(batch_messages))
            except sqlite3.OperationalError, e:
                if e.message == 'no such table: messages':
                    print "\n\nError: your backup database file appears to be corrupted."
                else:
                    print "SQL error:%s" % e
                sys.exit(8)

            message_details = sqlcur.fetchall()
            set_message_file_dates(options.local_folder, message_details)

        if options.refresh:
            #get all messages on server, unless we already have them, or else we may delete too much
            if not options.gmail_search:
                messages_to_process = getMessagesToBackupList(imapconn, '')

            backed_up_message_ids = get_backed_up_message_ids(sqlcur, sqlconn, options.local_folder)
            #find local messages that are not in the server set
            deleted_uids = list(set(backed_up_message_ids.iterkeys()) - set(messages_to_process))
            mark_messages_deleted(deleted_uids, backed_up_message_ids, sqlcur, sqlconn, options.local_folder)
            mark_removed_messages_deleted(sqlcur, sqlconn)
            archive_deleted_messages(sqlcur, sqlconn, options.local_folder)

            backed_up_messages = 0
            backup_count = len(messages_to_refresh)
            print "GYB needs to refresh %s messages" % backup_count
            sqlcur.executescript("""
                 CREATE TEMP TABLE current_labels (label TEXT);
                 CREATE TEMP TABLE current_flags (flag TEXT);
            """)

            messages_at_once *= 100
            for working_messages in batch(messages_to_refresh, messages_at_once):
                #Save message content
                batch_string = ','.join(working_messages)
                bad_count = 0
                while True:
                    try:
                        r, d = imapconn.uid('FETCH', batch_string, '(X-GM-LABELS FLAGS)')
                        if r != 'OK':
                            bad_count = bad_count + 1
                            if bad_count > 7:
                                print "\nError: failed to retrieve messages."
                                print "%s %s" % (r, d)
                                sys.exit(5)
                            sleep_time = math.pow(2, bad_count)
                            sys.stdout.write("\nServer responded with %s %s, will retry in %s seconds" % (r, d, str(sleep_time)))
                            time.sleep(sleep_time) # sleep 2 seconds, then 4, 8, 16, 32, 64, 128
                            imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                            imapconn.select(ALL_MAIL, readonly=True)
                            continue
                        break
                    except imaplib.IMAP4.abort, e:
                        print 'imaplib.abort error:%s, retrying...' % e
                        imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                        imapconn.select(ALL_MAIL, readonly=True)
                    except socket.error, e:
                        print 'socket.error:%s, retrying...' % e
                        imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                        imapconn.select(ALL_MAIL, readonly=True)
                for results in d:
                    search_results = re.search('X-GM-LABELS \((.*)\) UID ([0-9]*) (FLAGS \(.*\))', results)
                    labels = shlex.split(search_results.group(1))
                    uid = search_results.group(2)
                    message_flags_string = search_results.group(3)
                    message_flags = imaplib.ParseFlags(message_flags_string)
                    sqlcur.execute('DELETE FROM current_labels')
                    sqlcur.execute('DELETE FROM current_flags')
                    sqlcur.executemany('INSERT INTO current_labels (label) VALUES (?)',
                                ((label,) for label in labels))
                    sqlcur.executemany('INSERT INTO current_flags (flag) VALUES (?)',
                                ((flag,) for flag in message_flags))
                    sqlcur.execute("""DELETE FROM labels where message_num =
                                         (SELECT message_num from uids where uid = ?)
                                            AND label NOT IN current_labels""", ((uid),))
                    sqlcur.execute("""DELETE FROM flags where message_num =
                                         (SELECT message_num from uids where uid = ?)
                                            AND flag NOT IN current_flags""", ((uid),))
                    sqlcur.execute("""INSERT INTO labels (message_num, label)
                            SELECT message_num, label from uids, current_labels
                                 WHERE uid = ? AND label NOT IN
                                 (SELECT label FROM labels
                                        WHERE message_num = uids.message_num)""", ((uid),))
                    sqlcur.execute("""INSERT INTO flags (message_num, flag)
                            SELECT message_num, flag from uids, current_flags
                                 WHERE uid = ? AND flag NOT IN
                                 (SELECT flag FROM flags
                                        WHERE message_num = uids.message_num)""", ((uid),))
                    backed_up_messages += 1

                sqlconn.commit()
                restart_line()
                sys.stdout.write("refreshed %s of %s messages" % (backed_up_messages, backup_count))
                sys.stdout.flush()
        print "\n"

        if options.zip:
            create_compressed_archives(options.local_folder)

    # RESTORE #
    elif options.action == 'restore':
        if options.use_folder:
            imapconn.select(options.use_folder)
        else:
            imapconn.select(ALL_MAIL) # read/write!
        resumedb = os.path.join(options.local_folder, 
                                "%s-restored.sqlite" % options.email)
        if options.noresume:
            try:
                os.remove(resumedb)
            except IOError:
                pass
        sqlcur.execute('ATTACH ? as resume', (resumedb,))
        sqlcur.executescript('''CREATE TABLE IF NOT EXISTS resume.restored_messages 
                                (message_num INTEGER PRIMARY KEY); 
                                CREATE TEMP TABLE skip_messages (message_num INTEGER PRIMARY KEY);''')
        sqlcur.execute('''INSERT INTO skip_messages SELECT message_num from restored_messages''')
        sqlcur.execute('''SELECT message_num, message_internaldate, message_filename FROM messages
                          WHERE (is_deleted = 0 OR is_deleted IS NULL) AND message_num NOT IN skip_messages
                          ORDER BY message_internaldate DESC''') # All messages
        messages_to_restore_results = sqlcur.fetchall()
        restore_count = len(messages_to_restore_results)
        current = 0
        for x in messages_to_restore_results:
            restart_line()
            current += 1
            sys.stdout.write("restoring message %s of %s from %s" % (current, restore_count, x[1]))
            sys.stdout.flush()
            message_num = x[0]
            message_internaldate = x[1]
            message_internaldate_seconds = time.mktime(message_internaldate.timetuple())
            message_filename = x[2]
            if not os.path.isfile(os.path.join(options.local_folder, message_filename)):
                print 'WARNING! file %s does not exist for message %s' % (os.path.join(options.local_folder, message_filename), message_num)
                print '  this message will be skipped.'
                continue
            f = open(os.path.join(options.local_folder, message_filename), 'rb')
            full_message = f.read()
            f.close()
            labels_query = sqlcur.execute('SELECT DISTINCT label FROM labels WHERE message_num = ?', (message_num,))
            labels_results = sqlcur.fetchall()
            labels = []
            for l in labels_results:
                labels.append(l[0].replace('\\','\\\\').replace('"','\\"'))
            if options.label_restored:
                labels.append(options.label_restored)
            flags_query = sqlcur.execute('SELECT DISTINCT flag FROM flags WHERE message_num = ?', (message_num,))
            flags_results = sqlcur.fetchall()
            flags = []
            for f in flags_results:
                flags.append(f[0])
            flags_string = ' '.join(flags)
            while True:
                try:
                    r, d = imapconn.append(ALL_MAIL, flags_string, message_internaldate_seconds, full_message)
                    if r != 'OK':
                        print '\nError: %s %s' % (r,d)
                        sys.exit(5)
                    restored_uid = int(re.search('^[APPENDUID [0-9]* ([0-9]*)] \(Success\)$', d[0]).group(1))
                    if len(labels) > 0:
                        labels_string = '("' + '" "'.join(labels) + '")'
                        r, d = imapconn.uid('STORE', restored_uid, '+X-GM-LABELS', labels_string)
                        if r != 'OK':
                            print '\nGImap Set Message Labels Failed: %s %s' % (r, d)
                            sys.exit(33)
                    break
                except imaplib.IMAP4.abort, e:
                    print '\nimaplib.abort error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                    imapconn.select(ALL_MAIL)
                except socket.error, e:
                    print '\nsocket.error:%s, retrying...' % e
                    imapconn = gimaplib.ImapConnect(generateXOAuthString(options), options.debug, options.compress)
                    imapconn.select(ALL_MAIL)
            #Save the fact that it is completed
            sqlconn.execute('INSERT OR IGNORE INTO restored_messages (message_num) VALUES (?)',
                     (message_num,))
            sqlconn.commit()
        sqlconn.execute('DETACH resume')
        sqlconn.commit()

    # RESTORE-GROUP #
    elif options.action == 'restore-group':
        resumedb = os.path.join(options.local_folder,
                                "%s-restored.sqlite" % options.email)
        if options.noresume:
            try:
                os.remove(resumedb)
            except IOError:
                pass
        sqlcur.execute('ATTACH ? as resume', (resumedb,))
        sqlcur.executescript('''CREATE TABLE IF NOT EXISTS resume.restored_messages
                                (message_num INTEGER PRIMARY KEY);
             CREATE TEMP TABLE skip_messages (message_num INTEGER PRIMARY KEY);''')
        sqlcur.execute('''INSERT INTO skip_messages SELECT message_num from restored_messages''')
        sqlcur.execute('''SELECT message_num, message_internaldate, message_filename FROM messages
                    WHERE message_num NOT IN skip_messages ORDER BY message_internaldate DESC''') # All messages
        messages_to_restore_results = sqlcur.fetchall()
        restore_count = len(messages_to_restore_results)
        if options.service_account:
            if not options.use_admin:
                print 'Error: --restore_group and --service_account require --user_admin to specify Google Apps Admin to utilize.'
                sys.exit(5)
            f = file(getProgPath() + 'privatekey.p12', 'rb')
            key = f.read()
            f.close()
            scope = 'https://www.googleapis.com/auth/apps.groups.migration'
            credentials = oauth2client.client.SignedJwtAssertionCredentials(options.service_account, key, scope=scope, prn=options.use_admin)
            disable_ssl_certificate_validation = False
            if os.path.isfile(getProgPath() + 'noverifyssl.txt'):
                disable_ssl_certificate_validation = True
            http = httplib2.Http(ca_certs=getProgPath() + 'cacert.pem', disable_ssl_certificate_validation=disable_ssl_certificate_validation)
            if options.debug:
                httplib2.debuglevel = 4
            http = credentials.authorize(http)
        elif options.use_admin:
            cfgFile = os.path.join(getProgPath(), options.use_admin + '.cfg')
            f = open(cfgFile, 'rb')
            token = simplejson.load(f)
            f.close()
            storage = oauth2client.file.Storage(cfgFile)
            credentials = storage.get()
            disable_ssl_certificate_validation = False
            if os.path.isfile(getProgPath() + 'noverifyssl.txt'):
                disable_ssl_certificate_validation = True
            http = httplib2.Http(ca_certs=getProgPath() + 'cacert.pem', disable_ssl_certificate_validation=disable_ssl_certificate_validation)
            if options.debug:
                httplib2.debuglevel = 4
            http = credentials.authorize(http)
        else:
            print 'Error: restore-group requires that --use_admin is also specified.'
            sys.exit(5)
        gmig = apiclient.discovery.build('groupsmigration', 'v1', http=http)
        current = 0
        for x in messages_to_restore_results:
            restart_line()
            current += 1
            sys.stdout.write("restoring message %s of %s from %s" % (current, restore_count, x[1]))
            sys.stdout.flush()
            message_num = x[0]
            message_internaldate = x[1]
            message_filename = x[2]
            if not os.path.isfile(os.path.join(options.local_folder, message_filename)):
                print 'WARNING! file %s does not exist for message %s' % (os.path.join(options.local_folder, message_filename), message_num)
                print '  this message will be skipped.'
                continue
            f = open(os.path.join(options.local_folder, message_filename), 'rb')
            full_message = f.read()
            f.close()
            media = apiclient.http.MediaFileUpload(os.path.join(options.local_folder, message_filename), mimetype='message/rfc822')
            callGAPI(service=gmig.archive(), function='insert', groupId=options.email, media_body=media)
            #Save the fact that it is completed
            sqlconn.execute('INSERT INTO restored_messages (message_num) VALUES (?)',
                     (message_num,))
            sqlconn.commit()
        sqlconn.execute('DETACH resume')
        sqlconn.commit()

    # COUNT
    elif options.action == 'count':
        if options.use_folder:
            print 'Using label %s' % options.use_folder
            imapconn.select(options.use_folder, readonly=True)
        else:
            imapconn.select(ALL_MAIL, readonly=True)
        messages_to_process = gimaplib.GImapSearch(imapconn, options.gmail_search)
        messages_to_estimate = []
        #if we have a sqlcur , we'll compare messages to the db
        #otherwise just estimate everything
        for message_num in messages_to_process:
            try:
                sqlcur
                if message_is_backed_up(message_num, sqlcur, sqlconn, options.local_folder):
                    continue
                else:
                    messages_to_estimate.append(message_num)
            except NameError:
                messages_to_estimate.append(message_num)
        estimate_count = len(messages_to_estimate)
        total_size = float(0)
        list_position = 0
        messages_at_once = 10000
        loop_count = 0
        print "%s,%s" % (options.email, estimate_count)

    # PURGE #
    elif options.action == 'purge':
        imapconn.select(ALL_MAIL, readonly=False)
        messages_to_process = gimaplib.GImapSearch(imapconn, options.gmail_search)
        print 'Moving %s messages from All Mail to Trash for %s' % (len(messages_to_process), options.email)
        messages_at_once = 1000
        loop_count = 0
        for working_messages in batch(messages_to_process, messages_at_once):
            uid_string = ','.join(working_messages)
            t, d = imapconn.uid('STORE', uid_string, '+X-GM-LABELS', '\\Trash')
        r, d = imapconn.select(SPAM, readonly=False)
        if r == 'NO':
            print "Error: Cannot select the Gmail \"Spam\" folder. Please make sure it is not hidden from IMAP."
            sys.exit(3)
        spam_uids = gimaplib.GImapSearch(imapconn, options.gmail_search)
        print 'Purging %s Spam messages for %s' % (len(spam_uids), options.email)
        for working_messages in batch(spam_uids, messages_at_once):
            spam_uid_string = ','.join(working_messages)
            t, d = imapconn.uid('STORE', spam_uid_string, '+FLAGS', '\Deleted')
        imapconn.expunge()
        r, d = imapconn.select(TRASH, readonly=False)
        if r == 'NO':
            print "Error: Cannot select the Gmail \"Trash\" folder. Please make sure it is not hidden from IMAP."
            sys.exit(3)
        trash_uids = gimaplib.GImapSearch(imapconn, options.gmail_search) 
        print 'Purging %s Trash messages for %s' % (len(trash_uids), options.email)
        for working_messages in batch(trash_uids, messages_at_once):
            trash_uid_string = ','.join(working_messages)
            t, d = imapconn.uid('STORE', trash_uid_string, '+FLAGS', '\Deleted')
        imapconn.expunge()

    # ESTIMATE #
    elif options.action == 'estimate':
        if options.use_folder:
            imapconn.select(options.use_folder, readonly=True)
        else:
            imapconn.select(ALL_MAIL, readonly=True)
        messages_to_process = gimaplib.GImapSearch(imapconn, options.gmail_search)
        messages_to_estimate = []
        #if we have a sqlcur , we'll compare messages to the db
        #otherwise just estimate everything
        for message_uid in messages_to_process:
            try:
                sqlcur
                if message_is_backed_up(message_uid, sqlcur, sqlconn, options.local_folder):
                    continue
                else:
                    messages_to_estimate.append(message_uid)
            except NameError:
                messages_to_estimate.append(message_uid)
        estimate_count = len(messages_to_estimate)
        total_size = float(0)
        list_position = 0
        messages_at_once = 10000
        loop_count = 0
        print 'Email: %s' % options.email
        print "Messages to estimate: %s" % estimate_count
        estimated_messages = 0
        for working_messages in batch(messages_to_estimate, messages_at_once):
            messages_size = get_message_size(imapconn, working_messages)
            total_size = total_size + messages_size
            if total_size > 1048576:
                math_size = total_size / 1048576
                print_size = "%.2fM" % math_size
            elif total_size > 1024:
                math_size = total_size / 1024
                print_size = "%.2fK" % math_size
            else:
                print_size = "%.2fb" % total_size
            if estimated_messages + messages_at_once < estimate_count:
                estimated_messages = estimated_messages + messages_at_once
            else:
                estimated_messages = estimate_count
            restart_line()
            sys.stdout.write("Messages estimated: %s Estimated size: %s" % (estimated_messages, print_size))
            sys.stdout.flush()
            time.sleep(1)
        print ""

    # REFRESH EML FILE TIMES #
    elif options.action == 'refresh-time':
        try:
            sqlcur.execute("""SELECT message_filename, message_internaldate FROM messages""")
        except sqlite3.OperationalError, e:
            if e.message == 'no such table: messages':
                print "\n\nError: your backup database file appears to be corrupted."
            else:
                print "SQL error:%s" % e
            sys.exit(8)

        message_details = sqlcur.fetchall()
        set_message_file_dates(options.local_folder, message_details)

    try:
        sqlconn.close()
    except NameError:
        pass
    if options.compress > 0:
        imapconn.display_stats()
    try:
        imapconn.logout()
    except UnboundLocalError: # group-restore never does imapconn
        pass

if __name__ == '__main__':
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        try:
            sqlconn.commit()
            sqlconn.close()
            print
        except NameError:
            pass
        sys.exit(4)
