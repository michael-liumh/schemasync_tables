#!/usr/bin/python3

import sys
import os
import logging
import datetime
import warnings
import argparse
import syncdb
import utils
import json
from connection import parse_database_url, DatabaseError, DatabaseConnection
from schema import SchemaObject

__author__ = """
Mitch Matuson
Mustafa Ozgur
Michael Liu
"""
__copyright__ = """
Copyright 2009-2021 Mitch Matuson
Copyright 2016 Mustafa Ozgur
Copyright 2021 Michael Liu
"""
__version__ = "1.0.0"
__license__ = "Apache 2.0"

# supress MySQLdb DeprecationWarning in Python 2.6
warnings.simplefilter("ignore", DeprecationWarning)

try:
    import pymysql
except ImportError:
    print("Error: Missing Required Dependency PyMySQL.")
    sys.exit(1)

APPLICATION_VERSION = __version__
APPLICATION_NAME = "Schema Sync"
LOG_FILENAME = "schemasync.log"
DATE_FORMAT = "%Y%m%d"
TPL_DATE_FORMAT = "%a, %b %d, %Y"
PATCH_TPL = """--
-- Schema Sync %(app_version)s %(type)s
-- Created: %(created)s
-- Server Version: %(server_version)s
-- Apply To: %(target_host)s:%(target_port)s/%(target_database)s
--

%(data)s"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def set_log_format():
    import logging.handlers
    import colorlog

    global logger

    # set logger color
    log_colors_config = {
        'DEBUG': 'bold_purple',
        'INFO': 'bold_green',
        'WARNING': 'bold_yellow',
        'ERROR': 'bold_red',
        'CRITICAL': 'bold_red',
    }

    # set logger format
    log_format = colorlog.ColoredFormatter(
        "%(log_color)s[%(asctime)s] [%(module)s:%(funcName)s] [%(lineno)d] [%(levelname)s] %(message)s",
        log_colors=log_colors_config
    )

    # add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # add rotate file handler
    base_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(base_dir, 'logs')
    if not os.path.isdir(logs_dir):
        os.makedirs(logs_dir, exist_ok=True)

    logfile = logs_dir + os.sep + sys.argv[0].split(os.sep)[-1].split('.')[0] + '.log'
    file_maxsize = 1024 * 1024 * 100  # 100m
    # logfile_size = os.path.getsize(logfile) if os.path.exists(logfile) else 0

    file_handler = logging.handlers.RotatingFileHandler(logfile, maxBytes=file_maxsize, backupCount=10)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)


def parse_cmd_line(fn):
    def processor():
        """Parse the command line options and pass them to the application"""

        usage = """
            python3 %s [options] --source <source> --target <target>
            source/target format: mysql://user:pass@host:port/database
        """ % sys.argv[0]
        description = """A MySQL Schema Synchronization Utility"""

        parser = argparse.ArgumentParser(description=description, usage=usage, add_help=False,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('--help',
                            action='store_true',
                            dest='help',
                            default=False,
                            help='help information')

        parser.add_argument("-V", "--version",
                            action="store_true",
                            dest="show_version",
                            default=False,
                            help="show version and exit.")

        parser.add_argument("-r", "--revision",
                            dest="version_filename",
                            action="store_true",
                            default=False,
                            help=("increment the migration script version number "
                                  "if a file with the same name already exists."))

        parser.add_argument("-a", "--sync-auto-inc",
                            dest="sync_auto_inc",
                            action="store_true",
                            default=False,
                            help="sync the AUTO_INCREMENT value for each table.")

        parser.add_argument("-c", "--sync-comments",
                            dest="sync_comments",
                            action="store_true",
                            default=False,
                            help=("sync the COMMENT field for all "
                                  "tables AND columns"))

        parser.add_argument("-D", "--no-date",
                            dest="no_date",
                            action="store_true",
                            default=False,
                            help="removes the date from the file format ")

        parser.add_argument("--source",
                            dest="source_db",
                            type=str,
                            nargs='*',
                            help="source database url")

        parser.add_argument("--target",
                            dest="target_db",
                            type=str,
                            nargs='*',
                            help="target database url")

        parser.add_argument("--charset",
                            dest="charset",
                            type=str,
                            default='utf8',
                            help="set the connection charset, default: utf8")

        parser.add_argument("--tag",
                            dest="tag",
                            type=str,
                            help=("tag the migration scripts as <database>_<tag>."
                                  " Valid characters include [A-Za-z0-9-_]"))

        parser.add_argument("--out-dir",
                            dest="output_directory",
                            type=str,
                            default=os.getcwd(),
                            help=("directory to write the migration scrips. "
                                  "The default is current working directory. "
                                  "Must use absolute path if provided."))

        # parser.add_argument("--log-dir",
        #                     dest="log_directory",
        #                     type=str,
        #                     help=("set the directory to write the log to. "
        #                           "Must use absolute path if provided. "
        #                           "Default is output directory. "
        #                           "Log filename is schemasync.log"))

        parser.add_argument("--tables",
                            dest="filter_tables",
                            type=str,
                            nargs='*',
                            help="New feature: only sync the specified tables")

        parser.add_argument("--views",
                            dest="filter_views",
                            type=str,
                            nargs='*',
                            help="New feature: only sync the specified views")

        parser.add_argument("--triggers",
                            dest="filter_triggers",
                            type=str,
                            nargs='*',
                            help="New feature: only sync the specified triggers")

        parser.add_argument("--procedures",
                            dest="filter_procedures",
                            type=str,
                            nargs='*',
                            help="New feature: only sync the specified procedures")

        parser.add_argument("--only-sync-exists-tables",
                            dest="only_sync_exists_tables",
                            action="store_true",
                            default=False,
                            help="New feature: only sync the exists tables in target")

        parser.add_argument("--url",
                            dest="alert_url",
                            type=str,
                            help="New feature: when schema is not sync, "
                                 "send alert to fei shu with fei shu web hook url "
                                 "(tips: result file will be deleted.)")

        parser.add_argument("--no-delete",
                            dest="no_delete_result",
                            action="store_true",
                            default=False,
                            help="New feature: when use --url args, "
                                 "do not delete the result file "
                                 "(tips: default is delete.)")

        args = parser.parse_args(sys.argv[1:])
        if args.show_version:
            print(APPLICATION_NAME, __version__)
            return 0

        need_print_help = False if args else True
        if args.help or need_print_help or not args.source_db or not args.target_db:
            if not args.source_db or not args.target_db:
                logger.error('Missing source or target instance.')
            parser.print_help()
            return 0

        if not args.source_db or not args.target_db:
            logger.error('Missing source or target instance.')
            return 0

        return fn(**dict(sourcedb=args.source_db[0],
                         targetdb=args.target_db[0],
                         version_filename=args.version_filename,
                         output_directory=args.output_directory,
                         log_directory=os.getcwd(),
                         no_date=args.no_date,
                         tag=args.tag,
                         charset=args.charset,
                         sync_auto_inc=args.sync_auto_inc,
                         sync_comments=args.sync_comments,
                         filter_tables=args.filter_tables,
                         filter_views=args.filter_views,
                         filter_triggers=args.filter_triggers,
                         filter_procedures=args.filter_procedures,
                         only_sync_exists_tables=args.only_sync_exists_tables,
                         alert_url=args.alert_url,
                         no_delete_result=args.no_delete_result))

    return processor


def app(sourcedb='', targetdb='', version_filename=False,
        output_directory=None, log_directory=None, no_date=False,
        tag=None, charset=None, sync_auto_inc=False, sync_comments=False,
        filter_tables=None, filter_views=None, filter_triggers=None, filter_procedures=None,
        only_sync_exists_tables=False, alert_url=None, no_delete_result=False):
    """Main Application"""

    options = locals()

    if not os.path.isabs(output_directory):
        print("Error: Output directory must be an absolute path. Quiting.")
        return 1

    if not os.path.isdir(output_directory):
        print("Error: Output directory does not exist. Quiting.")
        return 1

    if not log_directory or not os.path.isdir(log_directory):
        if log_directory:
            print("Log directory does not exist, writing log to %s" % output_directory)
        log_directory = output_directory

    if not os.path.isdir(os.path.join(log_directory, 'logs')):
        os.makedirs(os.path.join(log_directory, 'logs'), exist_ok=True)

    logging.basicConfig(filename=os.path.join(log_directory, 'logs', LOG_FILENAME),
                        level=logging.INFO,
                        format='[%(levelname)s  %(asctime)s] %(message)s')

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    if len(logging.getLogger('').handlers) <= 1:
        logging.getLogger('').addHandler(console)

    if not sourcedb:
        logging.error("Source database URL not provided. Exiting.")
        return 1

    source_info = parse_database_url(sourcedb)
    if not source_info:
        logging.error("Invalid source database URL format. Exiting.")
        return 1

    if not source_info['protocol'] == 'mysql':
        logging.error("Source database must be MySQL. Exiting.")
        return 1

    if 'db' not in source_info:
        logging.error("Source database name not provided. Exiting.")
        return 1

    if not targetdb:
        logging.error("Target database URL not provided. Exiting.")
        return 1

    target_info = parse_database_url(targetdb)
    if not target_info:
        logging.error("Invalid target database URL format. Exiting.")
        return 1

    if not target_info['protocol'] == 'mysql':
        logging.error("Target database must be MySQL. Exiting.")
        return 1

    if 'db' not in target_info:
        logging.error("Target database name not provided. Exiting.")
        return 1

    if source_info['db'] == '*' and target_info['db'] == '*':
        # from schemaobject.connection import DatabaseConnection

        sourcedb_none = sourcedb[:-1]
        targetdb_none = targetdb[:-1]
        connection = DatabaseConnection()
        connection.connect(sourcedb_none, charset='utf8')
        sql_schema = """
        SELECT SCHEMA_NAME FROM information_schema.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
        """
        schemas = connection.execute(sql_schema)
        for schema_info in schemas:
            db = schema_info['SCHEMA_NAME']
            sourcedb = sourcedb_none + db
            targetdb = targetdb_none + db
            try:
                app(sourcedb=sourcedb, targetdb=targetdb, version_filename=version_filename,
                    output_directory=output_directory, log_directory=log_directory, no_date=no_date,
                    tag=tag, charset=charset, sync_auto_inc=sync_auto_inc, sync_comments=sync_comments,
                    filter_tables=filter_tables, filter_views=filter_views, filter_triggers=filter_triggers,
                    filter_procedures=filter_procedures, only_sync_exists_tables=only_sync_exists_tables,
                    alert_url=alert_url, no_delete_result=no_delete_result)
            except DatabaseError as e:
                logging.error("MySQL Error %d: %s (Ignore)" % (e.args[0], e.args[1]))
        return 1

    source_obj = SchemaObject(sourcedb, charset)
    target_obj = SchemaObject(targetdb, charset)

    if utils.compare_version(source_obj.version, '5.0.0') < 0:
        logging.error("%s requires MySQL version 5.0+ (source is v%s)"
                      % (APPLICATION_NAME, source_obj.version))
        return 1

    if utils.compare_version(target_obj.version, '5.0.0') < 0:
        logging.error("%s requires MySQL version 5.0+ (target is v%s)"
                      % (APPLICATION_NAME, target_obj.version))
        return 1

    if only_sync_exists_tables:
        # from schemaobject.connection import DatabaseConnection

        connection = DatabaseConnection()
        connection.connect(targetdb, charset='utf8')
        sql_tables = """
        SHOW TABLES FROM %s
        """ % target_obj.selected.name
        tables = connection.execute(sql_tables)
        key = 'Tables_in_%s' % target_obj.selected.name
        filter_tables = list(map(lambda d: d[key], tables))
        connection.close()

    # data transformation filters
    filters = (lambda d: utils.REGEX_MULTI_SPACE.sub(' ', d),
               lambda d: utils.REGEX_DISTANT_SEMICOLIN.sub(';', d),
               lambda d: utils.REGEX_SEMICOLON_EXPLODE_TO_NEWLINE.sub(";\n", d))

    # Information about this run, used in the patch/revert templates
    ctx = dict(app_version=APPLICATION_VERSION,
               server_version=target_obj.version,
               target_host=target_obj.host,
               target_port=target_obj.port,
               target_database=target_obj.selected.name,
               created=datetime.datetime.now().strftime(TPL_DATE_FORMAT))

    # patch_filename, revert_filename
    p_fname, r_fname = utils.create_pnames(target_obj.selected.name,
                                           tag=tag,
                                           date_format=DATE_FORMAT,
                                           no_date=no_date)

    ctx['type'] = "Patch Script"
    p_buffer = utils.PatchBuffer(name=os.path.join(output_directory, p_fname),
                                 filters=filters, tpl=PATCH_TPL, ctx=ctx.copy(),
                                 version_filename=version_filename)

    ctx['type'] = "Revert Script"
    r_buffer = utils.PatchBuffer(name=os.path.join(output_directory, r_fname),
                                 filters=filters, tpl=PATCH_TPL, ctx=ctx.copy(),
                                 version_filename=version_filename)

    db_selected = False
    for patch, revert in syncdb.sync_schema(source_obj.selected,
                                            target_obj.selected, options,
                                            filter_tables=filter_tables):
        if patch and revert:
            if not db_selected:
                p_buffer.write(target_obj.selected.select() + '\n')
                r_buffer.write(target_obj.selected.select() + '\n')
                p_buffer.write(target_obj.selected.fk_checks(0) + '\n')
                r_buffer.write(target_obj.selected.fk_checks(0) + '\n')
                db_selected = True

            p_buffer.write(patch + '\n')
            r_buffer.write(revert + '\n')

    if db_selected:
        p_buffer.write(target_obj.selected.fk_checks(1) + '\n')
        r_buffer.write(target_obj.selected.fk_checks(1) + '\n')

    for patch, revert in syncdb.sync_views(source_obj.selected, target_obj.selected,
                                           filter_views=filter_views):
        if patch and revert:
            if not db_selected:
                p_buffer.write(target_obj.selected.select() + '\n')
                r_buffer.write(target_obj.selected.select() + '\n')
                db_selected = True

            p_buffer.write(patch + '\n')
            r_buffer.write(revert + '\n')

    for patch, revert in syncdb.sync_triggers(source_obj.selected, target_obj.selected,
                                              filter_triggers=filter_triggers):
        if patch and revert:
            if not db_selected:
                p_buffer.write(target_obj.selected.select() + '\n')
                r_buffer.write(target_obj.selected.select() + '\n')
                db_selected = True

            p_buffer.write(patch + '\n')
            r_buffer.write(revert + '\n')

    for patch, revert in syncdb.sync_procedures(source_obj.selected, target_obj.selected,
                                                filter_procedures=filter_procedures):
        if patch and revert:
            if not db_selected:
                p_buffer.write(target_obj.selected.select() + '\n')
                r_buffer.write(target_obj.selected.select() + '\n')
                p_buffer.write(target_obj.selected.fk_checks(0) + '\n')
                r_buffer.write(target_obj.selected.fk_checks(0) + '\n')
                db_selected = True

            p_buffer.write(patch + '\n')
            r_buffer.write(revert + '\n')

    if not p_buffer.modified:
        print(("No migration scripts written."
               " mysql://%s:%s/%s and mysql://%s:%s/%s were in sync.") %
              (source_obj.host, source_obj.port, source_obj.selected.name,
               target_obj.host, target_obj.port, target_obj.selected.name))
    else:
        try:
            p_buffer.save()
            r_buffer.save()
            logging.info("Migration scripts created for mysql://%s:%s/%s\n"
                         "Patch Script: %s\nRevert Script: %s"
                         % (target_obj.host, target_obj.port, target_obj.selected.name,
                            p_buffer.name, r_buffer.name))

            if os.path.exists(p_buffer.name) and p_buffer.modified and alert_url:
                logger.warning("alerting...")
                target_addr = target_obj.host + ':' + str(target_obj.port) + '/' + target_obj.selected.name
                send_alert(p_buffer.name, target_addr, alert_url)

                if not no_delete_result:
                    os.remove(p_buffer.name)
                    os.remove(r_buffer.name)
                    logger.info('deleted ' + str(p_buffer.name))
                    logger.info('deleted ' + str(r_buffer.name))

        except OSError as e:
            p_buffer.delete()
            r_buffer.delete()
            logging.error("Failed writing migration scripts. %s" % e)
            return 1

    return 0


def send_alert(filename, target_addr, alert_url):
    if os.path.isfile(filename):
        with open(filename, encoding='utf8') as f:
            infos = list(map(lambda s: s.replace('\n', ''), f.readlines()))[7:]

        infos.remove('SET FOREIGN_KEY_CHECKS = 0;')
        infos.remove('SET FOREIGN_KEY_CHECKS = 1;')

        alert_msg = {
            '目标实例': target_addr,
            '同步SQL': infos
        }
        msg = json.dumps(alert_msg, ensure_ascii=False, indent=4)
        logger.info(msg)

        title = '分库表结构不一致告警'
        utils.send_msg_2_fei_shu(alert_url, msg, title, is_at_all=True)
    else:
        logger.error(str(filename) + ' does not exists.')


def main():
    try:
        sys.exit(parse_cmd_line(app)())
    except DatabaseError as e:
        logging.error("MySQL Error %d: %s" % (e.args[0], e.args[1]))
        sys.exit(1)
    except KeyboardInterrupt:
        print("Sync Interrupted, Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    set_log_format()
    main()
