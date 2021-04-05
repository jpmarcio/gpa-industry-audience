import paramiko, gnupg
import signal
import glob
from postgre_script import *
from credentials import *
from multiprocessing import Pool
from datetime import datetime
from collections import OrderedDict
from config import config
import gzip


def initializer():
    """Ignore CTRL+C in the worker process."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def connect_sftp():
    ssh = paramiko.SSHClient()
    ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(credentials['axway']['host'],
                username=credentials['axway']['user'],
                password=credentials['axway']['password'],
                timeout=100,
                compress=False,
                allow_agent=False,
                look_for_keys=False)
    ssh.get_transport().window_size = 2147483647
    console = ssh.open_sftp()
    console.keep_this = ssh
    return console


def download_from_sftp(filename):
    print('\t started ' + filename)
    sftp = connect_sftp()
    lfile = open(config['localpath'] + filename, 'w+')
    sftp.getfo(config['remotepath'] + filename, lfile)
    lfile.close()
    sftp.close()
    print('\t finished ' + filename)


def db_insert(filename):
    fileprefix = filename.split('.')[0]

    if fileprefix.startswith('create_'):
        return

    if fileprefix in ('prod_inc_prsn_count', 'prod_new_prsn_count_filter', 'index_l21','product_structure',
                      'obj_filter_count', 'obj_structure_count', 'un_filters', 'personas','conversion_rates','md_dash_api_cube_no1'
                      ,'md_dash_api_cube_no2','md_dash_api_cube_no3','md_dash_api_cube_no4'):
        recreate_and_load_table(fileprefix, filename)

    else:
        name = filename.split('=')[1]
        insert_into_db(name, filename)
        create_indexes(name)


def untar_files(localpath, fi):
    if os.path.exists(localpath + fi):
        gpg_time = os.stat(localpath + fi).st_mtime

    else:
        raise Exception("File {f} does not exist!".format(f=localpath + fi))

    ungpg = fi.split('.gpg')[0]
    ungpg_time = 0 if not os.path.exists(localpath + ungpg) else os.stat(localpath + ungpg).st_mtime
    ungz = ungpg.split('.gz')[0].split('.tar')[0]
    ungz_time = 0 if not os.path.exists(localpath + ungz) else os.stat(localpath + ungz).st_mtime

    if ungz_time > gpg_time:
        print("{f1} is already newer than {f2}".format(p=localpath,
                                                       f1=ungz,
                                                       f2=fi))
        return ungz

    if 0 < ungpg_time < gpg_time:
        os.remove(localpath + ungpg)

    if gpg_time > ungpg_time:
        print("Decrypting {f} into {d}".format(f=fi,
                                               d=ungpg))
        ##original
        os.system('gpg --batch --passphrase-fd 1 --passphrase-file {passfile} --output {p}{f1} {p}{f2}'.format(passfile=config['passfile'],p=localpath,
                                                                                                            f1=ungpg,
                                                                                                            f2=fi))
                                                                                               
    if ungz_time > 0 and ungpg != ungz:
        os.remove(localpath + ungz)

    if ungpg != ungz:
        print("Decompressing {p}{f}".format(p=localpath,
                                            f=ungpg))
        if os.name!='nt':
            os.system('gunzip {p}{f}'.format(p=localpath,
                                         f=ungpg))
        else:
            #windows
            with gzip.open('{p}{f}'.format(p=localpath,f=ungpg), 'r') as f:
                file_content = f.read()
                file_content = file_content.decode('utf-8')
                f_out = open(localpath + ungz.split('.')[0] , 'w+')
                f_out.write(file_content)
                f.close()
                f_out.close()

            
    for rn in glob.glob(localpath + ungz.split('.')[0] + '*.tar'):
        os.rename(rn, rn[:-4])


    return ungz


def custom_sorter(s):
    if s.startswith('index'):
        return 100

    if s.startswith('create_'):
        return 1

    if s.startswith('prod_comml'):
        return 10

    if s.startswith('un_filter'):
        return 20

    return 15


def seconds_to_ftime(elapsed):
    m, seconds = divmod(elapsed.days * 86400 + elapsed.seconds, 60)
    hours, minutes = divmod(m, 60)
    return '%.2d:%.2d:%.2d' % (hours, minutes, seconds)

def downloadfilesfromSftp():
    sftp = connect_sftp()


    for fi in sftp.listdir(config['remotepath']):
        if fi[0:4] not in ['crea', 'prod', 'inde', 'obj_', 'un_f', 'pers','conv','md_d']:
            print("Ignoring remote file {f}".format(f=fi))
            continue

        if str(fi).endswith('.tar') or str(fi).endswith('.dhctl') :
            print("Ignoring extension of file {f}".format(f=fi))
            continue

        if fi in local_file_list:
            lstat = os.stat(config['localpath'] + fi)
            ltime = datetime.fromtimestamp(lstat.st_mtime)
            lsize = lstat.st_size
            rstat = sftp.stat(config['remotepath'] + fi)
            rtime = datetime.fromtimestamp(rstat.st_mtime)
            rsize = rstat.st_size
            if (ltime == rtime and lsize == rsize) or ltime > rtime:
                print("Skipping existing file {f}, remote: {t1}, {s1}, local: {t2}, {s2}".format(f=fi,
                                                                                                t1=rtime,
                                                                                                s1=rsize,
                                                                                                t2=ltime,
                                                                                                s2=lsize))
                remote_file_list[fi] = False
                continue

        remote_file_list[fi] = True

    sftp.close()
    os.system('echo "{gpw}" >> ~/.pass'.format(gpw=credentials['gpg']))

    print('files in sftp: [' + str(len(remote_file_list)) + ']')
    print('\tDownloading files\n------------------------------')
    pool = Pool(initializer=initializer,
                processes=10)
    try:
        download = pool.map(download_from_sftp, [fl for fl in remote_file_list if remote_file_list[fl]])
    except KeyboardInterrupt:
        pool.terminate()
        pool.join()

    pool.close()
    pool.join()

    for fi in sorted([remote_file for remote_file in remote_file_list if remote_file_list[remote_file]], key=custom_sorter):
    #for fi in sorted([remote_file for remote_file in local_file_list], key=custom_sorter):
        if any(x in fi for x in ('create_table', 'create_personas', 'index', 'prod', 'obj', 'un', 'personas','conversion','md_dash')):
            filename = untar_files(config['localpath'], fi)
            print("Loading {f}".format(f=filename))
            try:
                db_insert(filename)

                if os.path.exists(config['localpath'] + filename) and 'create_' not in filename:
                    print("Removing {f}".format(f=filename))
                    os.remove(config['localpath'] + filename)

            except Exception as e:
                print(e)

            print('\tSuccess\n------------------------------')

def downloadlocalfiles():
    for fi in sorted([remote_file for remote_file in local_file_list], key=custom_sorter):
        if any(x in fi for x in ('create_table', 'create_personas', 'index', 'prod', 'obj', 'un', 'personas','conversion','md_dash_api')):
            filename = untar_files(config['localpath'], fi)
            print("Loading {f}".format(f=filename))
            try:
                db_insert(filename)

                if os.path.exists(config['localpath'] + filename) and 'create_' not in filename:
                    print("Removing {f}".format(f=filename))
                    os.remove(config['localpath'] + filename)

            except Exception as e:
                print(e)

            print('\tSuccess\n------------------------------')

print("========\n[{dt}] - Processed of ingestion on {cnn} connection \n========".format(
        dt=datetime.now(), cnn=conn_used))

remote_file_list = OrderedDict()
local_file_list = os.listdir(config['localpath'])


if os.name !='nt':
    downloadfilesfromSftp()
else:
    downloadlocalfiles()

try:

    print('\tCreation of script with indexes to be created in personas')
    ps_idx_query = """select sql
                      from (select distinct concat('create index ix_personas_', substr(column_name, 4),
                                                   ' on personas (',
                                                   column_name,
                                                   ') with (fillfactor = 100);') as sql,
                                            concat('ix_personas_', substr(column_name, 4)) as index_name
                            from information_schema.columns
                            where table_schema = 'public'
                            and table_name = 'un_filters'
                            and column_name like 'seg%') tb1
                      where index_name not in (select n.relname
                                               from pg_catalog.pg_class n
                                               inner join pg_catalog.pg_index i on (i.indexrelid = n.oid)
                                               inner join pg_catalog.pg_class t on (t.oid = i.indrelid)
                                               inner join pg_catalog.pg_namespace s on (s.oid = n.relnamespace)
                                               where t.relname = 'personas'
                                               and s.nspname = 'public')
                      order by index_name"""

    conn = psycopg2.connect(credentials['conn_string'])
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cursor.execute(ps_idx_query)
    creates_ps_idx = cursor.fetchall()

    index_count = 0
    total_indexes = len(creates_ps_idx)
    start_time = datetime.now()
    elapsed = 0

    for create in creates_ps_idx:
        index_count += 1

        if index_count >= 2:
            print("Executing {c} of {t}, elapsed: {e}, average: {a}, time left: {tl}".format(c=index_count,
                                                                                             t=total_indexes,
                                                                                             e=seconds_to_ftime(elapsed),
                                                                                             a=seconds_to_ftime(elapsed / (index_count - 1)),
                                                                                             tl=seconds_to_ftime((elapsed / (index_count - 1)) * (total_indexes - index_count + 1))))
        else:
            print("Executing {c} of {t}".format(c=index_count,
                                                t=total_indexes))

        print(create[0])
        cursor.execute(create[0])
        elapsed = datetime.now() - start_time
	
except Exception as e:
    print("Exception with personas indexes: {}".format(e))
	
try:

    print('Creation of script with indexes to be created in un_filters')
    un_idx_query = """select sql
                      from (select distinct concat('create index ix_un_filter_', substr(column_name, 4),
                                                   ' on un_filters (',
                                                   column_name,
                                                   ', loyalty_program_code, prsn_age_range_code, prsn_gender_code, prsn_price_sens_code, prsn_address_state_prov_code) with (fillfactor = 100);') as sql,
                                            concat('ix_un_filter_', substr(column_name, 4)) as index_name
                            from information_schema.columns
                            where table_schema = 'public'
                            and table_name = 'un_filters'
                            and column_name like 'un%') tb1
                      where index_name not in (select n.relname
                                               from pg_catalog.pg_class n
                                               inner join pg_catalog.pg_index i on (i.indexrelid = n.oid)
                                               inner join pg_catalog.pg_class t on (t.oid = i.indrelid)
                                               inner join pg_catalog.pg_namespace s on (s.oid = n.relnamespace)
                                               where t.relname = 'un_filters'
                                               and s.nspname = 'public')
                      order by index_name"""

    conn = psycopg2.connect(credentials['conn_string'])
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cursor.execute(un_idx_query)
    creates_un_idx = cursor.fetchall()

    index_count = 0
    total_indexes = len(creates_un_idx)
    start_time = datetime.now()
    elapsed = 0

    for create in creates_un_idx:
        index_count += 1

        if index_count >= 2:
            print("Executing {c} of {t}, elapsed: {e}, average: {a}, time left: {tl}".format(c=index_count,
                                                                                             t=total_indexes,
                                                                                             e=seconds_to_ftime(elapsed),
                                                                                             a=seconds_to_ftime(elapsed / (index_count - 1)),
                                                                                             tl=seconds_to_ftime((elapsed / (index_count - 1)) * (total_indexes - index_count + 1))))
        else:
            print("Executing {c} of {t}".format(c=index_count,
                                                t=total_indexes))

        print(create[0])
        cursor.execute(create[0])
        elapsed = datetime.now() - start_time

    print("vacuum analyze un_filters")
    cursor.execute("vacuum analyze un_filters")

    cursor.close()
    conn.close()

except Exception as e:
    print("Exception with un_filters indexes: {}".format(e))

