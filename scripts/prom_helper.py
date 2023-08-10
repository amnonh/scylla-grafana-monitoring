#!/usr/bin/env python3

import argparse
import datetime
import requests
import urllib.parse
import json
import awswrangler  as wr
import pandas  as pd
import os

dry_run = False
verbose = False
DATABASE_NAME="housekeeping"
TABLE_NAME="clustersinfo"
BUCKET=os.getenv("BUCKET")
S3KEY_RAW_DATASET = os.getenv("S3KEY_RAW_DATASET")
S3KEY_TRUSTED_DATASET = os.getenv("S3KEY_TRUSTED_DATASET")
ATHENA_DB = os.getenv("ATHENA_DB", "default")
ATHENA_TABLE = os.getenv("ATHENA_TABLE")
SG_LABELS = os.getenv("ATHENA_TABLE")

METRICS = [
    "scylla_manager_repair_progress",
    "scylla_manager_backup_progress",
    "scylla_coordinator_read_count",
    "scylla_total_requests",
    "scylla_coordinator_write_count",
    "scylla_ag_cache_row_hits",
    "scylla_ag_cache_row_misses",
    "scylla_node_filesystem_avail_bytes",
    "scylla_node_filesystem_total_avail_bytes",
    "scylla_node_filesystem_size_bytes",
    "scylla_node_filesystem_total_size_bytes",
    "scylla_ag_cache_bytes_used",
    "node_network_receive_packets",
    "node_network_transmit_packets",
    "node_network_receive_bytes",
    "node_network_transmit_bytes",
    "scylla_total_connection",
    "scylla_total_nodes",
    "scylla_total_unreachable_nodes",
    "scylla_total_joining_nodes",
    "scylla_total_leaving_nodes",
    "scylla_total_manager_tasks",
    "scylla_total_compactios",
    "scylla_storage_proxy_coordinator_background_writes_ag",
    "scylla_hints_manager_written_ag",
    "scylla_hints_manager_sent_ag",
]

def make_query(str, obj = {}):
    obj['DATABASE_NAME'] = DATABASE_NAME
    obj['TABLE_NAME'] = TABLE_NAME
    return str.format(**obj)
def get_int(str):
    try:
        return int(str)
    except:
        return None

def get_double(str):
    try:
        return float(str)
    except:
        return None

def trace_verbose(*arg):
    if verbose:
        print(*arg)


def get(url, params):
    trace_verbose(url)
    response = requests.get(url, params=params)
    return response.content

def parse(str):
    if str.startswith("#"):
        return None
    parts = str.split('{')
    res = {"name": parts[0]}
    parts = parts[1].split('}')
    res["labels"] = parts[0]
    for l in parts[0].split(','):
        la = l.split('=')
        label = la[0].replace('"','')
        res[label] = la[1].replace('"','')
    parts = parts[1].split(' ')
    res['__value'] = get_double(parts[1])
    res['__time'] = get_int(parts[2])
    return res

def print_federate_res(args, res):
    if args.coloumns:
        for c in args.coloumns:
            print(",".join([res[c] for c in args.coloumns if c in res]))
    else:
        print(res) 

# def get_connection(args):
#     host = 'scylla-downloads-v2.cluster-cmibwi2oeyz8.us-west-2.rds.amazonaws.com'
#     user = args.username
#     password = args.password
#     database = args.database
#     if verbose:
#         print("connect", host, user, password, database)
#     return pymysql.connect(host=host, user=user, password=password, database=database)

def store(args, res):
    host = 'admin.cjp8hsqu4je0.us-east-2.rds.amazonaws.com'
    user = 'admin'
    password = '12345678'
    database = 'admin'
    
    connection = pymysql.connect(host, user, password)
    with connection:
        cur = connection.cursor()
        cur.execute("SELECT VERSION()")
        version = cur.fetchone()
    print("Database version: {} ".format(version[0]))

def create_filter(filter):
    parts = filter.split('=')
    filter_parts = parts[1].split(',')
    return {'label' : parts[0], 'cmd' : filter_parts[0], 'val' : filter_parts[1] if len(filter_parts) > 1 else None}

def create_filters(args):
    if args.filter:
        return [create_filter(f) for f in args.filter]
    return []

def should_filter(filters, line):
    for f in filters:
        if f['label'] not in line: 
            if f['cmd'] in ['eq']:
                return True
        else:
            if f['cmd'] == 'drop':
                return True
            
            if f['cmd'] in ['eq', 'miseq'] and f['val'] != line[f['label']]:
                return True 
    return False

def s3_insert(args, record):
    result = []
    for c in records:
        result.append(records[c])
    
    df = pd.DataFrame(result)
    df['__time'] = df['__time'] / 1000
    df['partition'] = datetime.datetime.now().strftime('%Y%m%d')

    wr.s3.to_parquet(path='s3://scylla-cloud-reports-297607762119-eu-central-1/test/cluster_metrics', database='default', table='test_metrics', partition_cols=['partition'], df=df, dataset=True)

    df['partition'] = 'latest'
    wr.s3.to_parquet(path='s3://scylla-cloud-reports-297607762119-eu-central-1/test/cluster_metrics', database='default', table='test_metrics_latest', df=df, dataset=True)

def print_insert(args, records):
    for c in records:
        metrics = records[c]
        print(metrics)


def insert(args, record):
    if args.insert == 'print':
        print_insert(args, record)
    elif args.insert == 'rds':
        rds_insert(args, record)
    elif args.insert == 's3':
        s3_insert(args, record)

def get_table(args):
    connection = get_connection(args)
    results = []
    with connection:
        with connection.cursor() as cursor:
            # Read a single record
            sql = make_query("SELECT * from {DATABASE_NAME}.{TABLE_NAME}")
            if verbose:
                print(sql)
            cursor.execute(sql)
            for r in cursor.fetchall():
                obj = {"cluster_id": r[0], 'time' : str(r[1])}
                for idx, x in enumerate(METRICS):
                    obj[x] = r[idx + 2]
                results.append(obj)
    if args.limit:
        print(json.dumps(results[args.limit:], indent=2))
    else:
        print(json.dumps(results, indent=2))
def get_name(p, args):
    suf = "_".join([p[k[0]] for k in args.labels if k[0] in p and (len(k) < 2 or k[1] in p['name'])])
    return p['name'] + '_' + suf if suf else p['name'] 

def read_federate(args):
    url = "http://" + args.host + ":9090/federate"
    par = ",".join(args.match)
    param = urllib.parse.quote("{" + par + "}")
    url = url + "?match[]=" + param
    lines = get(url, {}).splitlines()
    res = [parse(l.decode('ascii')) for l in lines]
    results = {}
    filters = create_filters(args)
    for p in res:
        if not p:
            continue
        if should_filter(filters, p):
            continue 
        if 'cluster' in p:
            c = int(p['cluster'][1:])
            if c not in results:
                results[c] = {'cluster_id':c}
            n = get_name(p, args)
            if n in results[c]:
                results[c][n] += p['__value']
            else:
                results[c][n] = p['__value']
            time = p['__time']
            if time and '__time' not in results[c]:
                results[c]['__time'] = time
        if verbose:
            print_federate_res(args, p)
    insert(args, results)  
    trace_verbose("total results", len(results))
    
    
def update_args(args):
    if not args.host or args.host == "":
        try:
            args.__dict__['host'] = run('./scripts/find_ip.sh aprom').strip(' \t\n\r')
        except:
            print("find_ip not found")
    args.__dict__['labels'] = [ k.split(',') for k in args.labels]
    return args

parser = argparse.ArgumentParser(description='Prometheus helper tool', conflict_handler="resolve")
parser.add_argument('-H', '--host', default="127.0.0.1", help='A Prometheus server to connect to')
parser.add_argument('-V', '--verbose', default=False, help="when set, run in verbose mode", action='store_true')
parser.add_argument('-U', '--username', help="Database User name")
parser.add_argument('-P', '--password', help="Database User name")
parser.add_argument('-D', '--database', help="Database User name")
parser.add_argument('-M', '--match', action='append', help="Matchers to look for. Use key=value, for example 'by=\"cluster\"'", default=['by="cluster"'])
parser.add_argument('-F', '--filter', action='append', help="Filter the results based on label, use label=cmd,val. cmp=[eq - equal label must exists. miseq - label is missing, or is equal. drop - drop if label exists]")
parser.add_argument('-C', '--coloumns', action='append', help="Show only a few coloumn")
parser.add_argument('-L', '--labels', action='append', default = ['scheduling_group_name,latency'], help="if those label exists add them to the name")
parser.add_argument('-I', '--insert', default="print", choices=['print', 'rds', 's3'], help="how to insert the data")

# parser_rds = subparsers.add_parser('rds', help='perform an RDS command')
# parser_rds.add_argument('-l', '--limit', type=int, help="limit the number of printed results")
# parser_rds.add_argument('command', help='the rds command to perform, create - create a table, drop - drop the table, get - read from the table, truncate - truncate a table')
# parser_rds.set_defaults(func=do_rds)
args = parser.parse_args()
if args.verbose:
    verbose = True
update_args(args)
read_federate(args)

