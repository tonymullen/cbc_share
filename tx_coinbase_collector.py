from dotenv import load_dotenv
from pathlib import Path
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from pymongo import MongoClient
from collections import Counter
import boto3
import botocore
from boto3.dynamodb.types import TypeSerializer
from dynamodb_json import json_util as json
import sys
import os

load_dotenv()

mongo_url = os.environ.get("MONGO_URL")
rpc_user = os.environ.get("RPC_USER")
rpc_password = os.environ.get("RPC_PASSWORD")

MAX_RECURSIVE_DEPTH = 1
MAX_CB_PER_TX = 100

client = MongoClient(mongo_url)
tx_block_lookup = client.btc.tx

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
tx_table = dynamodb.Table('Transactions')

rpc_con = AuthServiceProxy(
    "http://%s:%s@127.0.0.1:8332"%(rpc_user, rpc_password)
    )
 

def get_tx_from_id(txid):
    tx_raw = rpc_con.getrawtransaction(txid)
    return rpc_con.decoderawtransaction(tx_raw)


def get_block_at_height(h):
    return rpc_con.getblock(rpc_con.getblockhash(h))


def get_coinbase_blockheight(txid):
    # get from mongodb
    tx_block = tx_block_lookup.find_one({ '_id': txid })
    return tx_block['block_height']


def insert_to_db(txid, origins):
    # Order by nearnesss, then take top MAX_CB_PER_TX
    kept_origins =\
        dict(sorted(origins.items(), key=lambda x: x[1])[:MAX_CB_PER_TX])
    serializer = TypeSerializer()
    serialized = json.loads(json.dumps(kept_origins))
    response = tx_table.put_item(
       Item={
            'txid': txid,
            'coinbase_origins': serialized
        }
    )
    return response


def get_origins_from_db(txid):
    # get one transactions origins list
    # from dynamodb tx_table
    try:
        response = tx_table.get_item(Key={'txid': txid})
    except botocore.exceptions.ClientError as e:
        print(e.response['Error']['Message'])
    else:
        if 'Item' in response:
            return json.loads(response['Item']['coinbase_origins'])
        else:
            return None


# Recursive version
def collect_coinbase_origins_2(tx):
    """Launch recursive function with depth initialized as zero"""
    return collect_coinbase_origins_rec(tx, 0)


def d_merge(dict1, dict2):
    """Takes two dictionaries and returns the
    union of the two with the min value for any
    keys present in both dictionaries"""
    # When we get collections of coinbase origins
    # where a single cb origin has multiple paths
    # from the transaction, we will keep the value
    # representing the shortest number of hops
    in_both = dict1.keys() & dict2.keys()
    mins = {
        k: min(dict1[k], dict2[k])
        for k in in_both
        }
    dict1.update(dict2)
    dict1.update(mins)
    return dict1


def collect_coinbase_origins_rec(tx, r_depth):
    txid = tx['txid']
    origins = {}
    new_origins = {}
    from_db = {}

    if r_depth > MAX_RECURSIVE_DEPTH:
        return origins

    vins = tx['vin']
    # if len(vins) == 1:
    if 'coinbase' in vins[0]:
        print('Coinbase')
        block_height = get_coinbase_blockheight(txid)
        origins[block_height] = 0
        vins = vins[1:]

    for inp in vins:
        r_cbs = {}
        # For each input tx
        if not 'txid' in inp:
            print(vins)
            exit()
        from_db = get_origins_from_db(inp['txid'])
        if from_db is not None:
            if 'coinbase_origins' in from_db.keys():
                # Repair some previous damage
                # to database where the string "coinbase_origins"
                # got added rather than the correct value
                print("Whoops", from_db)
                r_cbs = {}
                insert_to_db(inp['txid'], r_cbs)
            else:
                # print(inp['txid'], "Retrieved from db")
                r_cbs = from_db
        else:
            tx_ = get_tx_from_id(inp["txid"])
            r_cbs = collect_coinbase_origins_rec(tx_, r_depth+1)
        # cbs derived from database or from recursive search
        # add one to depth and make sure the block no is still an int
        r_cbs = { int(r_cb): d+1
                for (r_cb, d)
                in r_cbs.items() }
        origins = d_merge(origins, r_cbs)
    if (origins and (not from_db or (origins.values() != from_db.values()))):
        insert_to_db(txid, origins)
        # print(txid, "=>", origins)
        # print('.', end='')
    return origins


def main(start_block, skip=1):
    block_record = Path('blocks_completed_'+str(os.getpid())+'.txt')
    block_record.touch(exist_ok=True)
    f = open(block_record, "r+")

    blockchain_height = rpc_con.getblockcount()
    print(blockchain_height)
    for h in range(start_block, blockchain_height+1, skip):
        block = get_block_at_height(h)
        c = 0
        if len(block['tx']) > 1:
            # for txid in block['tx'][1:]:
            for txid in block['tx']:
                # print("BLK", h, "PROCESSING TX", c, "OF", len(block['tx'][1:]))
                c = c+1
                tx = get_tx_from_id(txid)

                # Recursive function handles db insertion
                origins = collect_coinbase_origins_2(tx)

        f.seek(0)
        f.write(str(h)+'\n')
        f.truncate()

if __name__=="__main__":
    main(*[int(arg) for arg in sys.argv[1:]])


