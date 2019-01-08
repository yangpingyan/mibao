# coding:utf8
import os
import time
from argparse import ArgumentParser
from flask import Flask, jsonify, request
from flask import make_response
import pandas as pd
import json
import warnings
from gevent.pywsgi import WSGIServer
from sql.sql import sql_connect
from spider_tianyancha import SpiderTianyangcha


#
# try:
#     workdir = os.path.dirname(os.path.realpath(__file__))
# except:
#     workdir = os.getcwd()
#
# sql_file = os.path.join(workdir, 'sql', 'sql_mibao_spider.json')
# ssh_pkey = os.path.join(workdir, 'sql', 'sql_pkey')
# conn = sql_connect('enterprise', sql_file, ssh_pkey)
spider = SpiderTianyangcha()

app = Flask(__name__)

# /query?name=111
@app.route('/query')
def get_predict_result():
    company_name = request.args.get("name")
    print(company_name)
    # df = pd.read_sql('''SELECT * FROM `tianyancha` WHERE company_name = '{}';'''.format(company_name), conn)
    df = spider.crawl_company(company_name)
    df = df.drop(columns=['id', 'id_51job', 'id_lagou', 'create_time', 'id_related', 'from_table'], errors='ignore')
    company_dict = {}
    if len(df) > 0:
        for col in df.columns:
            company_dict[col] = df.at[0, col]
    print(company_dict)
    return jsonify({"code": 200, "data": {"result": company_dict}, "message": "SUCCESS"}), 200
    # return json.dumps({"code": 200, "data": {"result": company_dict}, "message": "SUCCESS"})





@app.errorhandler(404)
def not_found():
    return make_response(jsonify({'error': 'bug'}), 404)


if __name__ == '__main__':
    opt = ArgumentParser()
    opt.add_argument('--model', default='gevent')
    args = opt.parse_args()

    if args.model == 'gevent':
        http_server = WSGIServer(('0.0.0.0', 5000), app)
        print('listen on 0.0.0.0:5000')
        http_server.serve_forever()
    elif args.model == 'raw':
        app.run(host='0.0.0.0')

