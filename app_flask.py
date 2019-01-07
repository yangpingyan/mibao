# coding:utf8
import os
import time
from argparse import ArgumentParser
from flask import Flask, jsonify
from flask import make_response
import pandas as pd
import json
import warnings
from gevent.pywsgi import WSGIServer



app = Flask(__name__)

# start with order_id 105914
@app.route('/crawl/<string:company_name>', methods=['GET'])
def get_predict_result(company_name):
    print(company_name)
    return jsonify({"code": 200, "data": {"result": company_name}, "message": "SUCCESS"}), 200




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

