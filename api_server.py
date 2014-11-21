#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from flask import Flask
from flask.ext import restful
from twcom import query
from flask.ext.restful import reqparse

def setlogger():
    logger = logging.getLogger('twcom')
    # Produce formater first
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Setup Handler
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)

    # Setup File Handler
    flog = logging.FileHandler('twcom.log', 'w')
    flog.setLevel(logging.INFO)
    flog.setFormatter(formatter)

    # Setup Logger
    # logger.addHandler(console)
    logger.addHandler(flog)
    logger.setLevel(logging.INFO)


setlogger()
app = Flask(__name__)
api = restful.Api(app)
compars = reqparse.RequestParser()
compars.add_argument('id', type=str)
bospars = reqparse.RequestParser()
bospars.add_argument('name', type=str)
bospars.add_argument('id', type=str)


class ComNetwork(restful.Resource):
    def get(self):
        args = compars.parse_args()
        G = query.get_network(args['id'], maxlvl=1)
        return query.exp_company(G)


class BossNetwork(restful.Resource):
    def get(self):
        args = bospars.parse_args()
        names = list(query.getcomboss(args['id']))
        G = query.get_boss_network(names, maxlvl=1)
        return query.exp_boss(G)


class Root(restful.Resource):
    def get(self):
        return 'ok'


api.add_resource(ComNetwork, '/com')
api.add_resource(BossNetwork, '/boss')
api.add_resource(Root, '/')


if __name__ == '__main__':
#app.run(debug=True)
    app.run(host='0.0.0.0', port=5000)
