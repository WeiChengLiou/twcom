#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from flask import Flask
from flask.ext import restful
from twcom import query
from flask.ext.restful import reqparse
import json
from pdb import set_trace


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
compars.add_argument('id', type=unicode)
compars.add_argument('boss', type=unicode)
compars.add_argument('target', type=unicode)
compars.add_argument('comboss', type=unicode)
compars.add_argument('comaddr', type=unicode)
compars.add_argument('maxlvl', type=int, default=1)

bospars = reqparse.RequestParser()
bospars.add_argument('name', type=unicode)
bospars.add_argument('id', type=unicode)


qrypars = reqparse.RequestParser()
qrypars.add_argument('boss', type=unicode)
qrypars.add_argument('com', type=unicode)
qrypars.add_argument('board', type=unicode)


class ComNetwork(restful.Resource):
    def get(self):
        args = compars.parse_args()
        args['maxlvl'] = min(args['maxlvl'], 3)
        if args.get('id'):
            G = query.get_network(
                args['id'],
                maxlvl=args['maxlvl'])
        elif args.get('boss'):
            G = query.get_network_boss(
                args.get('boss'),
                target=args.get('target'),
                maxlvl=args['maxlvl'])
        elif args.get('comboss'):
            G = query.get_network_comboss(
                args.get('comboss'),
                maxlvl=args['maxlvl'])
        elif args.get('comaddr'):
            G = query.get_network_comaddr(
                args.get('comaddr'),
                maxlvl=args['maxlvl'])

        return query.exp_company(G)


class BossNetwork(restful.Resource):
    def get(self):
        args = bospars.parse_args()
        names = list(query.getcomboss(args['id']))
        G = query.get_boss_network(names, maxlvl=1)
        return query.exp_boss(G)


class Query(restful.Resource):
    def get(self):
        args = qrypars.parse_args()
        if args.get('boss'):
            dic = query.queryboss(args.get('boss'))
        elif args.get('com'):
            dic = (
                {r['id']: r['name']
                 for r in query.getidlike(args.get('com'))})
        elif args.get('board'):
            dic = tuple(query.get_boss(args.get('board'),
                        ind=True, no_id=True))

        return json.dumps(dic)


class Root(restful.Resource):
    def get(self):
        return 'ok'


api.add_resource(ComNetwork, '/com')
api.add_resource(BossNetwork, '/boss')
api.add_resource(Query, '/query')
api.add_resource(Root, '/')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, debug=True)
