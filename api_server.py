#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from flask import Flask
import flask_restful as restful
from twcom import query
from flask_restful import reqparse
from flask import json


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
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSON_AS_ASCII'] = False
api = restful.Api(app)

compars = reqparse.RequestParser()
compars.add_argument('id', type=unicode)
compars.add_argument('boss', type=unicode)
compars.add_argument('target', type=unicode)
compars.add_argument('comboss', type=unicode)
compars.add_argument('comaddr', type=unicode)
compars.add_argument('maxlvl', type=int, default=1)
compars.add_argument('lineunit', type=unicode)

bospars = reqparse.RequestParser()
bospars.add_argument('name', type=unicode)
bospars.add_argument('bossid', type=unicode)

qrypars = reqparse.RequestParser()
qrypars.add_argument('boss', type=unicode)
qrypars.add_argument('com', type=unicode)
qrypars.add_argument('board', type=unicode)

rankpars = reqparse.RequestParser()
rankpars.add_argument('data', type=unicode)
rankpars.add_argument('rankby', type=unicode)
rankpars.add_argument('n', type=int, default=10)


class ComNetwork(restful.Resource):
    def get(self):
        args = compars.parse_args()
        maxlvl = min(args['maxlvl'], 3)
        lnunit = args['lineunit']
        if (lnunit != 'seat') and (lnunit != 'seatratio'):
            lnunit = 'seat'

        if args.get('id'):
            G = query.get_network(
                args['id'],
                maxlvl=maxlvl,
                lnunit=lnunit)
        elif (args.get('boss') or args.get('target')):
            G = query.get_network_boss(
                name=args.get('boss'),
                target=args.get('target'),
                maxlvl=maxlvl,
                lnunit=lnunit)
        elif args.get('comboss'):
            G = query.get_network_comboss(
                args.get('comboss'),
                maxlvl=maxlvl,
                lnunit=lnunit)
        elif args.get('comaddr'):
            G = query.get_network_comaddr(
                args.get('comaddr'),
                maxlvl=maxlvl,
                lnunit=lnunit)
        else:
            print args
            raise Exception('Unknown parameter')

        return json.jsonify(query.exp_company(G))


class BossNetwork(restful.Resource):
    def get(self):
        args = bospars.parse_args()
        G = query.get_boss_network(target=args['bossid'], maxlvl=1)
        return json.jsonify(query.exp_boss(G))


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
            dic = query.get_boss(
                args.get('board'),
                ind=True)
        else:
            print args
            raise Exception('Unknown parameter')

        return json.jsonify(dic)


class Rank(restful.Resource):
    def get(self):
        args = rankpars.parse_args()
        nlim = min(args['n'], 2000)
        return json.jsonify(query.getRanking(
            args['data'], args['rankby'], nlim))


class Root(restful.Resource):
    def get(self):
        return 'ok'


api.add_resource(ComNetwork, '/com')
api.add_resource(BossNetwork, '/boss')
api.add_resource(Query, '/query')
api.add_resource(Rank, '/rank')
api.add_resource(Root, '/')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000, debug=True)
