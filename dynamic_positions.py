import asyncio
import json
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from qp.platform import Platform  # requires python 3.10
from pyexos.ts.sql import Account

import json
from datetime import date, timedelta


async def generate_list_of_deal_identifiers():
    ts_config = {
        'ts.positions': {
            'class': 'qp.ts.positions#PositionsFactory',
            "environment": "poc"
        },
        'database': {
            'class': 'qp.database#PlainTextDatabaseFactory',
            'credentials': {
                'poc': {
                    'username': 'postgres',
                    'password': 'devpgadmin',
                }
            },
        },
        'log': {
            'class': 'qp.log#LogStreamFactory',
            'streams': {
                'composite': {
                    'class': 'qp.log#LogStream',
                },
            },
            'backends': {},
            'mapping': {},
        },
    }
    platform_ts = await Platform.from_dict(ts_config)
    engine_ts: Engine = await platform_ts.ts.positions(account="2874721a-acac-4aba-98f7-fdbcecf7441a",
                                                       date=date.today())  # this is the GNMA1_C account
    ts_list = []
    for ts in engine_ts:
        ts_list.append(ts.imnt)

    return ts_list


async def generate_deal_from_qp(identifier: str) -> dict:
    security_config = {
        'ts.security': {
            'class': 'qp.ts.security#SecurityFactory',
            "environment": "poc"
        },
        'database': {
            'class': 'qp.database#PlainTextDatabaseFactory',
            'credentials': {
                'poc': {
                    'username': 'postgres',
                    'password': 'devpgadmin',
                }
            },
        },
        'log': {
            'class': 'qp.log#LogStreamFactory',
            'streams': {
                'composite': {
                    'class': 'qp.log#LogStream',
                },
            },
            'backends': {},
            'mapping': {},
        },
    }

    platform_security = await Platform.from_dict(security_config)
    engine_security: Engine = await platform_security.ts.security(imnt=identifier)
    values_json = json.loads(engine_security.origin_ref_data["full_json_str"])
    identifier_json = {}
    identifier_json["identifier"] = engine_security.exos_id
    identifier_json["description"] = engine_security.origin_ref_data["name"]
    identifier_json["clc"] = values_json["clc"]
    identifier_json["draw_price"] = values_json["draw_price"]
    identifier_json["draw_schedule"] = values_json["draw_schedule"]

    return identifier_json


# there's slight differences in the draw values which is why if you compare this to the original it's off
__center_city = asyncio.run(generate_deal_from_qp("b1b02263-b0ca-468d-b015-5b08209278a8"))

identifier_list = asyncio.run(generate_list_of_deal_identifiers())
print(identifier_list)

deal_list = []
for identity in identifier_list:
    deal = asyncio.run(generate_deal_from_qp(identity))
    deal_list.append(deal)

print(deal_list)
