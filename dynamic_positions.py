import asyncio
import json
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from qp.platform import Platform  # requires python 3.10
from pyexos.ts.sql import Account

import json
from datetime import date

qp_config = {
    'ts.positions': {
        'class': 'qp.ts.positions#PositionsFactory',
        "environment": "poc"
    },
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


async def generate_list_of_deal_identifiers(config: dict) -> list[str]:
    platform_ts = await Platform.from_dict(config)
    engine_ts: Engine = await platform_ts.ts.positions(account="2874721a-acac-4aba-98f7-fdbcecf7441a",
                                                       date=date.today())  # this is the GNMA1_C account
    ts_list = []
    for ts in engine_ts:
        ts_list.append(ts.imnt)

    return ts_list


async def generate_deal_from_qp(config: dict, identifier: str) -> dict:
    platform_security = await Platform.from_dict(config)
    engine_security: Engine = await platform_security.ts.security(imnt=identifier)
    values_json = json.loads(engine_security.origin_ref_data["full_json_str"])
    return values_json

# there's slight differences in the draw values which is why if you compare this to the original it's off
__center_city = asyncio.run(generate_deal_from_qp(qp_config, "b1b02263-b0ca-468d-b015-5b08209278a8"))

identifier_list = asyncio.run(generate_list_of_deal_identifiers(qp_config))

deal_list = []
for identity in identifier_list:
    deal = asyncio.run(generate_deal_from_qp(qp_config, identity))
    deal_list.append(deal)

print(deal_list)
