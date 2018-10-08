#!/usr/bin/python3

from datetime import datetime

from sqlalchemy import (MetaData, Table, Column, Integer, Numeric, String,
                        DateTime, ForeignKey, create_engine)

from sqlalchemy import update
from sqlalchemy.sql import select

metadata = MetaData()


#____________________________________________________________________________
def create_table():
    tbl = Table('sensordata', metadata,
            Column('ID', Integer(), primary_key=True),
            Column('Name', String(20), nullable=False),
            Column('Channel', Integer()),
            Column('dpaId', Integer()),
            Column('created_on', DateTime(), default=datetime.now),
            Column('updated_on', DateTime(), default=datetime.now)
            )

    engine = create_engine('sqlite:///:memory')
    metadata.create_all(engine)
    conn = engine.connect()
    return conn, tbl




#____________________________________________________________________________

def insert_data(conn, tbl, sId, sName, channel_id, dpaId, timestamp):
    ins = tbl.insert().values(
            ID= sId,
            Name= sName,
            Channel= channel_id,
            dpaId=dpaId,
            created_on=datetime.now(),
            updated_on=timestamp
            )
    ins.compile().params
    result = conn.execute(ins)



#____________________________________________________________________________
def update_data(conn, tbl, sId, sName, ch, timestamp):
    u = update(tbl).where(tbl.c.ID == sId)
    u = u.values(Name=sName, Channel=ch, dpaId=0, created_on=timestamp, updated_on=timestamp)
    result = conn.execute(u)



#____________________________________________________________________________
def reset_table():
    conn, tbl = create_table()
    for ii in range(0,10):
        update_data(conn, tbl, ii, "NA", 0, datetime.now())

if __name__ == '__main__':
    reset_table()
    print("done")
