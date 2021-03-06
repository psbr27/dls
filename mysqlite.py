
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
    u = u.values(Name=sName, Channel=ch, updated_on=timestamp)
    result = conn.execute(u)



def select_data(conn, tbl):
    s = select([tbl])
    rp = conn.execute(s)
    results = rp.fetchall()
    return results

#____________________________________________________________________________
"""
if __name__ == '__main__':
    conn, tbl = create_table()
    for i in range(1,10):
        insert_data(conn, tbl, i, 'NA', 0, 0)
    update_data(tbl, conn)
        for ii in range(0,10):
        row = results[ii]
        print(row[5])
"""
