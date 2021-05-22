from flask import Flask, jsonify

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect
import datetime as dt

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
session = Session(engine)
Measurement = Base.classes['measurement']
Station = Base.classes['station']

app = Flask(__name__)

@app.route("/")
def home():
    result = '''
/api/v1.0/precipitation
/api/v1.0/stations
/api/v1.0/tobs
/api/v1.0/<start>
/api/v1.0/<start>/<end>
'''
    return result

@app.route("/api/v1.0/precipitation")
def precipitation():
    data = engine.execute("SELECT date, prcp FROM measurement")
    data_dict = dict()
    for record in data:
        data_dict[record[0]] = record[1]
    print(data_dict)
    return jsonify(data_dict)

@app.route("/api/v1.0/stations")
def stations():
    data = engine.execute("SELECT * FROM station")
    data_ls = list()
    for record in data:
        data_ls.append(list(record))
    print(data_ls)
    return jsonify(data_ls)

@app.route("/api/v1.0/tobs")
def tobs():
    ss = session.query(func.max(Measurement.date)).all()
    last_date = ss[0][0]

    start_date = (dt.datetime.strptime(last_date, "%Y-%m-%d") \
                  - dt.timedelta(days=365)).strftime("%Y-%m-%d")

    query_stn_rank = session\
            .query(Measurement.station, func.count(Measurement.id))\
            .group_by(Measurement.station)\
            .order_by(func.count(Measurement.id).desc())\
            .all()
    query_stn_rank
    active_no1_station = query_stn_rank[0][0]
    query_active_no1_station = session\
            .query(Measurement.date, Measurement.prcp)\
            .filter((Measurement.station == active_no1_station) & (Measurement.date > start_date))\
            .all()
    query_active_no1_station = [list(i) for i in query_active_no1_station]
    return jsonify(query_active_no1_station)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end=None):
    if end is None:
        ss = session.query(func.max(Measurement.date)).all()
        end = ss[0][0]
        
    cols = [func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)]
    query_active_no1_station = session\
            .query(*cols)\
            .filter((Measurement.date >= start) & (Measurement.date <= end))\
            .all()
    query_active_no1_station = [list(i) for i in query_active_no1_station]
    return jsonify(query_active_no1_station)

if __name__ == "__main__":
    app.run(debug=True)