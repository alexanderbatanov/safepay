from flask import jsonify
from flask import render_template
from app import app
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra-public-ip'])

session = cluster.connect('p2p')
session.default_timeout = 240

@app.route('/')
@app.route('/index')
def index():
	return render_template("index.html")

@app.route('/api/tx/<int:cnt>')
def get_tx(cnt):

	if cnt == 0:
		response_list = ["Invalid input"]
		return render_template("tx_list.html", mylist = response_list)
	else:

		stmt_f = "SELECT from_party_id, blacklisted, reason FROM from_party_stats WHERE blacklisted = True LIMIT %s ALLOW FILTERING"
		response_f = session.execute(stmt_f, parameters=[cnt])

		stmt_t = "SELECT to_party_id, blacklisted, reason FROM to_party_stats WHERE blacklisted = True LIMIT %s ALLOW FILTERING"
		response_t = session.execute(stmt_t, parameters=[cnt])

		response_list =[]

		if not response_f:
			response_list.append("None found")
		else:
			for val in response_f:
			    response_list.append({"type":"Senders", "id": val[0], "reason":val[2]})
		
		if not response_t:
			response_list.append("None found")
		else:
			for val in response_t:
			    response_list.append({"type":"Receivers", "id": val[0], "reason":val[2]})

		return render_template("tx_list.html", mylist = response_list)


@app.route('/api/cnts')
def get_cnts():

	stmt = "SELECT id, cnt FROM ops_tx_cnt"

	response = session.execute(stmt)

	response_list = {}

	for row in response:
		response_list[row[0]] = row[1]

	return jsonify(results=response_list)

@app.route('/cnts')
def get_cnts_page():

	return render_template('cnts.html')





