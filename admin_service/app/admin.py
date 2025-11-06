# app/admin.py

from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_cors import CORS
from app.models import create_table, add_topic, get_all_topics, delete_topic

app = Flask(__name__, template_folder="templates")
CORS(app)

# Initialize the database
create_table()

@app.route("/")
def index():
    topics = get_all_topics()
    return render_template("dashboard.html", topics=topics)

@app.route("/add", methods=["POST"])
def add():
    name = request.form.get("name")
    desc = request.form.get("description")
    if name:
        add_topic(name, desc or "")
    return redirect(url_for("index"))

@app.route("/delete/<int:topic_id>", methods=["POST"])
def delete(topic_id):
    delete_topic(topic_id)
    return redirect(url_for("index"))

# REST API for programmatic access
@app.route("/api/topics", methods=["GET"])
def api_list_topics():
    return jsonify(get_all_topics())

@app.route("/api/topics", methods=["POST"])
def api_add_topic():
    data = request.get_json()
    add_topic(data["name"], data.get("description", ""))
    return jsonify({"status": "success"}), 201

@app.route("/api/topics/<int:topic_id>", methods=["DELETE"])
def api_delete_topic(topic_id):
    delete_topic(topic_id)
    return jsonify({"status": "deleted"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
    
    
    
    
    
