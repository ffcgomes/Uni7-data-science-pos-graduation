from flask import Flask
from flask import jsonify
from produtos_store import ProdutoStore
import json

app = Flask(__name__)

redis = ProdutoStore('192.168.99.100', '6379', 0)

# O(1)
@app.route('/v1/recommendation/products/<id>')
def get_recommendations(id):
    result = redis.get_recommendations(id)

    return jsonify(key=id, result=result)

if __name__ == "__main__":
    app.run()