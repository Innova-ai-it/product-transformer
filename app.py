import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from transformer import transform_product

app = Flask(__name__)

# Abilita CORS per tutte le route
CORS(app)

@app.route("/transform", methods=["POST"])
def transform():
    data = request.json
    source_platform = data.get("source_platform")
    product = data.get("product")
    transformed = transform_product(source_platform, product)
    return jsonify({
        "status": "ok",
        "source": source_platform,
        "transformed_product": transformed
    })

@app.route("/", methods=["GET"])
def home():
    return "API Python attiva (product transformer)"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
