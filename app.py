from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/transform", methods=["POST"])
def transform():
    data = request.json

    source_platform = data.get("source_platform")
    product = data.get("product")

    return jsonify({
        "status": "ok",
        "source": source_platform,
        "product_received": product
    })

@app.route("/", methods=["GET"])
def home():
    return "API Python attiva (product transformer)"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
