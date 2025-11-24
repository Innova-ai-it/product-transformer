import os
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
from transformer import convert_csv_to_shopify
import pandas as pd

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
CORS(app)   # <-- NECESSARIO per Make.com e Postman
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

@app.route("/", methods=["GET"])
def home():
    return "API Python attiva (product transformer)"

@app.route("/convert_csv", methods=["POST"])
def convert_csv():
    """
    Endpoint universale:
    - Accetta CSV (WooCommerce o Wix)
    - Riconosce automaticamente la piattaforma
    - Trasforma nel formato Shopify completo
    - Restituisce CSV scaricabile
    """
    if 'file' not in request.files:
        return jsonify({"error": "Missing file field 'file'"}), 400

    f = request.files['file']

    if f.filename == "":
        return jsonify({"error": "Empty filename"}), 400

    if not allowed_file(f.filename):
        return jsonify({"error": "File type not allowed. Upload a CSV."}), 400

    filename = secure_filename(f.filename)
    input_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    f.save(input_path)

    base, _ = os.path.splitext(filename)
    output_filename = f"{base}_shopify_ready.csv"
    output_path = os.path.join(app.config["UPLOAD_FOLDER"], output_filename)

    try:
        platform_detected = convert_csv_to_shopify(input_path, output_path)
    except Exception as e:
        return jsonify({
            "error": "Conversion failed",
            "detail": str(e)
        }), 500

    return jsonify({
        "status": "ok",
        "platform_detected": platform_detected,
        "download_url": f"/download/{output_filename}"
    })

@app.route("/download/<path:filename>", methods=["GET"])
def download_transformed_file(filename):
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(file_path, as_attachment=True, download_name=filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
