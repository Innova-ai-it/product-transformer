import os
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
from transformer import convert_woocommerce_csv_path_to_shopify_csv
import pandas as pd
import io

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv", "txt"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

@app.route("/", methods=["GET"])
def home():
    return "API Python attiva (product transformer)"

@app.route("/convert_csv", methods=["POST"])
def convert_csv():
    """
    Endpoint: accetta multipart/form-data con campo 'file' (CSV WooCommerce)
    Restituisce: il CSV pronto per Shopify (file scaricabile)
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

    # output file path
    base, _ = os.path.splitext(filename)
    output_filename = f"{base}_shopify_ready.csv"
    output_path = os.path.join(app.config["UPLOAD_FOLDER"], output_filename)

    try:
        convert_woocommerce_csv_path_to_shopify_csv(input_path, output_path)
    except Exception as e:
        return jsonify({"error": "Conversion failed", "detail": str(e)}), 500

    # restituisci il file pronto per il download
    return send_file(output_path, as_attachment=True, download_name=output_filename)

if __name__ == "__main__":
    # porta dinamica da Railway (se presente)
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)