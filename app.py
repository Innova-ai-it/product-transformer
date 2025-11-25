import os
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
from transformer import convert_csv_path_to_shopify_csv
import traceback

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    """Verifica che il file sia un CSV"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
CORS(app)  # Necessario per Make.com e chiamate cross-origin
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB max file size

@app.route("/", methods=["GET"])
def home():
    """Health check endpoint"""
    return jsonify({
        "service": "Product CSV Transformer API",
        "version": "2.0",
        "status": "online",
        "supported_platforms": ["WooCommerce", "Wix", "PrestaShop (coming soon)"],
        "endpoints": {
            "convert": "/convert_csv (POST)",
            "download": "/download/<filename> (GET)"
        }
    })

@app.route("/health", methods=["GET"])
def health():
    """Railway health check"""
    return jsonify({"status": "healthy"}), 200

@app.route("/convert_csv", methods=["POST"])
def convert_csv():
    """
    Endpoint principale per conversione CSV → Shopify
    
    Accetta: multipart/form-data con campo 'file' (CSV)
    Ritorna: JSON con info sul file convertito + download URL
    
    Riconosce automaticamente:
    - WooCommerce (colonne: Type, Attribute 1 name, Images)
    - Wix (colonne: media, product_media)
    """
    
    # Validazione file
    if 'file' not in request.files:
        return jsonify({
            "error": "Missing file",
            "detail": "No 'file' field in request. Use multipart/form-data with a 'file' field."
        }), 400

    file = request.files['file']

    if file.filename == "":
        return jsonify({
            "error": "Empty filename",
            "detail": "Uploaded file has no name"
        }), 400

    if not allowed_file(file.filename):
        return jsonify({
            "error": "Invalid file type",
            "detail": "Only CSV files are allowed"
        }), 400

    # Salvataggio file di input
    filename = secure_filename(file.filename)
    input_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    
    try:
        file.save(input_path)
    except Exception as e:
        return jsonify({
            "error": "File save failed",
            "detail": str(e)
        }), 500

    # Preparazione output
    base, _ = os.path.splitext(filename)
    output_filename = f"{base}_shopify.csv"
    output_path = os.path.join(app.config["UPLOAD_FOLDER"], output_filename)

    # Conversione
    try:
        result_path = convert_csv_path_to_shopify_csv(input_path, output_path)
        
        # Verifica che il file output esista
        if not os.path.exists(result_path):
            raise FileNotFoundError(f"Output file not created: {result_path}")
        
        # Lettura info sul file generato
        file_size = os.path.getsize(result_path)
        
        return jsonify({
            "status": "success",
            "message": "CSV converted successfully",
            "input_file": filename,
            "output_file": output_filename,
            "output_size_bytes": file_size,
            "download_url": f"/download/{output_filename}"
        }), 200
        
    except Exception as e:
        # Log completo dell'errore
        error_trace = traceback.format_exc()
        print(f"[ERROR] Conversion failed:\n{error_trace}")
        
        return jsonify({
            "error": "Conversion failed",
            "detail": str(e),
            "trace": error_trace if app.debug else "Enable debug mode for full trace"
        }), 500
    
    finally:
        # Cleanup del file di input (opzionale)
        # Puoi commentare questa parte se vuoi mantenere i file caricati
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
        except:
            pass

@app.route("/download/<path:filename>", methods=["GET"])
def download_transformed_file(filename):
    """
    Download del file CSV trasformato
    """
    # Sanitizzazione nome file
    filename = secure_filename(filename)
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    
    if not os.path.exists(file_path):
        return jsonify({
            "error": "File not found",
            "detail": f"File '{filename}' does not exist or has been deleted"
        }), 404

    try:
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
    except Exception as e:
        return jsonify({
            "error": "Download failed",
            "detail": str(e)
        }), 500

@app.errorhandler(413)
def file_too_large(e):
    """Gestione file troppo grandi"""
    return jsonify({
        "error": "File too large",
        "detail": "Maximum file size is 50MB"
    }), 413

@app.errorhandler(500)
def internal_error(e):
    """Gestione errori generici"""
    return jsonify({
        "error": "Internal server error",
        "detail": str(e)
    }), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    debug_mode = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    
    print(f"🚀 Starting Product Transformer API on port {port}")
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print(f"🔧 Debug mode: {debug_mode}")
    
    app.run(host="0.0.0.0", port=port, debug=debug_mode)