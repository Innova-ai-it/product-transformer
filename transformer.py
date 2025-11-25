import pandas as pd
import re
import unicodedata
from itertools import product
from typing import List, Dict, Tuple

# -------------------------
# SHOPIFY CSV COLUMNS
# -------------------------
SHOPIFY_COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags",
    "Published", "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value",
    "Option3 Name", "Option3 Value", "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
    "Variant Inventory Qty", "Variant Inventory Policy", "Variant Fulfillment Service",
    "Variant Price", "Variant Compare At Price", "Variant Requires Shipping", "Variant Taxable",
    "Variant Barcode", "Image Src", "Image Position", "Image Alt Text", "Gift Card",
    "SEO Title", "SEO Description", "Google Shopping / Google Product Category",
    "Google Shopping / Gender", "Google Shopping / Age Group", "Google Shopping / MPN",
    "Google Shopping / AdWords Grouping", "Google Shopping / AdWords Labels",
    "Google Shopping / Condition", "Google Shopping / Custom Product",
    "Google Shopping / Custom Label 0", "Google Shopping / Custom Label 1",
    "Google Shopping / Custom Label 2", "Google Shopping / Custom Label 3",
    "Google Shopping / Custom Label 4", "Variant Image", "Variant Weight Unit",
    "Variant Tax Code", "Cost per item", "Status"
]

# -------------------------
# UTILITY FUNCTIONS
# -------------------------
def slugify(value: str) -> str:
    """Crea un handle Shopify-safe da un titolo"""
    if not value:
        return ""
    s = str(value)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s).strip().lower()
    s = re.sub(r"[-\s]+", "-", s)
    return s or "product"

def split_images(cell) -> List[str]:
    """Splitta una cella di immagini in lista"""
    if cell is None or pd.isna(cell):
        return []
    if isinstance(cell, list):
        return cell
    s = str(cell).strip()
    if not s:
        return []
    # Supporta newline, virgole, pipe, punti e virgola
    parts = re.split(r"\r\n|\n|[,|;]+", s)
    parts = [p.strip() for p in parts if p and p.strip()]
    return parts

def to_wix_url(filename: str) -> str:
    """Converte filename Wix in URL pubblico"""
    if not filename:
        return ""
    filename = filename.strip()
    if filename.startswith("http://") or filename.startswith("https://"):
        return filename
    if "static.wixstatic.com" in filename:
        return filename
    filename_clean = filename.lstrip("/").lstrip("\\")
    return f"https://static.wixstatic.com/media/{filename_clean}"

def parse_attribute_values(val: str) -> List[str]:
    """Parse valori attributi: 'M | L | XL' → ['M', 'L', 'XL']"""
    if val is None or pd.isna(val) or str(val).strip() == "":
        return []
    s = str(val)
    parts = re.split(r"\s*\|\s*|\s*,\s*|\s*;\s*", s.strip())
    return [p.strip() for p in parts if p.strip()]

def clean_price(price_val) -> str:
    """Pulisce valori prezzo rimuovendo simboli di valuta"""
    if price_val is None or pd.isna(price_val):
        return ""
    s = str(price_val).strip()
    # Rimuovi simboli comuni: €, $, £
    s = re.sub(r"[€$£,\s]", "", s)
    # Converti virgola decimale in punto
    s = s.replace(",", ".")
    try:
        float(s)
        return s
    except:
        return ""

def clean_weight(weight_val) -> str:
    """Converte peso in grammi (Shopify usa grammi)"""
    if weight_val is None or pd.isna(weight_val):
        return ""
    s = str(weight_val).strip().lower()
    # Rimuovi unità comuni
    s = re.sub(r"(kg|g|gr|grams?|kilos?)", "", s).strip()
    s = s.replace(",", ".")
    try:
        weight_float = float(s)
        # Se sembra essere in kg (< 10), converti in grammi
        if weight_float < 10:
            weight_float *= 1000
        return str(int(weight_float))
    except:
        return ""

# -------------------------
# PLATFORM DETECTION
# -------------------------
def detect_file_type(df: pd.DataFrame) -> str:
    """
    Rileva piattaforma di origine
    Returns: 'wix', 'woocommerce', 'prestashop', o 'unknown'
    """
    cols = [c.lower() for c in df.columns]
    
    # Wix detection
    if any("media" in c or "product_media" in c for c in cols):
        return "wix"
    
    # WooCommerce detection (anche export italiani)
    woo_indicators = ["tipo", "type", "attribute 1", "regular price", "prezzo", "images", "immagini"]
    if sum(1 for ind in woo_indicators if any(ind in c for c in cols)) >= 2:
        return "woocommerce"
    
    # PrestaShop detection (future)
    prestashop_indicators = ["id_product", "reference", "categories"]
    if sum(1 for ind in prestashop_indicators if any(ind in c for c in cols)) >= 2:
        return "prestashop"
    
    return "unknown"

# -------------------------
# COLUMN DETECTION
# -------------------------
def detect_common_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Mappa colonne comuni tra piattaforme"""
    lower_map = {c.lower(): c for c in df.columns}
    
    def find_any(candidates):
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None
    
    return {
        'id': find_any(['ID', 'id', 'post_id', 'product_id', 'handleid']),
        'title': find_any(['Name', 'Nome', 'post_title', 'title', 'Title']),
        'price': find_any(['Regular price', 'Prezzo', 'Price', 'regular_price', 'price']),
        'sale_price': find_any(['Sale price', 'Prezzo scontato', 'sale_price', 'compare_at_price']),
        'sku': find_any(['SKU', 'sku']),
        'barcode': find_any(['Barcode', 'barcode', 'ean', 'EAN', 'gtin']),
        'stock': find_any(['Stock', 'Stock quantity', 'Quantity', 'quantity', 'stock_quantity', 'Giacenza']),
        'images': find_any(['Images', 'Immagini', 'Gallery', 'Image', 'media', 'media url', 'product_media']),
        'desc': find_any(['Description', 'Descrizione', 'post_content', 'body', 'Body (HTML)']),
        'weight': find_any(['Weight', 'Peso', 'weight', 'weight_kg', 'weight_g', 'Variant Grams']),
        'type': find_any(['Type', 'Tipo', 'post_type', 'Product type']),
        'tags': find_any(['Tags', 'tags', 'Tag']),
        'vendor': find_any(['Vendor', 'vendor', 'brand', 'marca'])
    }

def find_attribute_columns(df: pd.DataFrame) -> Tuple[Dict[str,str], Dict[str,str]]:
    """
    Trova colonne attributi (es: Attribute 1 name/value)
    Returns: (attr_name_map, attr_value_map)
    """
    attr_names = {}
    attr_values = {}
    
    for c in df.columns:
        lc = c.lower()
        
        # Attribute name: "Attribute 1 name" o "Nome dell'attributo 1"
        m_name = re.search(r'attribute.*name.*?(\d+)', lc)
        if m_name:
            idx = m_name.group(1)
            attr_names[idx] = c
            continue
            
        m_it = re.search(r'nome.*attributo.*?(\d+)', lc)
        if m_it:
            idx = m_it.group(1)
            attr_names[idx] = c
            continue
        
        # Attribute value: "Attribute 1 value(s)" o "Attribute 1"
        m_val = re.search(r'attribute.*value.*?(\d+)', lc)
        if m_val:
            idx = m_val.group(1)
            attr_values[idx] = c
            continue
            
        m_val2 = re.search(r'attribute\s*(\d+)(?!\s*name)', lc)
        if m_val2:
            idx = m_val2.group(1)
            if idx not in attr_values:
                attr_values[idx] = c
            continue
        
        # WooCommerce pattern: "attribute_pa_color"
        if 'attribute_pa_' in lc or re.match(r'attribute_[a-z_]+$', lc):
            key = lc
            attr_values[key] = c
    
    return attr_names, attr_values

# -------------------------
# SHOPIFY ROW BUILDER
# -------------------------
def build_shopify_rows_from_group(
    group: pd.DataFrame,
    common: Dict[str,str],
    attr_name_map: Dict[str,str],
    attr_value_map: Dict[str,str],
    source_type: str
) -> List[Dict]:
    """
    Costruisce righe Shopify da un gruppo di prodotto
    """
    master = group.iloc[0]
    
    # Dati prodotto master
    title = str(master.get(common['title'], "")).strip() if common.get('title') else ""
    handle = slugify(title) or f"product-{master.name}"
    desc = str(master.get(common['desc'], "")).strip() if common.get('desc') else ""
    vendor = str(master.get(common['vendor'], "")).strip() if common.get('vendor') else ""
    tags = str(master.get(common['tags'], "")).strip() if common.get('tags') else ""
    product_type = str(master.get(common['type'], "")).strip() if common.get('type') else ""
    
    # Prezzi e stock master
    price_master = clean_price(master.get(common['price'], "")) if common.get('price') else ""
    sale_master = clean_price(master.get(common['sale_price'], "")) if common.get('sale_price') else ""
    stock_master = str(master.get(common['stock'], "")).strip() if common.get('stock') else "0"
    sku_master = str(master.get(common['sku'], "")).strip() if common.get('sku') else ""
    barcode_master = str(master.get(common['barcode'], "")).strip() if common.get('barcode') else ""
    weight_master = clean_weight(master.get(common['weight'], "")) if common.get('weight') else ""
    
    # Immagini
    imgs_master = split_images(master.get(common['images'], "")) if common.get('images') else []
    if source_type == "wix":
        imgs_master = [to_wix_url(i) for i in imgs_master]
    
    # Cerca righe varianti esplicite (Type=variation)
    var_rows = group[group.apply(
        lambda r: str(r.get('Type', '')).lower() == 'variation' or 
                  str(r.get('type', '')).lower() == 'variation',
        axis=1
    )]
    
    variants = []
    
    if not var_rows.empty:
        # Varianti esplicite
        for _, vr in var_rows.iterrows():
            v_sku = str(vr.get(common['sku'], sku_master)).strip() if common.get('sku') else sku_master
            v_price = clean_price(vr.get(common['price'], price_master)) if common.get('price') else price_master
            v_sale = clean_price(vr.get(common['sale_price'], sale_master)) if common.get('sale_price') else sale_master
            v_stock = str(vr.get(common['stock'], stock_master)).strip() if common.get('stock') else stock_master
            v_barcode = str(vr.get(common['barcode'], barcode_master)).strip() if common.get('barcode') else barcode_master
            v_weight = clean_weight(vr.get(common['weight'], weight_master)) if common.get('weight') else weight_master
            
            v_imgs = split_images(vr.get(common['images'], "")) if common.get('images') and vr.get(common['images']) else imgs_master
            if source_type == "wix":
                v_imgs = [to_wix_url(i) for i in v_imgs]
            
            # Opzioni variante
            opt_vals = []
            for key, col in attr_value_map.items():
                if col in vr.index and str(vr.get(col)).strip():
                    opt_vals.append(str(vr.get(col)).strip())
            
            variants.append({
                'sku': v_sku,
                'price': v_price,
                'compare_at_price': v_sale,
                'stock': v_stock,
                'barcode': v_barcode,
                'weight': v_weight,
                'images': v_imgs,
                'options': opt_vals
            })
    else:
        # Crea combinazioni da attributi master
        option_names = []
        lists = []
        
        for idx, name_col in attr_name_map.items():
            name_val = master.get(name_col, "")
            val_col = attr_value_map.get(idx)
            if val_col and master.get(val_col):
                option_names.append(str(name_val) if name_val else f"Option{len(option_names)+1}")
                lists.append(parse_attribute_values(master.get(val_col)))
        
        if not lists and attr_value_map:
            for idx, val_col in attr_value_map.items():
                v = master.get(val_col, "")
                if v and str(v).strip():
                    option_names.append(f"Option{len(option_names)+1}")
                    lists.append(parse_attribute_values(v))
        
        if lists:
            combos = list(product(*lists))
            for combo in combos:
                variants.append({
                    'sku': sku_master,
                    'price': price_master,
                    'compare_at_price': sale_master,
                    'stock': stock_master,
                    'barcode': barcode_master,
                    'weight': weight_master,
                    'images': imgs_master,
                    'options': list(combo)
                })
        else:
            # Singola variante
            variants.append({
                'sku': sku_master,
                'price': price_master,
                'compare_at_price': sale_master,
                'stock': stock_master,
                'barcode': barcode_master,
                'weight': weight_master,
                'images': imgs_master,
                'options': []
            })
    
    # Determina nomi opzioni finali
    option_names_final = []
    if attr_name_map:
        try:
            keys_sorted = sorted(attr_name_map.keys(), 
                               key=lambda k: int(re.search(r'\d+', str(k)).group(0)) 
                               if re.search(r'\d+', str(k)) else str(k))
        except:
            keys_sorted = list(attr_name_map.keys())
        for k in keys_sorted:
            option_names_final.append(str(master.get(attr_name_map[k], f"Option{len(option_names_final)+1}")))
    else:
        try:
            keys_sorted = sorted(attr_value_map.keys(), 
                               key=lambda k: int(re.search(r'\d+', str(k)).group(0)) 
                               if re.search(r'\d+', str(k)) else str(k))
        except:
            keys_sorted = list(attr_value_map.keys())
        for k in keys_sorted:
            option_names_final.append(f"Option{len(option_names_final)+1}")
    
    # Costruisci righe Shopify
    rows = []
    first = True
    
    for v in variants:
        r = {c: "" for c in SHOPIFY_COLUMNS}
        
        if first:
            # Prima riga contiene info prodotto
            r["Handle"] = handle
            r["Title"] = title
            r["Body (HTML)"] = desc
            r["Vendor"] = vendor
            r["Type"] = product_type
            r["Tags"] = tags
            r["Published"] = "TRUE"
            r["Status"] = "active"
        else:
            # Righe successive solo handle
            r["Handle"] = handle
        
        # Opzioni
        if len(option_names_final) > 0:
            r["Option1 Name"] = option_names_final[0]
            r["Option1 Value"] = v['options'][0] if len(v['options']) > 0 else "Default Title"
        if len(option_names_final) > 1:
            r["Option2 Name"] = option_names_final[1]
            r["Option2 Value"] = v['options'][1] if len(v['options']) > 1 else ""
        if len(option_names_final) > 2:
            r["Option3 Name"] = option_names_final[2]
            r["Option3 Value"] = v['options'][2] if len(v['options']) > 2 else ""
        
        # Variante
        r["Variant SKU"] = v.get('sku', "")
        r["Variant Price"] = v.get('price', "")
        r["Variant Compare At Price"] = v.get('compare_at_price', "")
        r["Variant Inventory Qty"] = v.get('stock', "0")
        r["Variant Barcode"] = v.get('barcode', "")
        r["Variant Grams"] = v.get('weight', "")
        r["Variant Weight Unit"] = "g"
        r["Variant Inventory Tracker"] = "shopify"
        r["Variant Inventory Policy"] = "deny"
        r["Variant Fulfillment Service"] = "manual"
        r["Variant Requires Shipping"] = "TRUE"
        r["Variant Taxable"] = "TRUE"
        
        # Immagini
        if v.get('images'):
            if first:
                r["Image Src"] = v['images'][0]
                r["Image Position"] = "1"
            r["Variant Image"] = v['images'][0]
        
        rows.append(r)
        first = False
    
    return rows

# -------------------------
# CONVERTERS
# -------------------------
def convert_woocommerce_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    """Converte CSV WooCommerce → Shopify"""
    common = detect_common_columns(df)
    attr_names, attr_values = find_attribute_columns(df)
    
    id_col = common.get('id') or (df.columns[0] if len(df.columns) > 0 else None)
    if id_col is None:
        df = df.reset_index().rename(columns={'index': 'ID'})
        id_col = 'ID'
    
    grouped = df.groupby(id_col, dropna=False, sort=False)
    out_rows = []
    
    for pid, group in grouped:
        rows = build_shopify_rows_from_group(group, common, attr_names, attr_values, "woocommerce")
        out_rows.extend(rows)
    
    out_df = pd.DataFrame(out_rows, columns=SHOPIFY_COLUMNS)
    out_df = out_df.fillna("")
    return out_df

def convert_wix_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    """Converte CSV Wix → Shopify"""
    common = detect_common_columns(df)
    attr_names, attr_values = find_attribute_columns(df)
    
    id_col = common.get('id') or (df.columns[0] if len(df.columns) > 0 else None)
    if id_col is None:
        df = df.reset_index().rename(columns={'index': 'ID'})
        id_col = 'ID'
    
    grouped = df.groupby(id_col, dropna=False, sort=False)
    out_rows = []
    
    for pid, group in grouped:
        rows = build_shopify_rows_from_group(group, common, attr_names, attr_values, "wix")
        out_rows.extend(rows)
    
    out_df = pd.DataFrame(out_rows, columns=SHOPIFY_COLUMNS)
    out_df = out_df.fillna("")
    return out_df

# -------------------------
# PUBLIC API
# -------------------------
def convert_csv_path_to_shopify_csv(input_csv_path: str, output_csv_path: str) -> str:
    """
    Funzione principale: converte CSV → Shopify
    
    Args:
        input_csv_path: percorso file CSV input
        output_csv_path: percorso file CSV output
    
    Returns:
        output_csv_path
    """
    print(f"[transformer] Reading CSV: {input_csv_path}")
    
    # Lettura con encoding robusto
    try:
        df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False, encoding='latin1')
    
    print(f"[transformer] Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Rilevamento piattaforma
    file_type = detect_file_type(df)
    print(f"[transformer] Detected platform: {file_type}")
    
    # Conversione
    if file_type == "wix":
        out_df = convert_wix_df_to_shopify(df)
    elif file_type == "woocommerce":
        out_df = convert_woocommerce_df_to_shopify(df)
    else:
        # Fallback a WooCommerce
        print(f"[transformer] WARNING: Unknown format, trying WooCommerce conversion")
        out_df = convert_woocommerce_df_to_shopify(df)
    
    # Salvataggio
    out_df.to_csv(output_csv_path, index=False, encoding="utf-8")
    print(f"[transformer] ✅ Saved Shopify CSV: {output_csv_path} ({len(out_df)} rows)")
    
    return output_csv_path