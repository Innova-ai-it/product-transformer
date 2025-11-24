# transformer.py
import pandas as pd
import re
import unicodedata
from itertools import product
from typing import List, Dict, Tuple

# Shopify header requested by user (subset important fields included)
SHOPIFY_COLUMNS = [
"Title","URL handle","Description","Vendor","Product category","Type","Tags",
"Published on online store","Status","SKU","Barcode","Option1 name","Option1 value",
"Option2 name","Option2 value","Option3 name","Option3 value","Price","Compare-at price",
"Cost per item","Charge tax","Tax code","Unit price total measure","Unit price total measure unit",
"Unit price base measure","Unit price base measure unit","Inventory tracker","Inventory quantity",
"Continue selling when out of stock","Weight value (grams)","Weight unit for display","Requires shipping",
"Fulfillment service","Product image URL","Image position","Image alt text","Variant image URL","Gift card",
"SEO title","SEO description","Google Shopping / Google product category","Google Shopping / Gender",
"Google Shopping / Age group","Google Shopping / MPN","Google Shopping / AdWords Grouping",
"Google Shopping / AdWords labels","Google Shopping / Condition","Google Shopping / Custom product",
"Google Shopping / Custom label 0","Google Shopping / Custom label 1","Google Shopping / Custom label 2",
"Google Shopping / Custom label 3","Google Shopping / Custom label 4"
]

# -------------------------
# Utility helpers
# -------------------------
def slugify(value: str) -> str:
    """Create a Shopify-safe handle from title."""
    if value is None:
        return ""
    s = str(value)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s).strip().lower()
    s = re.sub(r"[-\s]+", "-", s)
    return s

def split_images(cell) -> List[str]:
    """Split an images cell into a list of image strings."""
    if cell is None:
        return []
    if isinstance(cell, list):
        return cell
    s = str(cell).strip()
    if not s:
        return []
    # support newlines, commas, pipes, semicolons
    parts = re.split(r"\r\n|\n|[,|;]+", s)
    parts = [p.strip() for p in parts if p and p.strip()]
    return parts

def to_wix_url(filename: str) -> str:
    """Convert a Wix stored filename to a public Wix static URL, if it isn't already a URL."""
    if not filename:
        return ""
    filename = filename.strip()
    if filename.startswith("http://") or filename.startswith("https://"):
        return filename
    # Some Wix filenames may contain leading folders - keep them
    # If filename already contains 'static.wixstatic.com', return as is
    if "static.wixstatic.com" in filename:
        return filename
    # Ensure we don't add leading slashes double
    filename_clean = filename.lstrip("/").lstrip("\\")
    return f"https://static.wixstatic.com/media/{filename_clean}"

def parse_attribute_values(val: str) -> List[str]:
    """Given a cell like 'M | L | XL' or 'M,L,XL' return list ['M','L','XL']"""
    if val is None or str(val).strip() == "":
        return []
    s = str(val)
    parts = re.split(r"\s*\|\s*|\s*,\s*|\s*;\s*", s.strip())
    return [p.strip() for p in parts if p.strip()]

# -------------------------
# Detection
# -------------------------
def detect_file_type(df: pd.DataFrame) -> str:
    """
    Heuristic detection of file origin.
    Returns: 'wix', 'woocommerce', or 'unknown'
    """
    cols = [c.lower() for c in df.columns]
    # Wix often has a column named 'media' or 'media url' or 'media\n...' or 'product media'
    if any("media" == c or c.startswith("media") or "product_media" in c or "media" in c for c in cols):
        return "wix"
    # WooCommerce exports often include 'Tipo', 'Nome', 'ID' (in Italian exports) or 'Type' 'SKU' etc.
    if any(x in cols for x in ["tipo", "type", "nome", "name", "post_title", "id", "variations", "attribute 1"]):
        return "woocommerce"
    return "unknown"

# -------------------------
# Column detection helpers
# -------------------------
def detect_common_columns(df: pd.DataFrame) -> Dict[str, str]:
    """
    Returns dict mapping of commonly used columns (title, sku, price, sale_price, stock, images, desc, weight, type, id)
    """
    lower_map = {c.lower(): c for c in df.columns}
    def find_any(cands):
        for cand in cands:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None

    common = {
        'id': find_any(['ID','Id','id','post_id','product_id']),
        'title': find_any(['Name','Nome','post_title','title']),
        'price': find_any(['Regular price','Prezzo','Price','regular_price','price']),
        'sale_price': find_any(['Sale price','Prezzo scontato','sale_price']),
        'sku': find_any(['SKU','sku']),
        'stock': find_any(['Stock','Stock quantity','Quantity','quantity','stock_quantity']),
        'images': find_any(['Images','Immagini','Gallery','Image','media','media url','product_media']),
        'desc': find_any(['Description','Descrizione','post_content','body']),
        'weight': find_any(['Weight','Peso','weight','weight_kg','weight_g']),
        'type': find_any(['Type','Tipo','post_type'])
    }
    return common

def find_attribute_columns(df: pd.DataFrame) -> Tuple[Dict[str,str], Dict[str,str]]:
    """
    Heuristic: find attribute name columns and attribute value columns for Woo exports
    returns (attr_name_map, attr_value_map) keyed by idx (string)
    """
    attr_names = {}
    attr_values = {}
    for c in df.columns:
        lc = c.lower()
        # attribute name columns: 'attribute 1 name' or 'nome dell'attributo 1'
        m_name = re.match(r'.*(attribute).*name.*?(\d+).*', lc)
        if m_name:
            idx = m_name.group(2)
            attr_names[idx] = c
            continue
        m_it = re.match(r'.*nome.*attributo.*?(\d+).*', lc)
        if m_it:
            idx = m_it.group(1)
            attr_names[idx] = c
            continue
        # attribute value columns: 'attribute 1 value' or 'attribute 1' or 'valore dell'attributo 1'
        m_val = re.match(r'.*(attribute).*value.*?(\d+).*', lc)
        if m_val:
            idx = m_val.group(2)
            attr_values[idx] = c
            continue
        m_val2 = re.match(r'.*(attribute)\s?(\d+).*', lc)
        if m_val2:
            idx = m_val2.group(2)
            if idx not in attr_values:
                attr_values[idx] = c
            continue
        # fallback patterns like attribute_pa_color etc.
        if 'attribute_pa_' in lc or re.match(r'attribute_.+', lc):
            key = lc
            attr_values[key] = c
    # fallback for generic columns 'Attributo 1' etc.
    for i in range(1,7):
        cand = f'Attribute {i}'
        it = f'Attributo {i}'
        if cand in df.columns and str(df[cand]).strip():
            attr_values[str(i)] = cand
        if it in df.columns and str(df[it]).strip():
            attr_values[str(i)] = it
    return attr_names, attr_values

# -------------------------
# Core builders
# -------------------------
def build_shopify_rows_from_group(group: pd.DataFrame, common: Dict[str,str], attr_name_map: Dict[str,str], attr_value_map: Dict[str,str], source_type: str) -> List[Dict]:
    """Given a grouped DataFrame (one product), produce list of shopify rows (one per variant)."""
    master = group.iloc[0]
    title = master.get(common['title'], "") if common.get('title') else ""
    handle = slugify(title) or f"product-{master.name}"
    desc = master.get(common['desc'], "") if common.get('desc') else ""
    price_master = master.get(common['price'], "") if common.get('price') else ""
    sale_master = master.get(common['sale_price'], "") if common.get('sale_price') else ""
    stock_master = master.get(common['stock'], "") if common.get('stock') else ""
    sku_master = master.get(common['sku'], "") if common.get('sku') else ""
    imgs_master = split_images(master.get(common['images'], "")) if common.get('images') else []

    # If source is wix and images are filenames, convert to wixcdn urls
    if source_type == "wix":
        imgs_master = [to_wix_url(i) for i in imgs_master]

    # Detect explicit variation rows (Woo: Type == 'variation' or type == 'variation')
    var_rows = group[group.apply(lambda r: str(r.get('Type','')).lower()=='variation' or str(r.get('type','')).lower()=='variation', axis=1)]

    variants = []
    if not var_rows.empty:
        for _, vr in var_rows.iterrows():
            v_sku = vr.get(common['sku'], "") if common.get('sku') else ""
            v_price = vr.get(common['price'], price_master) if common.get('price') else ""
            v_sale = vr.get(common['sale_price'], sale_master) if common.get('sale_price') else ""
            v_stock = vr.get(common['stock'], stock_master) if common.get('stock') else ""
            v_imgs = split_images(vr.get(common['images'], "")) if common.get('images') and vr.get(common['images']) else imgs_master
            if source_type == "wix":
                v_imgs = [to_wix_url(i) for i in v_imgs]
            # collect option values for this variation
            opt_vals = []
            for key, col in attr_value_map.items():
                if col in vr.index and str(vr.get(col)).strip():
                    opt_vals.append(str(vr.get(col)).strip())
            # fallback scan for 'attribute_pa_' style columns
            if not opt_vals:
                for c in vr.index:
                    if re.search(r'attribute_pa_|attribute_', c, re.IGNORECASE):
                        if str(vr.get(c)).strip():
                            opt_vals.append(str(vr.get(c)).strip())
            # Fallback: some Wix exports may have explicit option columns like 'option1', 'option2'
            for on in ['option1','option2','option3','variant_option_1','variant_option_2']:
                if on in vr.index and str(vr.get(on)).strip():
                    opt_vals.append(str(vr.get(on)).strip())

            variants.append({
                'sku': v_sku,
                'price': v_price,
                'compare_at_price': v_sale,
                'stock': v_stock,
                'images': v_imgs,
                'options': opt_vals
            })
    else:
        # No explicit variation rows -> build combinations from master attribute columns
        option_names = []
        lists = []
        # use attr_name_map & attr_value_map if present
        for idx, name_col in attr_name_map.items():
            name_val = master.get(name_col, "")
            val_col = attr_value_map.get(idx)
            if val_col and master.get(val_col):
                option_names.append(name_val if name_val else f"Option{len(option_names)+1}")
                lists.append(parse_attribute_values(master.get(val_col)))
        # fallback: attr_value_map only
        if not lists and attr_value_map:
            for idx, val_col in attr_value_map.items():
                v = master.get(val_col, "")
                if v:
                    option_names.append(f"Option{len(option_names)+1}")
                    lists.append(parse_attribute_values(v))
        # Wix specific: some exports have 'media' for images and option columns like 'variants'
        # fallback for Wix: detect columns named 'variant name' 'variant sku' etc.
        if lists:
            combos = list(product(*lists))
            for combo in combos:
                variants.append({
                    'sku': "", 'price': price_master, 'compare_at_price': sale_master,
                    'stock': stock_master, 'images': imgs_master, 'options': list(combo)
                })
        else:
            # single variant
            variants.append({
                'sku': sku_master or "",
                'price': price_master,
                'compare_at_price': sale_master,
                'stock': stock_master,
                'images': imgs_master,
                'options': []
            })

    # determine option names final
    option_names_final = []
    if attr_name_map:
        # order keys natural if possible
        try:
            keys_sorted = sorted(attr_name_map.keys(), key=lambda k: int(re.search(r'\d+', str(k)).group(0)) if re.search(r'\d+', str(k)) else str(k))
        except:
            keys_sorted = list(attr_name_map.keys())
        for k in keys_sorted:
            option_names_final.append(str(master.get(attr_name_map[k], f"Option{len(option_names_final)+1}")))
    else:
        # derive from attr_value_map ordering
        try:
            keys_sorted = sorted(attr_value_map.keys(), key=lambda k: int(re.search(r'\d+', str(k)).group(0)) if re.search(r'\d+', str(k)) else str(k))
        except:
            keys_sorted = list(attr_value_map.keys())
        for k in keys_sorted:
            option_names_final.append(f"Option{len(option_names_final)+1}")

    # build rows for Shopify (one row per variant). the first variant has product fields
    rows = []
    first = True
    for v in variants:
        r = {c: "" for c in SHOPIFY_COLUMNS}
        if first:
            r["Title"] = title
            r["URL handle"] = handle
            r["Description"] = desc
            r["Vendor"] = ""
            r["Published on online store"] = "TRUE"
            r["Status"] = "active"
        else:
            r["URL handle"] = handle

        # fill options
        if len(option_names_final) > 0:
            r["Option1 name"] = option_names_final[0]
            r["Option1 value"] = v['options'][0] if len(v['options'])>0 else ""
        if len(option_names_final) > 1:
            r["Option2 name"] = option_names_final[1]
            r["Option2 value"] = v['options'][1] if len(v['options'])>1 else ""
        if len(option_names_final) > 2:
            r["Option3 name"] = option_names_final[2]
            r["Option3 value"] = v['options'][2] if len(v['options'])>2 else ""

        # SKU / price / stock
        r["SKU"] = v.get('sku', "")
        r["Price"] = v.get('price', "")
        r["Compare-at price"] = v.get('compare_at_price', "")
        r["Inventory quantity"] = v.get('stock', "")

        # Images: first image as product image (first variant), variant image mapped each variant
        if v.get('images'):
            if first:
                r["Product image URL"] = v['images'][0]
                r["Image position"] = 1
            r["Variant image URL"] = v['images'][0]

        rows.append(r)
        first = False

    return rows

# -------------------------
# Converters per platform (df -> df)
# -------------------------
def convert_woocommerce_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    common = detect_common_columns(df)
    attr_names, attr_values = find_attribute_columns(df)
    # id column
    id_col = common.get('id') or (df.columns[0] if len(df.columns)>0 else None)
    if id_col is None:
        df = df.reset_index().rename(columns={'index':'ID'})
        id_col = 'ID'
    grouped = df.groupby(id_col, dropna=False, sort=False)
    out_rows = []
    for pid, group in grouped:
        rows = build_shopify_rows_from_group(group, common, attr_names, attr_values, source_type="woocommerce")
        out_rows.extend(rows)
    out_df = pd.DataFrame(out_rows, columns=SHOPIFY_COLUMNS)
    out_df = out_df.fillna("")
    return out_df

def convert_wix_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    # Wix: identify id/title/images/variant columns heuristically
    # Wix exports often have 'media' column with filenames separated by newline
    # detect columns
    common = detect_common_columns(df)
    # Wix sometimes store variants in separate rows or in JSON columns. We'll attempt to read:
    attr_names, attr_values = find_attribute_columns(df)
    id_col = common.get('id') or (df.columns[0] if len(df.columns)>0 else None)
    if id_col is None:
        df = df.reset_index().rename(columns={'index':'ID'})
        id_col = 'ID'
    grouped = df.groupby(id_col, dropna=False, sort=False)
    out_rows = []
    for pid, group in grouped:
        rows = build_shopify_rows_from_group(group, common, attr_names, attr_values, source_type="wix")
        out_rows.extend(rows)
    out_df = pd.DataFrame(out_rows, columns=SHOPIFY_COLUMNS)
    out_df = out_df.fillna("")
    return out_df

# -------------------------
# Public function to call from app.py
# -------------------------
def convert_csv_path_to_shopify_csv(input_csv_path: str, output_csv_path: str) -> str:
    """
    Detects CSV type (Wix / WooCommerce) and converts file to Shopify CSV.
    Returns the output path.
    """
    df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)
    file_type = detect_file_type(df)
    print(f"[transformer] detected file type: {file_type}")
    if file_type == "wix":
        out_df = convert_wix_df_to_shopify(df)
    else:
        # fallback to woocommerce conversion
        out_df = convert_woocommerce_df_to_shopify(df)
    out_df.to_csv(output_csv_path, index=False, encoding="utf-8")
    print(f"[transformer] saved shopify csv to: {output_csv_path} (rows: {len(out_df)})")
    return output_csv_path
