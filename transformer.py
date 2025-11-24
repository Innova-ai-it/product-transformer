# transformer.py
import pandas as pd
import re
import unicodedata
from itertools import product
from typing import List

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

def slugify(value: str) -> str:
    if pd.isna(value) or value is None:
        return ""
    s = str(value)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^\w\s-]", "", s).strip().lower()
    s = re.sub(r"[-\s]+", "-", s)
    return s

def split_images(cell) -> List[str]:
    if pd.isna(cell) or cell is None:
        return []
    if isinstance(cell, list):
        return cell
    s = str(cell).strip()
    parts = re.split(r"\s*,\s*|\s*\|\s*|\s*;\s*", s)
    parts = [p for p in parts if p]
    return parts

def parse_attribute_values(val: str) -> List[str]:
    if pd.isna(val) or val is None:
        return []
    s = str(val).strip()
    parts = re.split(r"\s*\|\s*|\s*,\s*|\s*;\s*", s)
    return [p.strip() for p in parts if p.strip()]

def ensure_cols(df: pd.DataFrame, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = ""

def detect_common_columns(df: pd.DataFrame):
    # try to find id/title/sku/price/stock/images/desc/weight columns with robust fallbacks
    cols = {c.lower(): c for c in df.columns}
    def find(candidates):
        for cand in candidates:
            if cand.lower() in cols:
                return cols[cand.lower()]
        return None

    id_col = find(['ID','Id','id','post_id','product_id'])
    title_col = find(['Name','Nome','post_title','title'])
    price_col = find(['Regular price','Prezzo','Price','regular_price'])
    sale_price_col = find(['Sale price','Prezzo scontato','sale_price'])
    sku_col = find(['SKU','sku'])
    stock_col = find(['Stock','Stock quantity','Quantity','quantity'])
    images_col = find(['Images','Immagini','Gallery','Image','Product image'])
    desc_col = find(['Description','Descrizione','post_content'])
    weight_col = find(['Weight','Peso','weight','weight_kg','weight_g'])
    type_col = find(['Type','Tipo','post_type'])
    return {
        'id': id_col, 'title': title_col, 'price': price_col, 'sale_price': sale_price_col,
        'sku': sku_col, 'stock': stock_col, 'images': images_col, 'desc': desc_col,
        'weight': weight_col, 'type': type_col
    }

def find_attribute_columns(df: pd.DataFrame):
    """
    Heuristics to find attribute name/value columns (WooCommerce export naming variations).
    Returns two dicts: attr_names[idx] = col_name, attr_values[idx] = col_name
    """
    attr_names = {}
    attr_values = {}
    for c in df.columns:
        cl = c.lower()
        # name columns
        m_name = re.match(r'.*(attribute).*name.*?(\d+).*', cl)
        if m_name:
            idx = m_name.group(2)
            attr_names[idx] = c
            continue
        m_it = re.match(r'.*nome.*attributo.*?(\d+).*', cl)
        if m_it:
            idx = m_it.group(1)
            attr_names[idx] = c
            continue
        # value columns
        m_val = re.match(r'.*(attribute).*value.*?(\d+).*', cl)
        if m_val:
            idx = m_val.group(2)
            attr_values[idx] = c
            continue
        m_val2 = re.match(r'.*(attribute)\s?(\d+).*', cl)
        if m_val2:
            idx = m_val2.group(2)
            if idx not in attr_values:
                attr_values[idx] = c
            continue
        # generic patterns like attribute_pa_size or attribute_size
        if 'attribute_pa_' in cl or re.match(r'attribute_.+', cl):
            # use a synthetic idx based on column name
            key = cl
            attr_values[key] = c

    # fallback: columns named "Attributo 1", "Attribute 1"
    for i in range(1,7):
        cand = f'Attribute {i}'
        it = f'Attributo {i}'
        if cand in df.columns and str(df[cand]).strip():
            attr_values[str(i)] = cand
        if it in df.columns and str(df[it]).strip():
            attr_values[str(i)] = it

    return attr_names, attr_values

def build_shopify_rows_from_group(group: pd.DataFrame, common: dict, attr_names_map, attr_values_map) -> List[dict]:
    """
    group: DataFrame rows that belong to the same product ID
    common: detected common columns mapping
    returns list of rows (one per variant) ready for Shopify DataFrame
    """
    master = group.iloc[0]
    title = master.get(common['title'], "") if common['title'] else ""
    handle = slugify(title) or f"product-{master.name}"
    desc = master.get(common['desc'], "") if common['desc'] else ""
    vendor = ""  # not used
    price_master = master.get(common['price'], "") if common['price'] else ""
    sale_master = master.get(common['sale_price'], "") if common['sale_price'] else ""
    stock_master = master.get(common['stock'], "") if common['stock'] else ""
    sku_master = master.get(common['sku'], "") if common['sku'] else ""

    # collect product-level images (master row)
    imgs_master = split_images(master.get(common['images'], "")) if common['images'] else []

    # Detect explicit variation rows (Woo exports variations as separate rows of Type 'variation')
    var_rows = group[group.apply(lambda r: str(r.get('Type','')).lower()=='variation' or str(r.get('type','')).lower()=='variation', axis=1)]
    variants = []

    if not var_rows.empty:
        for _, vr in var_rows.iterrows():
            v_sku = vr.get(common['sku'], "") if common['sku'] else ""
            v_price = vr.get(common['price'], price_master) if common['price'] else ""
            v_sale = vr.get(common['sale_price'], sale_master) if common['sale_price'] else ""
            v_stock = vr.get(common['stock'], stock_master) if common['stock'] else ""
            v_imgs = split_images(vr.get(common['images'], "")) if common['images'] and vr.get(common['images']) else imgs_master

            # gather option values from attribute value columns for this variation
            opt_vals = []
            for key, col in attr_values_map.items():
                if col in vr.index and str(vr.get(col)).strip():
                    opt_vals.append(str(vr.get(col)).strip())
            # fallback: scan for attribute_pa_ style columns
            if not opt_vals:
                for c in vr.index:
                    if re.search(r'attribute_pa_|attribute_', c, re.IGNORECASE):
                        if str(vr.get(c)).strip():
                            opt_vals.append(str(vr.get(c)).strip())

            variants.append({
                'sku': v_sku,
                'price': v_price,
                'compare_at_price': v_sale,
                'stock': v_stock,
                'images': v_imgs,
                'options': opt_vals
            })
    else:
        # No explicit variation rows: build combos from product-level attribute value fields
        option_names = []
        lists = []
        # use attr_names_map/attr_values_map to get option names and values
        for idx, name_col in attr_names_map.items():
            # if there's a name column, find its value in master
            val_name = master.get(name_col, "")
            val_values_col = attr_values_map.get(idx)
            if val_values_col and master.get(val_values_col):
                option_names.append(val_name if val_name else f"Option{len(option_names)+1}")
                lists.append(parse_attribute_values(master.get(val_values_col)))
        # fallback: if no explicit attribute name columns, use attr_values_map generically
        if not lists and attr_values_map:
            for key, col in attr_values_map.items():
                v = master.get(col, "")
                if v:
                    option_names.append(f"Option{len(option_names)+1}")
                    lists.append(parse_attribute_values(v))
        if lists:
            combos = list(product(*lists))
            for combo in combos:
                variants.append({
                    'sku': "", 'price': price_master, 'compare_at_price': sale_master,
                    'stock': stock_master, 'images': imgs_master, 'options': list(combo)
                })
        else:
            # single variant product
            variants.append({
                'sku': sku_master or "",
                'price': price_master,
                'compare_at_price': sale_master,
                'stock': stock_master,
                'images': imgs_master,
                'options': []
            })

    # Determine option names (prioritize attr_names_map, otherwise generic)
    option_names_final = []
    if attr_names_map:
        # order by numeric index where possible
        try:
            sorted_keys = sorted(attr_names_map.keys(), key=lambda k: int(re.search(r'\d+', str(k)).group(0)) if re.search(r'\d+', str(k)) else str(k))
        except Exception:
            sorted_keys = list(attr_names_map.keys())
        for k in sorted_keys:
            option_names_final.append(str(master.get(attr_names_map[k], f"Option{len(option_names_final)+1}")))
    else:
        # try to derive from attr_values_map order
        try:
            sorted_keys = sorted(attr_values_map.keys(), key=lambda k: int(re.search(r'\d+', str(k)).group(0)) if re.search(r'\d+', str(k)) else str(k))
        except Exception:
            sorted_keys = list(attr_values_map.keys())
        for k in sorted_keys:
            option_names_final.append(f"Option{len(option_names_final)+1}")

    # Build Shopify rows (one per variant). First variant contains product-level info (Title, Description, Vendor)
    rows = []
    first = True
    for v in variants:
        r = {c: "" for c in SHOPIFY_COLUMNS}
        if first:
            r["Title"] = title
            r["URL handle"] = handle
            r["Description"] = desc
            r["Vendor"] = vendor
            r["Published on online store"] = "TRUE"
            r["Status"] = "active"
        else:
            r["Title"] = ""
            r["URL handle"] = handle

        # Options
        if len(option_names_final) > 0:
            r["Option1 name"] = option_names_final[0]
            r["Option1 value"] = v['options'][0] if len(v['options'])>0 else ""
        if len(option_names_final) > 1:
            r["Option2 name"] = option_names_final[1]
            r["Option2 value"] = v['options'][1] if len(v['options'])>1 else ""
        if len(option_names_final) > 2:
            r["Option3 name"] = option_names_final[2]
            r["Option3 value"] = v['options'][2] if len(v['options'])>2 else ""

        # SKU, prices, stock
        r["SKU"] = v.get('sku', "")
        r["Price"] = v.get('price', "")
        r["Compare-at price"] = v.get('compare_at_price', "")
        r["Inventory quantity"] = v.get('stock', "")

        # Images: first image as Product image on first variant, Variant image on each variant if available
        if v.get('images'):
            if first:
                r["Product image URL"] = v['images'][0]
                r["Image position"] = 1
            r["Variant image URL"] = v['images'][0]

        # Weight conversion (best-effort)
        # handled at product master level if present, else left empty

        rows.append(r)
        first = False

    return rows

def convert_woocommerce_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    # detect columns
    common = detect_common_columns(df)
    ensure_cols(df, [])  # no-op but kept for extensibility
    attr_names_map, attr_values_map = find_attribute_columns(df)

    # define id column
    id_col = common.get('id') or (df.columns[0] if len(df.columns)>0 else None)
    if id_col is None:
        df = df.reset_index().rename(columns={'index':'ID'})
        id_col = 'ID'

    grouped = df.groupby(id_col, dropna=False, sort=False)
    out_rows = []
    for pid, group in grouped:
        rows = build_shopify_rows_from_group(group, common, attr_names_map, attr_values_map)
        out_rows.extend(rows)

    out_df = pd.DataFrame(out_rows, columns=SHOPIFY_COLUMNS)
    out_df = out_df.fillna("")
    return out_df

def convert_woocommerce_csv_path_to_shopify_csv(input_csv_path: str, output_csv_path: str):
    df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)
    shopify_df = convert_woocommerce_df_to_shopify(df)
    shopify_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    return output_csv_path
