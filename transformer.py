import pandas as pd
import re
import unicodedata
from itertools import product
from typing import List

def slugify(value: str) -> str:
    """Crea un handle compatibile (slug) per Shopify."""
    if pd.isna(value):
        return ""
    value = str(value)
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    value = re.sub(r"[-\s]+", "-", value)
    return value

def split_images(cell) -> List[str]:
    """Splitta stringa immagini separate da virgola/pipe/space."""
    if pd.isna(cell):
        return []
    if isinstance(cell, list):
        return cell
    s = str(cell)
    # common separators: comma, pipe, semicolon
    parts = re.split(r"\s*,\s*|\s*\|\s*|\s*;\s*", s.strip())
    parts = [p for p in parts if p]
    return parts

def parse_attribute_values(val: str) -> List[str]:
    """Da 'M | L | XL' o 'M,L,XL' restituisce lista."""
    if pd.isna(val):
        return []
    s = str(val).strip()
    parts = re.split(r"\s*\|\s*|\s*,\s*|\s*;\s*", s)
    parts = [p.strip() for p in parts if p.strip()]
    return parts

def ensure_cols(df: pd.DataFrame, cols: List[str]):
    for c in cols:
        if c not in df.columns:
            df[c] = ""

def convert_woocommerce_df_to_shopify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte un DataFrame esportato da WooCommerce in un DataFrame con schema compatibile Shopify CSV.
    Strategie:
      - Raggruppa per product ID (colonna 'ID' o 'id' o 'post_id')
      - Se ci sono righe tipo 'variation', considerale singole varianti
      - Altrimenti genera varianti combinando attributi presenti (Attr1, Attr2, ...)
      - Crea righe extra per immagini aggiuntive (solo Handle + Image Src)
    """

    # Normalizzazioni colonne comuni
    possible_id_cols = ['ID', 'Id', 'id', 'post_id']
    id_col = next((c for c in possible_id_cols if c in df.columns), None)
    if id_col is None:
        # se non c'è id, creiamo uno
        df = df.reset_index().rename(columns={'index': 'ID'})
        id_col = 'ID'

    # assicurati colonne usate
    ensure_cols(df, [
        'Type', 'type', 'post_type', 'Nome', 'Nome prodotto', 'Name',
        'SKU', 'sku', 'Descrizione', 'Short description',
        'Prezzo', 'Prezzo scontato', 'Regular price', 'Sale price',
        'Images', 'Image', 'Gallery', 'Immagini',
        'Published', 'Status',
        'Stock', 'Stock quantity', 'Quantity'
    ])

    # Colonne mappate più probabili (scegli il primo esistente)
    title_col = next((c for c in ['Nome', 'Name', 'post_title'] if c in df.columns), id_col)
    sku_col = next((c for c in ['SKU', 'sku', 'sku_'] if c in df.columns), None)
    price_col = next((c for c in ['Prezzo', 'Price', 'Regular price', 'regular_price'] if c in df.columns), None)
    sale_price_col = next((c for c in ['Prezzo scontato', 'Sale price', 'sale_price'] if c in df.columns), None)
    stock_col = next((c for c in ['Stock', 'Stock quantity', 'Quantity', 'quantity'] if c in df.columns), None)
    images_col = next((c for c in ['Immagini', 'Images', 'Gallery', 'Image'] if c in df.columns), None)
    desc_col = next((c for c in ['Descrizione', 'Description', 'post_content'] if c in df.columns), None)
    tags_col = next((c for c in ['Tags', 'tags', 'Categorie', 'Categories'] if c in df.columns), None)
    vendor_col = next((c for c in ['Marca', 'Brand', 'vendor', 'Vendor'] if c in df.columns), None)

    # Trova colonne attributo (es. "Attribute 1 name" o "Nome dell'attributo 1")
    attr_name_cols = [c for c in df.columns if re.search(r'attributo|attribute|attribute.+name|nome.*attributo', c, re.IGNORECASE)]
    attr_value_cols = [c for c in df.columns if re.search(r'valore.*attributo|attribute.+value', c, re.IGNORECASE)]
    # fallback: cerca colonne come "Attribute 1" o "Attributo 1"
    for i in range(1,6):
        ncol = f'Attribute {i}'
        if ncol in df.columns and ncol not in attr_value_cols:
            attr_value_cols.append(ncol)
        ncol2 = f'Attributo {i}'
        if ncol2 in df.columns and ncol2 not in attr_value_cols:
            attr_value_cols.append(ncol2)

    # Prepara lista di output rows (dizionari)
    out_rows = []

    # Raggruppa per prodotto principale
    grouped = df.groupby(id_col, dropna=False, sort=False)

    for pid, group in grouped:
        # Prendi la prima riga come "master" del prodotto
        master = group.iloc[0]

        p_title = master.get(title_col, "")
        p_handle = slugify(p_title)
        p_desc = master.get(desc_col, "") if desc_col else ""
        p_vendor = master.get(vendor_col, "") if vendor_col else ""
        p_tags = master.get(tags_col, "") if tags_col else ""
        p_images = split_images(master.get(images_col, "")) if images_col else []
        p_price = master.get(price_col, "") if price_col else ""
        p_sale = master.get(sale_price_col, "") if sale_price_col else ""
        p_sku = master.get(sku_col, "") if sku_col else ""
        p_stock = master.get(stock_col, "") if stock_col else ""

        # check se ci sono righe di tipo 'variation' nel group
        variation_rows = group[group.apply(lambda r: str(r.get('Type','')).lower() == 'variation' or str(r.get('type','')).lower() == 'variation', axis=1)]
        has_variations = not variation_rows.empty

        # Se ci sono righe variation: usale come varianti
        variants = []
        if has_variations:
            for _, vr in variation_rows.iterrows():
                v_sku = vr.get(sku_col, "") if sku_col else ""
                v_price = vr.get(price_col, p_price)
                v_sale = vr.get(sale_price_col, p_sale)
                v_stock = vr.get(stock_col, p_stock)
                v_images = split_images(vr.get(images_col, "")) or p_images
                # attempt to find option values in variation row (common columns)
                option1 = vr.get('Attribute 1 value') if 'Attribute 1 value' in vr.index else None
                # generic strategy: collect any attribute columns present
                option_vals = []
                for c in vr.index:
                    if re.search(r'attribute.*value|valore.*attributo|attributo.*valore', str(c), re.IGNORECASE):
                        option_vals.append(str(vr.get(c)))
                variants.append({
                    'sku': v_sku,
                    'price': v_price,
                    'compare_at_price': v_sale,
                    'stock': v_stock,
                    'images': v_images,
                    'options': option_vals
                })
        else:
            # No explicit variation rows -> prova a leggere attributi della master row
            # Cerca colonne che definiscono attributo name/value
            attr_names = []
            attr_values = []
            # Strategy: find columns like "Attribute 1 name" and "Attribute 1 value" or "Nome dell'attributo 1" / "Valore dell'attributo 1"
            for i in range(1,6):
                name_candidates = [f'Attribute {i} name', f'Nome dell\'attributo {i}', f'Nome attributo {i}']
                value_candidates = [f'Attribute {i} value', f'Valore dell\'attributo {i}', f'Attribute {i}', f'Attributo {i}']
                found_name = None
                found_value = None
                for nc in name_candidates:
                    if nc in master.index and str(master.get(nc)).strip():
                        found_name = str(master.get(nc)).strip()
                        break
                for vc in value_candidates:
                    if vc in master.index and str(master.get(vc)).strip():
                        found_value = str(master.get(vc)).strip()
                        break
                if found_name and found_value:
                    attr_names.append(found_name)
                    attr_values.append(parse_attribute_values(found_value))

            if attr_values:
                # crea tutte le combinazioni (product)
                combos = list(product(*attr_values))
                for combo in combos:
                    opt_list = list(combo)
                    variants.append({
                        'sku': "",  # SKU in genere a livello variante può non esserci nel csv master
                        'price': p_price,
                        'compare_at_price': p_sale,
                        'stock': p_stock,
                        'images': p_images,
                        'options': opt_list
                    })
            else:
                # Prodotto semplice -> una sola variante
                variants.append({
                    'sku': p_sku or "",
                    'price': p_price,
                    'compare_at_price': p_sale,
                    'stock': p_stock,
                    'images': p_images,
                    'options': []
                })

        # Determina option names (Option1/2/3) basandoci su attr_names trovati (se esistono), altrimenti generici
        option_names = []
        if attr_names:
            option_names = attr_names[:3]
        else:
            # se abbiamo varianti con options content >0, proviamo a dedurre nomi generici
            max_opts = max((len(v['options']) for v in variants), default=0)
            for i in range(max_opts):
                option_names.append(f"Option{i+1}")

        # Costruzione righe Shopify
        first_variant = True
        for v in variants:
            row = {
                "Handle": p_handle,
                "Title": p_title if first_variant else "",
                "Body (HTML)": p_desc if first_variant else "",
                "Vendor": p_vendor if first_variant else "",
                "Tags": p_tags if first_variant else "",
                "Option1 Name": option_names[0] if len(option_names) > 0 else "",
                "Option1 Value": v['options'][0] if len(v['options']) > 0 else (p_title if not v['options'] else ""),
                "Option2 Name": option_names[1] if len(option_names) > 1 else "",
                "Option2 Value": v['options'][1] if len(v['options']) > 1 else "",
                "Option3 Name": option_names[2] if len(option_names) > 2 else "",
                "Option3 Value": v['options'][2] if len(v['options']) > 2 else "",
                "Variant SKU": v.get('sku', ""),
                "Variant Price": v.get('price', ""),
                "Variant Compare At Price": v.get('compare_at_price', ""),
                "Variant Inventory Qty": v.get('stock', ""),
                "Image Src": v['images'][0] if v['images'] else (p_images[0] if p_images else ""),
                "Image Position": 1 if v['images'] else (1 if p_images else ""),
                "Published": master.get('Published', master.get('Status', '')) if first_variant else ""
            }
            out_rows.append(row)
            first_variant = False

        # Aggiungi righe immagine extra (solo Handle e Image Src) per immagini aggiuntive oltre la prima
        if p_images and len(p_images) > 1:
            for pos, img in enumerate(p_images[1:], start=2):
                img_row = {
                    "Handle": p_handle,
                    "Title": "", "Body (HTML)": "", "Vendor": "", "Tags": "",
                    "Option1 Name": "", "Option1 Value": "",
                    "Option2 Name": "", "Option2 Value": "",
                    "Option3 Name": "", "Option3 Value": "",
                    "Variant SKU": "", "Variant Price": "", "Variant Compare At Price": "",
                    "Variant Inventory Qty": "", "Image Src": img, "Image Position": pos,
                    "Published": ""
                }
                out_rows.append(img_row)

    # Costruisci DataFrame finale con colonne nell'ordine richiesto da Shopify
    columns = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Tags",
        "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value", "Option3 Name", "Option3 Value",
        "Variant SKU", "Variant Price", "Variant Compare At Price", "Variant Inventory Qty",
        "Image Src", "Image Position", "Published"
    ]
    out_df = pd.DataFrame(out_rows, columns=columns)
    # Pulizia finale: sostituisci NaN con stringa vuota
    out_df = out_df.fillna("")
    return out_df


def convert_woocommerce_csv_path_to_shopify_csv(input_csv_path: str, output_csv_path: str):
    df = pd.read_csv(input_csv_path, dtype=str, keep_default_na=False)
    shopify_df = convert_woocommerce_df_to_shopify(df)
    # Shopify vuole UTF-8 senza BOM
    shopify_df.to_csv(output_csv_path, index=False, encoding='utf-8')
    return output_csv_path