import pandas as pd
import re
import unicodedata
from itertools import product
from typing import Dict, List, Any

def clean_html(text):
    """Rimuove tag HTML e pulisce il testo"""
    if pd.isna(text) or text == "":
        return ""
    text = str(text)
    # Rimuove tag HTML
    text = re.sub(r'<[^>]+>', '', text)
    # Decodifica HTML entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&#039;', "'").replace('&quot;', '"')
    text = text.replace('\n', ' ').replace('\r', '')
    return text.strip()

def generate_handle(title):
    """Genera un handle Shopify valido dal titolo"""
    if pd.isna(title) or title == "":
        return ""
    handle = str(title).lower()
    # Rimuove caratteri speciali
    handle = re.sub(r'[^a-z0-9\s-]', '', handle)
    # Sostituisce spazi con trattini
    handle = re.sub(r'\s+', '-', handle)
    # Rimuove trattini multipli
    handle = re.sub(r'-+', '-', handle)
    # Rimuove trattini iniziali/finali
    handle = handle.strip('-')
    return handle

def parse_images(image_string):
    """Converte stringa immagini in lista"""
    if pd.isna(image_string) or image_string == "":
        return []
    images = str(image_string).split(',')
    return [img.strip() for img in images if img.strip()]

def get_variant_options(row):
    """Estrae le opzioni varianti da attributi"""
    options = {}
    
    # Cerca attributi fino a 3 (puoi estendere)
    for i in range(1, 4):
        attr_name_col = f"Nome dell'attributo {i}"
        attr_value_col = f"Valore dell'attributo {i}"
        
        if attr_name_col in row and not pd.isna(row[attr_name_col]):
            name = str(row[attr_name_col]).strip()
            value = str(row[attr_value_col]).strip() if attr_value_col in row else ""
            
            if name and value:
                options[f"Option{i} Name"] = name
                options[f"Option{i} Value"] = value
    
    return options

def transform_woocommerce_to_shopify(df):
    """
    Trasforma un DataFrame WooCommerce in formato Shopify
    
    Input: DataFrame con struttura WooCommerce
    Output: DataFrame con struttura Shopify
    """
    
    shopify_products = []
    
    # Raggruppa prodotti variabili con le loro varianti
    # Prodotti semplici hanno Tipo = "simple"
    # Prodotti variabili hanno Tipo = "variable" (padre) e varianti con Genitore != NaN
    
    # Prima passiamo sui prodotti padre (simple o variable)
    parent_products = df[df['Genitore'].isna() | (df['Genitore'] == '')]
    
    for idx, row in parent_products.iterrows():
        product_id = row['ID']
        product_type = row['Tipo']
        
        # Dati base del prodotto
        title = str(row['Nome']) if not pd.isna(row['Nome']) else ""
        handle = generate_handle(title)
        body_html = str(row['Descrizione']) if not pd.isna(row['Descrizione']) else ""
        vendor = "Default"  # Puoi personalizzare
        
        # Categorie → Collections
        categories = str(row['Categorie']) if not pd.isna(row['Categorie']) else ""
        collections = categories  # Shopify usa le collections
        
        # Product Type (prima categoria)
        product_type_value = categories.split(',')[0].strip() if categories else ""
        
        # Tags
        tags = str(row['Tag']) if not pd.isna(row['Tag']) else ""
        
        # Immagini
        images = parse_images(row['Immagine'])
        main_image = images[0] if images else ""
        
        # Prezzo
        regular_price = str(row['Prezzo di listino']) if not pd.isna(row['Prezzo di listino']) else "0"
        sale_price = str(row['Prezzo in offerta']) if not pd.isna(row['Prezzo in offerta']) else ""
        
        # Se c'è sale price, quello è il prezzo e regular diventa compare-at
        if sale_price:
            variant_price = sale_price
            compare_at_price = regular_price
        else:
            variant_price = regular_price
            compare_at_price = ""
        
        # Stock
        stock_status = str(row['In stock?']) if not pd.isna(row['In stock?']) else "0"
        stock_quantity = str(row['Quantità in magazzino']) if not pd.isna(row['Quantità in magazzino']) else "0"
        
        # Inventory policy
        inventory_policy = "deny"  # Non vendere se out of stock
        if stock_status == "1" or int(stock_quantity) > 0:
            inventory_tracker = "shopify"
        else:
            inventory_tracker = ""
            stock_quantity = "0"
        
        # SKU
        sku = str(row['SKU']) if not pd.isna(row['SKU']) else ""
        
        if product_type == "simple" or product_type == "simple, virtual":
            # Prodotto semplice - una sola riga
            shopify_row = {
                "Handle": handle,
                "Title": title,
                "Body (HTML)": body_html,
                "Vendor": vendor,
                "Product Category": collections,
                "Type": product_type_value,
                "Tags": tags,
                "Published": "TRUE",
                "Option1 Name": "Title",
                "Option1 Value": "Default Title",
                "Option2 Name": "",
                "Option2 Value": "",
                "Option3 Name": "",
                "Option3 Value": "",
                "Variant SKU": sku,
                "Variant Grams": "",
                "Variant Inventory Tracker": inventory_tracker,
                "Variant Inventory Qty": stock_quantity,
                "Variant Inventory Policy": inventory_policy,
                "Variant Fulfillment Service": "manual",
                "Variant Price": variant_price,
                "Variant Compare At Price": compare_at_price,
                "Variant Requires Shipping": "TRUE",
                "Variant Taxable": "TRUE",
                "Variant Barcode": "",
                "Image Src": main_image,
                "Image Position": "1",
                "Image Alt Text": "",
                "Gift Card": "FALSE",
                "SEO Title": "",
                "SEO Description": "",
                "Google Shopping / Google Product Category": "",
                "Google Shopping / Gender": "",
                "Google Shopping / Age Group": "",
                "Google Shopping / MPN": "",
                "Google Shopping / AdWords Grouping": "",
                "Google Shopping / AdWords Labels": "",
                "Google Shopping / Condition": "",
                "Google Shopping / Custom Product": "",
                "Google Shopping / Custom Label 0": "",
                "Google Shopping / Custom Label 1": "",
                "Google Shopping / Custom Label 2": "",
                "Google Shopping / Custom Label 3": "",
                "Google Shopping / Custom Label 4": "",
                "Variant Image": main_image,
                "Variant Weight Unit": "kg",
                "Variant Tax Code": "",
                "Cost per item": "",
                "Status": "active"
            }
            
            shopify_products.append(shopify_row)
            
            # Aggiungi altre immagini come righe separate
            for i, img_url in enumerate(images[1:], start=2):
                img_row = shopify_row.copy()
                img_row["Title"] = ""
                img_row["Body (HTML)"] = ""
                img_row["Vendor"] = ""
                img_row["Product Category"] = ""
                img_row["Type"] = ""
                img_row["Tags"] = ""
                img_row["Option1 Name"] = ""
                img_row["Option1 Value"] = ""
                img_row["Variant SKU"] = ""
                img_row["Variant Price"] = ""
                img_row["Image Src"] = img_url
                img_row["Image Position"] = str(i)
                shopify_products.append(img_row)
        
        elif product_type == "variable":
            # Prodotto variabile - cerca tutte le varianti
            variants = df[df['Genitore'] == product_id]
            
            if len(variants) == 0:
                # Se non ha varianti, trattalo come semplice
                continue
            
            first_variant = True
            image_position = 1
            
            for var_idx, var_row in variants.iterrows():
                # Estrai opzioni variante
                variant_options = get_variant_options(var_row)
                
                # SKU variante
                var_sku = str(var_row['SKU']) if not pd.isna(var_row['SKU']) else ""
                
                # Prezzo variante
                var_regular_price = str(var_row['Prezzo di listino']) if not pd.isna(var_row['Prezzo di listino']) else "0"
                var_sale_price = str(var_row['Prezzo in offerta']) if not pd.isna(var_row['Prezzo in offerta']) else ""
                
                if var_sale_price:
                    var_variant_price = var_sale_price
                    var_compare_at = var_regular_price
                else:
                    var_variant_price = var_regular_price
                    var_compare_at = ""
                
                # Stock variante
                var_stock_status = str(var_row['In stock?']) if not pd.isna(var_row['In stock?']) else "0"
                var_stock_qty = str(var_row['Quantità in magazzino']) if not pd.isna(var_row['Quantità in magazzino']) else "0"
                
                if var_stock_status == "1" or int(var_stock_qty) > 0:
                    var_inventory_tracker = "shopify"
                else:
                    var_inventory_tracker = ""
                    var_stock_qty = "0"
                
                # Immagine variante
                var_images = parse_images(var_row['Immagine'])
                var_main_image = var_images[0] if var_images else main_image
                
                shopify_row = {
                    "Handle": handle,
                    "Title": title if first_variant else "",
                    "Body (HTML)": body_html if first_variant else "",
                    "Vendor": vendor if first_variant else "",
                    "Product Category": collections if first_variant else "",
                    "Type": product_type_value if first_variant else "",
                    "Tags": tags if first_variant else "",
                    "Published": "TRUE" if first_variant else "",
                    "Option1 Name": variant_options.get("Option1 Name", ""),
                    "Option1 Value": variant_options.get("Option1 Value", ""),
                    "Option2 Name": variant_options.get("Option2 Name", ""),
                    "Option2 Value": variant_options.get("Option2 Value", ""),
                    "Option3 Name": variant_options.get("Option3 Name", ""),
                    "Option3 Value": variant_options.get("Option3 Value", ""),
                    "Variant SKU": var_sku,
                    "Variant Grams": "",
                    "Variant Inventory Tracker": var_inventory_tracker,
                    "Variant Inventory Qty": var_stock_qty,
                    "Variant Inventory Policy": inventory_policy,
                    "Variant Fulfillment Service": "manual",
                    "Variant Price": var_variant_price,
                    "Variant Compare At Price": var_compare_at,
                    "Variant Requires Shipping": "TRUE",
                    "Variant Taxable": "TRUE",
                    "Variant Barcode": "",
                    "Image Src": main_image if first_variant else "",
                    "Image Position": str(image_position) if first_variant else "",
                    "Image Alt Text": "",
                    "Gift Card": "FALSE",
                    "SEO Title": "",
                    "SEO Description": "",
                    "Google Shopping / Google Product Category": "",
                    "Google Shopping / Gender": "",
                    "Google Shopping / Age Group": "",
                    "Google Shopping / MPN": "",
                    "Google Shopping / AdWords Grouping": "",
                    "Google Shopping / AdWords Labels": "",
                    "Google Shopping / Condition": "",
                    "Google Shopping / Custom Product": "",
                    "Google Shopping / Custom Label 0": "",
                    "Google Shopping / Custom Label 1": "",
                    "Google Shopping / Custom Label 2": "",
                    "Google Shopping / Custom Label 3": "",
                    "Google Shopping / Custom Label 4": "",
                    "Variant Image": var_main_image,
                    "Variant Weight Unit": "kg",
                    "Variant Tax Code": "",
                    "Cost per item": "",
                    "Status": "active"
                }
                
                shopify_products.append(shopify_row)
                first_variant = False
                image_position += 1
                
                # Aggiungi immagini aggiuntive della variante
                for img_url in var_images[1:]:
                    img_row = shopify_row.copy()
                    img_row["Title"] = ""
                    img_row["Image Src"] = img_url
                    img_row["Image Position"] = str(image_position)
                    img_row["Variant Image"] = ""
                    shopify_products.append(img_row)
                    image_position += 1
    
    return pd.DataFrame(shopify_products)

def transform_product(source_platform, product):
    """
    Funzione principale chiamata da app.py
    
    Args:
        source_platform: "woocommerce", "wix", o "prestashop"
        product: dict o DataFrame con i dati del prodotto
    
    Returns:
        dict o DataFrame trasformato per Shopify
    """
    
    if source_platform == "woocommerce":
        # Se product è un dict, convertilo in DataFrame
        if isinstance(product, dict):
            df = pd.DataFrame([product])
        elif isinstance(product, pd.DataFrame):
            df = product
        else:
            return {"error": "Formato prodotto non valido"}
        
        # Trasforma
        shopify_df = transform_woocommerce_to_shopify(df)
        
        # Ritorna come lista di dict
        return shopify_df.to_dict(orient='records')
    
    elif source_platform == "wix":
        # TODO: Implementare trasformazione Wix
        return {"error": "Wix non ancora implementato"}
    
    elif source_platform == "prestashop":
        # TODO: Implementare trasformazione Prestashop
        return {"error": "Prestashop non ancora implementato"}
    
    else:
        return {"error": f"Piattaforma {source_platform} non supportata"}

def convert_woocommerce_csv_path_to_shopify_csv(input_path: str, output_path: str, **read_csv_kwargs):
    """
    Legge un CSV WooCommerce da `input_path`, lo trasforma in formato Shopify
    e scrive il CSV risultante in `output_path`.
    """
    # Leggi CSV (passa kwargs se serve encoding, sep, etc.)
    df = pd.read_csv(input_path, **read_csv_kwargs)

    # Trasforma con la funzione esistente
    shopify_df = transform_woocommerce_to_shopify(df)

    # Salva il CSV pronto per Shopify
    shopify_df.to_csv(output_path, index=False, encoding='utf-8')