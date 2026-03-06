import streamlit as st
import pandas as pd
import numpy as np
import re
import io
from datetime import datetime
import plotly.express as px
import asyncio
from check_status_codes import analyze_status_codes

# ----------------- KONFIGURACJA STRONY -----------------
st.set_page_config(page_title="Audyt SEO - Streamlit App", page_icon="📈", layout="wide")

# ----------------- AUTORYZACJA -------------------------
def check_password():
    """Zwraca True, jeśli użytkownik wpisał poprawne hasło."""
    def password_entered():
        if st.session_state["password"] == st.secrets.get("passwords", {}).get("admin_password", "admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Sprawdzenie uprawnień. Podaj hasło:", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Sprawdzenie uprawnień. Podaj hasło:", type="password", on_change=password_entered, key="password")
        st.error("😕 Błędne hasło!")
        return False
    return True

if not check_password():
    st.stop()  # Zatrzymuje działanie aplikacji, dopóki hasło nie będzie poprawne.

# ----------------- FUNKCJE POMOCNICZE ------------------
@st.cache_data
def smart_load_gkp_bytes(file_bytes, filename=""):
    if filename.endswith('.xlsx'):
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
            if df.shape[1] > 2: return df
        except:
            pass

    encodings = ['utf-16', 'utf-8', 'cp1250', 'latin1']
    seps = ['\t', ',', ';']
    content = None
    
    for enc in encodings:
        try:
            content = file_bytes.decode(enc).split('\n')
            break
        except:
             continue
                
    if not content: return None
    
    header_row = 0
    for i, line in enumerate(content[:25]):
        if 'Keyword' in line or 'Słowo' in line or 'Currency' in line or 'Fraza' in line:
            header_row = i; break
            
    for sep in seps:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc, sep=sep, skiprows=header_row, engine='python')
            if df.shape[1] > 2: return df
        except:
            continue
    return None

def clean_money(series):
    return pd.to_numeric(series.astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce').fillna(0)

def detect_missing_data(df):
    return (df['Clicks_Curr'] == 0) & (df['Impr_Curr'] == 0) & (df['Pos_Curr'].fillna(0) == 0)

def process_gsc_sheet(df, type_col_name='Query'):
    df.columns = df.columns.str.strip()
    new_map = {}
    
    def get_year(c): 
        match = re.search(r'202\d|203\d', str(c))
        return int(match.group(0)) if match else 0
    
    patterns = {'Clicks': ['Kliknięcia', 'Clicks'], 'Impr': ['Wyświetlenia', 'Impressions'], 'Pos': ['Pozycja', 'Position'], 'CTR': ['CTR']}
    
    for metric, keys in patterns.items():
        cands = [c for c in df.columns if any(k in c for k in keys) and 'różnic' not in str(c).lower() and 'diff' not in str(c).lower() and 'roznica' not in str(c).lower()]
        if len(cands) >= 2:
            cands.sort(key=get_year)
            # Zawsze przedostatni to poprzedni rok, a ostatni to aktualny rok
            new_map[cands[-2]] = f'{metric}_Prev'
            new_map[cands[-1]] = f'{metric}_Curr'
    
    if type_col_name == 'Query':
        k_col = next((c for c in df.columns if c in ['Najczęstsze zapytania', 'Query', 'Top queries']), df.columns[0])
    else:
        k_col = next((c for c in df.columns if c in ['Najczęstsze strony', 'Page', 'Top pages', 'Strona']), df.columns[0])
        
    new_map[k_col] = 'KeyItem'
    df = df.rename(columns=new_map).copy()
    
    for c in [x for x in df.columns if '_Prev' in x or '_Curr' in x]:
        if 'CTR' in c:
            df[c] = df[c].astype(str).str.replace(',', '.', regex=False).str.replace('%', '', regex=False).str.strip()
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
            if df[c].max() > 1.0:
                df[c] = df[c] / 100.0
        else:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    df['Diff_Clicks'] = df['Clicks_Curr'] - df['Clicks_Prev']
    df['Diff_Pos'] = df.get('Pos_Curr', 0) - df.get('Pos_Prev', 0)
    df['Diff_Impr'] = df.get('Impr_Curr', 0) - df.get('Impr_Prev', 0)
    df['Diff_CTR'] = df.get('CTR_Curr', 0) - df.get('CTR_Prev', 0)
    
    return df

def generate_ui_dataframe(df_orig, type_name="Query"):
    df = df_orig.sort_values(by="Diff_Clicks", ascending=True).copy()
    mask = detect_missing_data(df)

    col_map = {
        'Query': 'Fraza', 'URL': 'Adres URL (GSC)',
        'Diagnosis': 'Diagnoza',
        'Type': 'Typ (Brand/Generic)',
        'Pos_Prev': 'Poprzednia pozycja (GSC)', 'Pos_Curr': 'Aktualna pozycja (GSC)', 'Diff_Pos': 'Różnica pozycji (GSC)',
        'Clicks_Prev': 'Poprzednie kliknięcia (GSC)', 'Clicks_Curr': 'Aktualne kliknięcia (GSC)', 'Diff_Clicks': 'Różnica kliknięć (GSC)',
        'Impr_Prev': 'Poprzednie wyświetlenia (GSC)', 'Impr_Curr': 'Aktualne wyświetlenia (GSC)', 'Diff_Impr': 'Różnica wyśw. (GSC)',
        'CTR_Prev': 'Poprzedni CTR (GSC)', 'CTR_Curr': 'Aktualny CTR (GSC)', 'Diff_CTR': 'Różnica CTR (GSC)',
        'GKP_Vol_Prev': 'Popyt Poprz. (GKP)', 'GKP_Vol_Curr': 'Popyt Akt. (GKP)', 
        'Ah_Traff_Prev': 'Ruch Poprz. (Ahrefs)', 'Ah_Traff_Curr': 'Ruch Akt. (Ahrefs)', 'Ah_Pos_Prev': 'Poz. Poprz. (Ahrefs)', 'Ah_Pos_Curr': 'Poz. Akt. (Ahrefs)',
        'Ah_URL_Prev': 'Poprzedni URL (Ahrefs)', 'Ah_URL_Curr': 'Aktualny URL (Ahrefs)',
        'Status_Code': 'Status Code'
    }
    
    df = df.rename(columns=col_map)
    
    ordered_cols = []
    if type_name == "Query":
        ordered_cols.extend(["Fraza", "Typ (Brand/Generic)"])
        if 'Status Code' in df.columns: ordered_cols.append("Status Code")
    else:
        ordered_cols.append("Adres URL (GSC)")
        if 'Status Code' in df.columns: ordered_cols.append("Status Code")
        
    bases = ['Diagnoza', 'Różnica kliknięć (GSC)', 'Poprzednie kliknięcia (GSC)', 'Aktualne kliknięcia (GSC)', 
             'Różnica pozycji (GSC)', 'Poprzednia pozycja (GSC)', 'Aktualna pozycja (GSC)', 
             'Różnica wyśw. (GSC)', 'Poprzednie wyświetlenia (GSC)', 'Aktualne wyświetlenia (GSC)', 
             'Różnica CTR (GSC)', 'Poprzedni CTR (GSC)', 'Aktualny CTR (GSC)']
             
    for c in ['Popyt Poprz. (GKP)', 'Popyt Akt. (GKP)', 'Ruch Poprz. (Ahrefs)', 'Ruch Akt. (Ahrefs)', 'Poz. Poprz. (Ahrefs)', 'Poz. Akt. (Ahrefs)', 'Poprzedni URL (Ahrefs)', 'Aktualny URL (Ahrefs)']:
        if c in df.columns:
            bases.append(c)
            
    final_cols = [c for c in ordered_cols + bases if c in df.columns]
    df_out = df[final_cols].copy()
    
    # Round all numerics to 4 decimals
    for col in df_out.select_dtypes(include=['float64']):
        df_out[col] = df_out[col].round(4)
        
    return df_out
    
def generate_html_report(df, df_pages, df_loss, df_growth, fig1, fig2, fig_brand, fig_ah, fig3, ui_gkp, fig_ctr, ui_df_queries, ui_df_pages):
    total_diff = df_pages['Diff_Clicks'].sum() if df_pages is not None else df['Diff_Clicks'].sum()
    loss_sum = df_pages[(df_pages['Diff_Clicks'] < 0)]['Diff_Clicks'].sum() if df_pages is not None else df_loss['Diff_Clicks'].sum()
    
    html = f"""
    <!DOCTYPE html>
    <html lang="pl">
    <head>
        <meta charset="utf-8">
        <title>Audyt SEO - Pełny Raport</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: 'Helvetica', sans-serif; background: #F5F7FA; margin: 0; padding: 20px; color: #333; }}
            .container {{ max-width: 1400px; margin: auto; background: white; padding: 40px; box-shadow: 0 5px 20px rgba(0,0,0,0.05); border-radius: 8px; }}
            h1 {{ color: #003366; border-bottom: 4px solid #006699; padding-bottom: 10px; }}
            h2 {{ color: #006699; margin-top: 50px; border-left: 6px solid #FF9900; padding-left: 15px; background: #f8f9fa; padding-top:10px; padding-bottom:10px; }}
            h3 {{ color: #7f8c8d; margin-top: 30px; }}
            
            .dict-box {{ background: #e8f6f3; border: 1px solid #d1f2eb; padding: 20px; border-radius: 8px; margin-bottom: 30px; }}
            .dict-title {{ font-weight: bold; color: #006699; font-size: 18px; margin-bottom: 10px; }}
            .dict-list dt {{ font-weight: bold; color: #003366; margin-top: 10px; }}
            .dict-list dd {{ margin-left: 20px; margin-bottom: 5px; color: #555; }}

            .kpi-container {{ display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 30px; }}
            .kpi {{ flex: 1; background: #fff; border: 1px solid #ddd; padding: 20px; text-align: center; border-radius: 8px; min-width: 200px; }}
            .kpi-val {{ font-size: 32px; font-weight: bold; margin: 10px 0; }}
            .red {{ color: #c0392b; }} .green {{ color: #27ae60; }}

            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 13px; }}
            th {{ background: #ecf0f1; padding: 12px; text-align: left; cursor: pointer; border-bottom: 2px solid #bdc3c7; }}
            th:hover {{ background: #d5dbdb; }}
            td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; }}
            tr:hover {{ background: #fdfefe; }}
            .num {{ text-align: right; font-family: monospace; }}
            
            .section-desc {{ font-style: italic; color: #7f8c8d; margin-bottom: 15px; }}
        </style>
        <script>
        function sortTable(n, tableId) {{
          var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
          table = document.getElementById(tableId);
          switching = true;
          dir = "asc"; 
          while (switching) {{
            switching = false;
            rows = table.rows;
            for (i = 1; i < (rows.length - 1); i++) {{
              shouldSwitch = false;
              x = rows[i].getElementsByTagName("TD")[n];
              y = rows[i + 1].getElementsByTagName("TD")[n];
              var xContent = x.innerText.replace(/[^0-9.-]/g, '');
              var yContent = y.innerText.replace(/[^0-9.-]/g, '');
              var isNum = !isNaN(parseFloat(xContent)) && xContent !== "";
              
              if (dir == "asc") {{
                if (isNum ? (parseFloat(xContent) > parseFloat(yContent)) : (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase())) {{
                  shouldSwitch = true; break;
                }}
              }} else if (dir == "desc") {{
                if (isNum ? (parseFloat(xContent) < parseFloat(yContent)) : (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase())) {{
                  shouldSwitch = true; break;
                }}
              }}
            }}
            if (shouldSwitch) {{
              rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
              switching = true;
              switchcount ++;      
            }} else {{
              if (switchcount == 0 && dir == "asc") {{ dir = "desc"; switching = true; }}
            }}
          }}
        }}
        </script>
    </head>
    <body>
        <div class="container">
            <h1>📊 Audyt SEO - Raport Kompleksowy (PDF / HTML)</h1>
            <p>Data wygenerowania: {datetime.now().strftime('%d.%m.%Y')}</p>

            <div class="kpi-container">
                <div class="kpi"><h3>Bilans Całkowity (GSC)</h3><div class="kpi-val {'green' if total_diff>=0 else 'red'}">{int(total_diff):+}</div></div>
                <div class="kpi"><h3>Utracone Kliknięcia (GSC)</h3><div class="kpi-val red">{int(loss_sum)}</div></div>
                <div class="kpi"><h3>Liczba Fraz ze spadkiem (GSC)</h3><div class="kpi-val">{len(df_loss)}</div></div>
            </div>

            <div class="dict-box">
                <div class="dict-title">📚 Słownik Pojęć i Diagnoz</div>
                <dl class="dict-list">
                    <dt>Spadek Pozycji (GSC)</dt><dd>Strona spadła w wynikach Google. Wymaga audytu treści i linków.</dd>
                    <dt>Spadek Pozycji (Ahrefs)</dt><dd>Wykryto drastyczny spadek na pozycjach estymowanych przez algorytm Ahrefs (obiektywny problem).</dd>
                    <dt>Spadek Wyświetleń (GSC)</dt><dd>Odnotowano fizycznie mniejszą widoczność wyniku (ilość wyświetleń) u użytkowników.</dd>
                    <dt>Spadek CTR (GSC)</dt><dd>Gorsza klikalność wyniku na podobnych pozycjach (np. weszła nowa funkcja SERP na górę strony, osłabiając widoczność).</dd>
                    <dt>Spadek Popytu (GKP)</dt><dd>Keyword Planner potwierdza spadek liczby wyszukiwań na rynku o ponad 15%. Problem niezależny od SEO.</dd>
                    <dt>Kanibalizacja URL (Ahrefs/GSC)</dt><dd>Google zmieniło przypisany adres URL wyświetlany dla tej frazy, co spowodowało spadek w wyliczeniach.</dd>
                    <dt>Utrata Widoczności (GSC)</dt><dd>Całkowite zniknięcie frazy z wyników (0 kliknięć, 0 wyświetleń na aktualny okres).</dd>
                    <dt>Brak Danych (GSC)</dt><dd>Strona wygasła/usunięta (odfiltrowane z podsumowań twardych spadków).</dd>
                    <dt>Kod 3xx (Przekierowanie)</dt><dd>Adres URL odpowiada kodem z grupy 3xx, co może powodować wahania widoczności po redesignie/migracji.</dd>
                    <dt>Kod 4xx (Błąd Strony)</dt><dd>Adres URL został trwale usunięty lub jest popsuty (np. 404), powodując usuwanie go przez Google z indeksu.</dd>
                    <dt>Kod 5xx (Błąd Serwera)</dt><dd>Brak możliwości nawiązania połączenia przez przeciążenie serwera, błąd bazy danych itd.</dd>
                </dl>
            </div>

            <h2>1. Analiza Fraz (GSC Queries)</h2>
            <div id="fig1">{fig1.to_html(full_html=False, include_plotlyjs='cdn') if fig1 else ''}</div>
            <div id="fig2">{fig2.to_html(full_html=False, include_plotlyjs='cdn') if fig2 else ''}</div>
    """

    html += "<h2>2. Analiza Adresów URL (GSC Pages)</h2>"
    if df_pages is not None and ui_df_pages is not None:
        temp_df = ui_df_pages.copy()
        temp_df['sort_key'] = pd.to_numeric(temp_df['Różnica kliknięć (GSC)'], errors='coerce').fillna(0)
        top_urls = temp_df.sort_values('sort_key').head(50)
        
        has_status = 'Status Code' in temp_df.columns
        status_th = """<th onclick="sortTable(1, 'tbl_url')">Status Code</th>""" if has_status else ""
        
        html += f"""<table id="tbl_url"><thead><tr>
            <th onclick="sortTable(0, 'tbl_url')">Adres URL</th>
            {status_th}
            <th onclick="sortTable(2, 'tbl_url')" class="num">Strata Kliknięć</th>
            <th onclick="sortTable(3, 'tbl_url')" class="num">Zmiana Wyświetleń</th>
            <th onclick="sortTable(4, 'tbl_url')" class="num">Zmiana Pozycji</th>
            <th onclick="sortTable(5, 'tbl_url')" class="num">Zmiana CTR</th>
        </tr></thead><tbody>"""
        for _, r in top_urls.iterrows():
            clicks = r.get('Różnica kliknięć (GSC)', 0)
            impr = r.get('Różnica wyśw. (GSC)', 0)
            pos = r.get('Różnica pozycji (GSC)', 0)
            ctr = r.get('Różnica CTR (GSC)', 0)
            
            s_clicks = f"<b>{int(clicks)}</b>" if isinstance(clicks, (int, float)) else str(clicks)
            s_impr = str(int(impr)) if isinstance(impr, (int, float)) else str(impr)
            s_pos = f"{pos:.4f}" if isinstance(pos, (int, float)) else str(pos)
            s_ctr = f"{ctr*100:.4f}%" if isinstance(ctr, (int, float)) else str(ctr)
            
            status_td = f"<td>{str(r.get('Status Code', '')).replace('nan', '')}</td>" if has_status else ""

            html += f"""<tr>
                <td><a href="{r.get('Adres URL (GSC)', '')}" target="_blank">{str(r.get('Adres URL (GSC)', ''))[:80]}...</a></td>
                {status_td}
                <td class="num red">{s_clicks}</td>
                <td class="num">{s_impr}</td>
                <td class="num">{s_pos}</td>
                <td class="num">{s_ctr}</td>
            </tr>"""
        html += "</tbody></table>"
    else:
        html += "<p>Brak danych o stronach.</p>"

    html += "<h2>3. Analiza Popytu Rynkowego (Keyword Planner)</h2>"
    if fig3 or (ui_gkp is not None and not ui_gkp.empty):
        if fig3: html += f"<div>{fig3.to_html(full_html=False, include_plotlyjs='cdn')}</div>"
        if ui_gkp is not None and not ui_gkp.empty:
            html += """<h3>Top 50 Fraz ze stratą popytu (GKP)</h3><table id="tbl_gkp"><thead><tr>
                <th onclick="sortTable(0, 'tbl_gkp')">Fraza</th>
                <th onclick="sortTable(1, 'tbl_gkp')">Diagnoza</th>
                <th onclick="sortTable(2, 'tbl_gkp')" class="num">Strata Popytu (Num)</th>
                <th onclick="sortTable(3, 'tbl_gkp')" class="num">Strata Popytu (%)</th>
                <th onclick="sortTable(4, 'tbl_gkp')" class="num">Strata Kliknięć</th>
            </tr></thead><tbody>"""
            for _, r in ui_gkp.head(50).iterrows():
                html += f"""<tr>
                    <td><b>{r['Fraza']}</b></td>
                    <td>{r['Diagnoza']}</td>
                    <td class="num red"><b>{int(r['Strata Popytu Num (GKP)'])}</b></td>
                    <td class="num">{r['Strata Popytu % (GKP)']}</td>
                    <td class="num">{int(r['Strata Kliknięć (GSC)'])}</td>
                </tr>"""
            html += "</tbody></table>"
    else: 
        html += "<p>Brak danych GKP lub brak korelacji spadków.</p>"

    if fig_ah:
        html += "<h2>4. Analiza Konkurencji (Ahrefs)</h2>"
        html += f"<div>{fig_ah.to_html(full_html=False, include_plotlyjs='cdn')}</div>"

    if fig_ctr:
        html += "<h2>5. Krzywa CTR (GSC)</h2>"
        html += f"<div>{fig_ctr.to_html(full_html=False, include_plotlyjs='cdn')}</div>"

    html += """<h2>6. Szczegółowa Analiza Top 100 Spadków (GSC)</h2>
    <table id="tbl_main"><thead><tr>
        <th onclick="sortTable(0, 'tbl_main')">Fraza</th>
        <th onclick="sortTable(1, 'tbl_main')">Diagnoza</th>
        <th onclick="sortTable(2, 'tbl_main')" class="num">Strata (GSC)</th>
        <th onclick="sortTable(3, 'tbl_main')" class="num">Pozycja GSC (Prev->Curr)</th>
        <th onclick="sortTable(4, 'tbl_main')" class="num">Kliki GSC (Prev->Curr)</th>
    </tr></thead><tbody>"""
    
    top_100 = df_loss.sort_values('Diff_Clicks').head(100)
    for _, row in top_100.iterrows():
        s_code = str(row.get('Status_Code', '')).replace('nan', '').strip()
        diag = f"{s_code} | " if s_code else ""
        html += f"""<tr>
            <td><b>{row['Query']}</b></td>
            <td>{diag}{row['Diagnosis']}</td>
            <td class="num red"><b>{int(row['Diff_Clicks'])}</b></td>
            <td class="num">{row['Pos_Prev']:.4f} ➝ {row['Pos_Curr']:.4f}</td>
            <td class="num">{int(row['Clicks_Prev'])} ➝ {int(row['Clicks_Curr'])}</td>
        </tr>"""
        
    html += """</tbody></table>"""

    if df_growth is not None and not df_growth.empty:
        html += """<h2>7. Szczegółowa Analiza Top 100 Wzrostów (GSC)</h2>
        <table id="tbl_growth"><thead><tr>
            <th onclick="sortTable(0, 'tbl_growth')">Fraza</th>
            <th onclick="sortTable(1, 'tbl_growth')">Diagnoza</th>
            <th onclick="sortTable(2, 'tbl_growth')" class="num">Zysk (GSC)</th>
            <th onclick="sortTable(3, 'tbl_growth')" class="num">Pozycja GSC (Prev->Curr)</th>
            <th onclick="sortTable(4, 'tbl_growth')" class="num">Kliki GSC (Prev->Curr)</th>
        </tr></thead><tbody>"""
        
        top_100_g = df_growth.sort_values('Diff_Clicks', ascending=False).head(100)
        for _, row in top_100_g.iterrows():
            s_code = str(row.get('Status_Code', '')).replace('nan', '').strip()
            diag = f"{s_code} | " if s_code else ""
            html += f"""<tr>
                <td><b>{row['Query']}</b></td>
                <td>{diag}{row['Diagnosis']}</td>
                <td class="num green"><b>+{int(row['Diff_Clicks'])}</b></td>
                <td class="num">{row['Pos_Prev']:.4f} ➝ {row['Pos_Curr']:.4f}</td>
                <td class="num">{int(row['Clicks_Prev'])} ➝ {int(row['Clicks_Curr'])}</td>
            </tr>"""
        html += "</tbody></table>"

    html += """
    <div style="margin-top:50px; text-align:center; color:#999;">Wygenerowano z aplikacji Streamlit. Aby zapisać jako plik PDF na komputerze, wciśnij CTRL+P w przeglądarce i wybierz "Zapisz jako PDF".</div>
    </div></body></html>"""
    
    return html

def assign_multiple_diagnoses(df, mask_missing):
    diagnoses = pd.Series([[] for _ in range(len(df))], index=df.index)
    
    # Growth
    mask_wzrost = df['Diff_Clicks'] > 0
    diagnoses.loc[mask_wzrost] = diagnoses.loc[mask_wzrost].apply(lambda x: x + ["Wzrost (GSC)"])
    
    # Declines
    mask_spadek = df['Diff_Clicks'] < 0
    
    if 'Ah_URL_Changed' in df.columns:
        m1 = mask_spadek & (df['Ah_URL_Changed'] == True)
        diagnoses.loc[m1] = diagnoses.loc[m1].apply(lambda x: x + ["Kanibalizacja URL (Ahrefs/GSC)"])
        
    if 'GKP_Trend' in df.columns:
        m2 = mask_spadek & (df['GKP_Trend'] < -0.15)
        diagnoses.loc[m2] = diagnoses.loc[m2].apply(lambda x: x + ["Spadek Popytu (GKP)"])
        
    m3 = mask_spadek & (df['Clicks_Prev'] > 0) & (df['Clicks_Curr'] == 0)
    diagnoses.loc[m3] = diagnoses.loc[m3].apply(lambda x: x + ["Utrata Widoczności (GSC)"])
    
    m4 = mask_spadek & (df['Diff_Pos'] > 1.0)
    diagnoses.loc[m4] = diagnoses.loc[m4].apply(lambda x: x + ["Spadek Pozycji (GSC)"])
    
    if 'Ah_Diff_Pos' in df.columns:
        m4_ah = mask_spadek & (df['Ah_Diff_Pos'] >= 1.0)
        diagnoses.loc[m4_ah] = diagnoses.loc[m4_ah].apply(lambda x: x + ["Spadek Pozycji (Ahrefs)"])

    if 'Status_Code' in df.columns:
        m_3xx = (df['Status_Code'].astype(str).str.startswith('3'))
        diagnoses.loc[m_3xx] = diagnoses.loc[m_3xx].apply(lambda x: x + ["Kod 3xx (Przekierowanie)"])
        m_4xx = (df['Status_Code'].astype(str).str.startswith('4'))
        diagnoses.loc[m_4xx] = diagnoses.loc[m_4xx].apply(lambda x: x + ["Kod 4xx (Błąd Strony)"])
        m_5xx = (df['Status_Code'].astype(str).str.startswith('5'))
        diagnoses.loc[m_5xx] = diagnoses.loc[m_5xx].apply(lambda x: x + ["Kod 5xx (Błąd Serwera)"])

    m5 = mask_spadek & (df['Diff_Impr'] < 0)
    diagnoses.loc[m5] = diagnoses.loc[m5].apply(lambda x: x + ["Spadek Wyświetleń (GSC)"])
    
    m6 = mask_spadek & (df['Diff_CTR'] < 0)
    diagnoses.loc[m6] = diagnoses.loc[m6].apply(lambda x: x + ["Spadek CTR (GSC)"])
    
    # Missing data
    diagnoses.loc[mask_missing] = diagnoses.loc[mask_missing].apply(lambda x: x + ["Brak Danych (GSC)"])
    
    # No Change (only if no other diagnosis was found)
    mask_zero = (df['Diff_Clicks'] == 0) & ~mask_missing
    mask_zero_empty = mask_zero & (diagnoses.str.len() == 0)
    diagnoses.loc[mask_zero_empty] = diagnoses.loc[mask_zero_empty].apply(lambda x: x + ["Bez zmian (GSC)"])

    return diagnoses.apply(lambda x: ", ".join(x))


# ----------------- UI GŁÓWNE APLIKACJI -----------------
st.title("📈 Zaawansowany Audyt SEO i Analiza Spadków")

with st.expander("🛠️ Instrukcja pobierania danych i Słownik Pojęć", expanded=False):
    st.markdown("""
    ### 🛠️ Instrukcja pobierania danych
    1. **Google Search Console (GSC)**: Pobierz dane w języku PL z Google Search Console (porównaj np. podstrony zawierające `/category` - porównanie styczeń 2026 -> styczeń 2025).
    2. **Google Keyword Planner (GKP)**: Skopiuj wszystkie frazy z punktu 1 i wklej je w [https://data-center.space/app/keywordplanner](https://data-center.space/app/keywordplanner). Następnie pobierz wszystkie dane w okresie podanym w punkcie 1 (np. od stycznia 2024 do stycznia 2026).
    3. **Ahrefs**: Pobierz plik z Ahrefs stąd: [Link do Ahrefs dla mediamarkt.pl](https://app.ahrefs.com/v2-site-explorer/organic-keywords?brandedMode=all&chartGranularity=weekly&chartInterval=all&chartMetric=Keywords&compareDate=prevYear&country=pl&currentDate=2026-02-28&hiddenColumns=AllIntents%7C%7CCPC%7C%7CEntities%7C%7CKD%7C%7COtherIntents%7C%7CPaidTraffic%7C%7CPositionHistory%7C%7CSF%7C%7CUserIntents&intentsAttrs=&keywordRules=&languages=languageMatch%3A%5Ball%5D~~languageRules%3A%5BlangMatchType%3Ais%2Clangs%3Apl%2CmatchMode%3Aany%5D&limit=100&localMode=all&mainOnly=0&mode=subdomains&multipleUrlsOnly=0&offset=0&performanceChartTopPosition=top11_20%7C%7Ctop21_50%7C%7Ctop3%7C%7Ctop4_10%7C%7Ctop51&positionChanges=&positions=-20&sort=OrganicTrafficInitial&sortDirection=desc&target=mediamarkt.pl%2F&urlRules=&volume=10-&volume_type=average)
    
    ### 📚 Słownik Pojęć i Diagnoz
    - **Spadek Pozycji (GSC)**: Strona spadła w wynikach Google (np. z poz. 1 na 5). Wymaga audytu treści i linków.
    - **Spadek Pozycji (Ahrefs)**: Narzędzie zidentyfikowało spadek na swych niezależnych estymacjach rynkowych.
    - **Spadek Wyświetleń (GSC)**: Pozycja stabilna, ale mniej wyświetleń. Użytkownicy rzadziej wpisują hasło lokalnie na GSC.
    - **Spadek Popytu (GKP)**: Keyword Planner potwierdza spadek twardej liczby wyszukiwań na rynku o ponad 15%.
    - **Kanibalizacja URL (Ahrefs/GSC)**: Google zmieniło główną przypisaną podstronę, przez którą gubiony jest dawny ruch.
    - **Utrata Widoczności (GSC)**: Całkowite zniknięcie frazy z wyników (0 kliknięć, stare pozycje zanikły).
    - **Brak Danych (GSC)**: Strona najpewniej wygasła/usunięta ze sklepu (0 na wszystkich metrykach na podstronie aktualnie). Wyrzucana z analiz.
    """)

with st.sidebar:
    st.header("📂 1. Wgraj Pliki")
    file_gsc = st.file_uploader("Wgraj raport GSC (.xlsx)", type=['xlsx'])
    file_gkp = st.file_uploader("Wgraj raport GKP - Keyword Planner (.csv, opcjonalne)", type=['csv', 'xlsx'])
    file_ahrefs = st.file_uploader("Wgraj raport Ahrefs (.csv, opcjonalne)", type=['csv'])

    st.header("⚙️ 2. Konfiguracja")
    brand_cfg = st.text_input("Słowa Brandowe po przecinku (np. mediamarkt, media markt):", value="mediamarkt")
    gkp_prev_cfg = st.text_input("GKP Kolumna Poprzednia (np. Jan 2025):")
    gkp_curr_cfg = st.text_input("GKP Kolumna Aktualna (np. Jan 2026):")
    run_btn = st.button("🚀 URUCHOM ANALIZĘ", type="primary", use_container_width=True)

if run_btn:
    st.session_state['run_analysis'] = True
    st.session_state['full_df_extracted'] = False
    
if st.session_state.get('run_analysis', False):
    if not file_gsc:
        st.error("❌ Plik GSC jest wymagany do uruchomienia analizy!")
    else:
        # Extract base DFs silently if not done
        if not st.session_state.get('full_df_extracted', False):
            with st.spinner("⏳ Przetwarzanie i złączane danych ustrukturyzowanych..."):
                try:
                    gsc_bytes = file_gsc.getvalue()
                    
                    try: 
                        df = pd.read_excel(io.BytesIO(gsc_bytes), sheet_name='Zapytania', engine='openpyxl')
                    except:
                        try: df = pd.read_excel(io.BytesIO(gsc_bytes), sheet_name='Queries', engine='openpyxl')
                        except: df = pd.read_excel(io.BytesIO(gsc_bytes), sheet_name=0, engine='openpyxl')
                    
                    df = process_gsc_sheet(df, 'Query')
                    df = df.rename(columns={'KeyItem': 'Query'})

                    brands = [b.strip() for b in brand_cfg.split(',') if b.strip()]
                    if brands:
                        df['Type'] = np.where(df['Query'].astype(str).str.contains('|'.join(brands), case=False), 'Brand', 'Generic')
                    else:
                        df['Type'] = 'Generic'
                    
                    df['join_key'] = df['Query'].astype(str).str.strip().str.lower().replace(r'[^\w\s]', '', regex=True)

                    # ----------------- 2. GSC PAGES -----------------
                    df_pages = None
                    try: df_pages = pd.read_excel(io.BytesIO(gsc_bytes), sheet_name='Strony', engine='openpyxl')
                    except:
                        try: df_pages = pd.read_excel(io.BytesIO(gsc_bytes), sheet_name='Pages', engine='openpyxl')
                        except: pass

                    if df_pages is not None:
                        df_pages = process_gsc_sheet(df_pages, 'Page')
                        df_pages = df_pages.rename(columns={'KeyItem': 'URL'})

                    external_cols = ['GKP_Vol_Prev', 'GKP_Vol_Curr', 'GKP_Trend', 'Ah_Traff_Prev', 'Ah_Traff_Curr', 'Ah_Pos_Prev', 'Ah_Pos_Curr', 'Ah_URL_Changed', 'Ah_URL_Prev', 'Ah_URL_Curr', 'Ah_Diff_Traff', 'Ah_Diff_Pos']
                    for c in external_cols: df[c] = 0.0 if not 'URL' in c else None
                
                    # ----------------- 3. GKP MERGE -----------------
                    if file_gkp and gkp_prev_cfg and gkp_curr_cfg:
                        gkp_bytes = file_gkp.getvalue()
                        df_gkp = smart_load_gkp_bytes(gkp_bytes, file_gkp.name)
                        if df_gkp is not None:
                            col_prev = next((c for c in df_gkp.columns if gkp_prev_cfg in c), None)
                            col_curr = next((c for c in df_gkp.columns if gkp_curr_cfg in c), None)
                            if col_prev and col_curr:
                                df_gkp[col_prev] = clean_money(df_gkp[col_prev])
                                df_gkp[col_curr] = clean_money(df_gkp[col_curr])
                                k_col = next((c for c in df_gkp.columns if 'Keyword' in c or 'Słowo' in c), df_gkp.columns[0])
                                df_gkp['join_key'] = df_gkp[k_col].astype(str).str.strip().str.lower().replace(r'[^\w\s]', '', regex=True)
                            
                                df['GKP_Vol_Prev'] = df['join_key'].map(dict(zip(df_gkp['join_key'], df_gkp[col_prev]))).fillna(0)
                                df['GKP_Vol_Curr'] = df['join_key'].map(dict(zip(df_gkp['join_key'], df_gkp[col_curr]))).fillna(0)
                                df['GKP_Trend'] = np.where(df['GKP_Vol_Prev'] > 0, (df['GKP_Vol_Curr'] - df['GKP_Vol_Prev']) / df['GKP_Vol_Prev'], 0)

                    # ----------------- 4. AHREFS MERGE -----------------
                    if file_ahrefs:
                        ahrefs_bytes = file_ahrefs.getvalue()
                        try: df_ah = pd.read_csv(io.BytesIO(ahrefs_bytes), encoding='utf-8', sep=None, engine='python')
                        except: df_ah = pd.read_csv(io.BytesIO(ahrefs_bytes), encoding='utf-16', sep='\t', engine='python')
                        
                        if df_ah is not None:
                            col_kw = next((c for c in df_ah.columns if 'Keyword' in c), None)
                            if col_kw:
                                df_ah['join_key'] = df_ah[col_kw].astype(str).str.strip().str.lower()
                            
                                c_up = next((c for c in df_ah.columns if 'Previous URL' in c), None)
                                c_uc = next((c for c in df_ah.columns if 'Current URL' in c), None)
                                if c_up and c_uc:
                                    df['Ah_URL_Changed'] = df['join_key'].map(dict(zip(df_ah['join_key'], (df_ah[c_up] != df_ah[c_uc]) & df_ah[c_uc].notna()))).fillna(False)
                                    df['Ah_URL_Prev'] = df['join_key'].map(dict(zip(df_ah['join_key'], df_ah[c_up])))
                                    df['Ah_URL_Curr'] = df['join_key'].map(dict(zip(df_ah['join_key'], df_ah[c_uc])))
                                
                                c_tp = next((c for c in df_ah.columns if 'Previous organic traffic' in c), None)
                                c_tc = next((c for c in df_ah.columns if 'Current organic traffic' in c), None)
                                c_pp = next((c for c in df_ah.columns if 'Previous position' in c), None)
                                c_pc = next((c for c in df_ah.columns if 'Current position' in c), None)
                            
                                if c_tp and c_tc:
                                    df['Ah_Traff_Prev'] = df['join_key'].map(dict(zip(df_ah['join_key'], pd.to_numeric(df_ah[c_tp], errors='coerce').fillna(0)))).fillna(0)
                                    df['Ah_Traff_Curr'] = df['join_key'].map(dict(zip(df_ah['join_key'], pd.to_numeric(df_ah[c_tc], errors='coerce').fillna(0)))).fillna(0)
                                    df['Ah_Diff_Traff'] = df['Ah_Traff_Curr'] - df['Ah_Traff_Prev']
                                if c_pp and c_pc:
                                    df['Ah_Pos_Prev'] = df['join_key'].map(dict(zip(df_ah['join_key'], pd.to_numeric(df_ah[c_pp], errors='coerce').fillna(0)))).fillna(0)
                                    df['Ah_Pos_Curr'] = df['join_key'].map(dict(zip(df_ah['join_key'], pd.to_numeric(df_ah[c_pc], errors='coerce').fillna(0)))).fillna(0)
                                    df['Ah_Diff_Pos'] = df['Ah_Pos_Curr'] - df['Ah_Pos_Prev']

                        # Save extracted state
                        st.session_state['df_raw'] = df
                        st.session_state['df_pages_raw'] = df_pages
                        st.session_state['full_df_extracted'] = True

                except Exception as e:
                    st.error(f"Krytyczny błąd wczytywania podstawowych danych: {e}")
                    import traceback
                    st.code(traceback.format_exc())
                    st.stop()
                    
        # Load from State
        df = st.session_state['df_raw'].copy()
        df_pages = st.session_state.get('df_pages_raw', None)
        if df_pages is not None: df_pages = df_pages.copy()

        # ----------------- 4.5 STATUS CODES MERGE -----------------
        all_urls = set()
        if df_pages is not None and 'URL' in df_pages.columns:
            for u in df_pages['URL'].dropna():
                if str(u).startswith("http"): all_urls.add(str(u))
        
        if 'Ah_URL_Prev' in df.columns:
            for u in df['Ah_URL_Prev'].dropna():
                if str(u).startswith("http"): all_urls.add(str(u))
        if 'Ah_URL_Curr' in df.columns:
            for u in df['Ah_URL_Curr'].dropna():
                if str(u).startswith("http"): all_urls.add(str(u))
                
        # Status Sidebar Options
        with st.sidebar:
            st.divider()
            if all_urls:
                with st.expander("📋 Pokaż wyodrębnione skanowane URL z podsumowania [Do skopiowania]"):
                    st.text_area("Zaznacz wszystko i skopiuj:", value="\n".join(sorted(list(all_urls))), height=200)

            st.header("🌐 3. Kody Odpowiedzi (Opcjonalnie)")
            file_status = st.file_uploader("Wgraj gotowy plik z Kodami (.csv, .xlsx) jako wyższy priorytet", type=['csv', 'xlsx'])
            st.info("💡 Skopiuj adresy powyżej i wklej do obcego Crawlera (np. Screaming Frog), by wygenerować plik `CSV` unikalnych kodów, który odczytywać będzie nasza analiza bez obaw o limity zabezpieczeń 403.")
            
            if st.button("Sprawdź kody odpowiedzi online 🚀"):
                st.session_state['run_online_trigger'] = True
            if st.session_state.get('run_online_trigger', False):
                if st.button("🛑 Przerwij sprawdzanie online"):
                    st.session_state['run_online_trigger'] = False

        try:
            with st.spinner("Tworzenie metryk i kalkulacja diagnoz..."):
                if 'df_status_memory' not in st.session_state:
                    st.session_state['df_status_memory'] = pd.DataFrame(columns=["Address", "Status Code"])
                if 'df_status_online' not in st.session_state:
                    st.session_state['df_status_online'] = pd.DataFrame(columns=["Address", "Status Code"])
                
                if file_status:
                    try:
                        if file_status.name.endswith('.csv'):
                            st.session_state['df_status_memory'] = pd.read_csv(io.BytesIO(file_status.getvalue()), sep=None, engine='python')
                        else:
                            st.session_state['df_status_memory'] = pd.read_excel(io.BytesIO(file_status.getvalue()), engine='openpyxl')
                    except Exception as e:
                        st.warning(f"Błąd przy odczycie pliku Status Codes: {e}")
                        
                elif st.session_state.get('run_online_trigger', False) and all_urls:
                    st.info("Trwa sprawdzanie kodów odpowiedzi w tle...")
                    progress_bar = st.progress(0)
                    
                    def prog_callback(val):
                        progress_bar.progress(val)
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    cancel_f = [False]
                    st.session_state['cancel_check'] = cancel_f
                    
                    try:
                        temp_df = loop.run_until_complete(analyze_status_codes(list(all_urls), prog_callback, cancel_flag=cancel_f))
                        st.session_state['df_status_online'] = temp_df.rename(columns={"URL": "Address"})
                    except Exception as e:
                        pass
                    
                    progress_bar.empty()
                    st.session_state['run_online_trigger'] = False
                
                # Merge logic - Plik MA PRIORYTET nad Online.
                df_status = st.session_state['df_status_online'].copy()
                if not st.session_state['df_status_memory'].empty:
                    df_status = pd.concat([df_status, st.session_state['df_status_memory']]).drop_duplicates(subset=['Address'], keep='last')
                
                if not df_status.empty:
                    c_addr = next((c for c in df_status.columns if 'Address' in c or 'URL' in c), None)
                    c_code = next((c for c in df_status.columns if 'Status' in c or 'Code' in c), None)
                    
                    if c_addr and c_code:
                        status_map = dict(zip(df_status[c_addr], df_status[c_code]))
                        
                        # Apply Status Code prioritizing Current Ahrefs URL, then Previous, else None
                        if 'Ah_URL_Curr' in df.columns:
                            df['Status_Code'] = df['Ah_URL_Curr'].map(status_map).fillna(df.get('Ah_URL_Prev', pd.Series(dtype=str)).map(status_map))
                            df['Status_Code'] = pd.to_numeric(df['Status_Code'], errors='coerce').fillna(0).astype(int).astype(str).replace('0', '')
                            
                        if df_pages is not None:
                            df_pages['Status_Code'] = df_pages['URL'].map(status_map)
                            df_pages['Status_Code'] = pd.to_numeric(df_pages['Status_Code'], errors='coerce').fillna(0).astype(int).astype(str).replace('0', '')

                # ----------------- 5. DIAGNOZA MULTI -----------------
                mask_missing = detect_missing_data(df)
                df['Diagnosis'] = assign_multiple_diagnoses(df, mask_missing)
                
                if df_pages is not None:
                    mask_missing_pg = detect_missing_data(df_pages)
                    df_pages['Diagnosis'] = assign_multiple_diagnoses(df_pages, mask_missing_pg)

                st.success("Analiza zakończona sukcesem!")
                
                if not df_status.empty:
                    c_code = next((c for c in df_status.columns if 'Status' in c or 'Code' in c), None)
                    if c_code:
                        codes_count = df_status[c_code].astype(str).str[0].value_counts()
                        summ_text = " | ".join([f"Kody {k}xx: {v}" for k, v in codes_count.items() if k.isdigit()])
                        st.info(f"📊 Zintegrowano URL: {len(df_status)} ({summ_text})")

                tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
                    "📊 KPI & Wykresy", 
                    "🔍 Analiza Fraz", 
                    "📄 Analiza Adresów", 
                    "🕵️‍♂️ Eksplorator Diagnoz", 
                    "🎯 Ahrefs",
                    "📈 GKP & GSC Trendy",
                    "💾 Pobierz Pliki"
                ])
                
                df_loss = df[df['Diff_Clicks'] < 0].copy()
                df_growth = df[df['Diff_Clicks'] > 0].copy()
                df_chart = df_loss.copy()

                fig1 = fig2 = fig_brand = fig_ah = fig3 = fig_ctr = fig_serp = ui_gkp = None
                ui_df_queries = generate_ui_dataframe(df, type_name="Query")
                ui_df_pages = generate_ui_dataframe(df_pages, type_name="Page") if df_pages is not None else None

                with tab1:
                    st.header("Szybkie Podsumowanie")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    total_diff_ui = df_pages['Diff_Clicks'].sum() if df_pages is not None else df['Diff_Clicks'].sum()
                    loss_sum_ui = df_pages[df_pages['Diff_Clicks'] < 0]['Diff_Clicks'].sum() if df_pages is not None else df_loss['Diff_Clicks'].sum()
                    
                    col1.metric("Bilans Całkowity Kliknięć (GSC)", f"{int(total_diff_ui):+}")
                    col2.metric("Suma Utraconych Kliknięć (GSC)", f"{int(loss_sum_ui)}")
                    col3.metric("Ilość Fraz Ze Spadkiem (GSC)", f"{len(df_loss)}")
                    col4.metric("Ilość Fraz Wygasłych (Brak Danych GSC)", f"{mask_missing.sum()}")
                    
                    st.divider()
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        st.subheader("Utracone Kliknięcia (GSC Frazy): Brand vs Generic")
                        brand_loss_sum = df_loss.groupby('Type')['Diff_Clicks'].sum().abs().reset_index()
                        if not brand_loss_sum.empty:
                            fig_brand = px.pie(brand_loss_sum, names='Type', values='Diff_Clicks', title="Utrata ruchu (Zapytania / Frazy): Udział podziału", hole=0.3)
                            st.plotly_chart(fig_brand, use_container_width=True)
                            st.caption("Powyższy wykres sumuje straty przypisane precyzyjnemu słowu, które mogą być mniejsze od globalnych z całych wierszy URL przez maskowanie GSC.")

                    with c2:
                        st.subheader("Mapa Spadków: Zmiana Pozycji vs Strata Kliknięć")
                        if not df_chart.empty:
                            fig2 = px.scatter(df_chart.head(500), x='Diff_Pos', y='Diff_Clicks', color='Type', size='Clicks_Prev',
                                              hover_data={'Query': True, 'Diagnosis': True, 'Diff_Pos': ':.4f', 'Diff_Clicks': True, 'Type': False}, title="Oś X: Zmiana pozycji (GSC) | Oś Y: Strata kliknięć (GSC)", labels={'Diff_Pos': 'Zmiana Pozycji (GSC)', 'Diff_Clicks': 'Strata Kliknięć (GSC)', 'Query': 'Fraza', 'Diagnosis': 'Diagnoza'})
                            st.plotly_chart(fig2, use_container_width=True)
                            
                    st.divider()
                    st.subheader("Wpływ Zmian SERP: Strata Kliknięć a Pozycja Google")
                    if not df_loss.empty:
                        df_loss['Pos_Round'] = df_loss['Pos_Prev'].round().astype(int)
                        
                        df_loss_brand = df_loss[df_loss['Type'] == 'Brand']
                        df_loss_gen = df_loss[df_loss['Type'] == 'Generic']
                        
                        serp_loss_brand = df_loss_brand[df_loss_brand['Pos_Round'] <= 25].groupby('Pos_Round')['Diff_Clicks'].sum().abs().reset_index()
                        serp_loss_gen = df_loss_gen[df_loss_gen['Pos_Round'] <= 25].groupby('Pos_Round')['Diff_Clicks'].sum().abs().reset_index()

                        c_serp1, c_serp2 = st.columns(2)
                        
                        with c1:
                            if not serp_loss_brand.empty:
                                fig_serp_b = px.bar(
                                    serp_loss_brand, x='Pos_Round', y='Diff_Clicks', 
                                    title="Suma Strat wg. Pozycji GSC - BRAND (Top 25)",
                                    labels={'Pos_Round': 'Była Pozycja w Wynikach GSC', 'Diff_Clicks': 'Utracone Kliknięcia'},
                                    color_discrete_sequence=['#FF9900']
                                )
                                fig_serp_b.update_xaxes(dtick=1)
                                c_serp1.plotly_chart(fig_serp_b, use_container_width=True)
                                
                        with c2:
                            if not serp_loss_gen.empty:
                                fig_serp_g = px.bar(
                                    serp_loss_gen, x='Pos_Round', y='Diff_Clicks', 
                                    title="Suma Strat wg. Pozycji GSC - GENERIC (Top 25)",
                                    labels={'Pos_Round': 'Była Pozycja w Wynikach GSC', 'Diff_Clicks': 'Utracone Kliknięcia'},
                                    color_discrete_sequence=['#c0392b']
                                )
                                fig_serp_g.update_xaxes(dtick=1)
                                c_serp2.plotly_chart(fig_serp_g, use_container_width=True)

                        st.caption("Te wykresy pokazują na jakich pozycjach uciekło najwięcej kliknięć, z pominięciem ogólnych fraz Brandowych, które mogą zakrzywiać wykres przy utracie popytu GKP na markę.")
                        
                        st.markdown("### Top 10 Największych Strat wg. Pozycji GSC (Top 10 miejsc)")
                        st.markdown("Poniższe tabele ułatwią zrozumienie jakie konkretnie frazy (Brand vs Generic) zostały dotknięte na poszczególnych kluczowych pozycjach.")
                        
                        for i in range(1, 11):
                            loss_pos_b = df_loss_brand[df_loss_brand['Pos_Round'] == i].sort_values('Diff_Clicks', ascending=True).head(10)
                            loss_pos_g = df_loss_gen[df_loss_gen['Pos_Round'] == i].sort_values('Diff_Clicks', ascending=True).head(10)
                            
                            total_loss = abs(int(df_loss[df_loss['Pos_Round'] == i]['Diff_Clicks'].sum()))
                            
                            if not loss_pos_b.empty or not loss_pos_g.empty:
                                with st.expander(f"📌 Raport dla zaokrąglonej pozycji {i} (-{total_loss} całkowitych klików utraconych na tym slocie)"):
                                    c_t1, c_t2 = st.columns(2)
                                    with c_t1:
                                        st.markdown(f"**🔴 Utrata Brand (Top {len(loss_pos_b)})**")
                                        if not loss_pos_b.empty:
                                            st.dataframe(generate_ui_dataframe(loss_pos_b, "Query"), use_container_width=True)
                                        else:
                                            st.info("Brak spadków brandowych.")
                                    with c_t2:
                                        st.markdown(f"**🔴 Utrata Generic (Top {len(loss_pos_g)})**")
                                        if not loss_pos_g.empty:
                                            st.dataframe(generate_ui_dataframe(loss_pos_g, "Query"), use_container_width=True)
                                        else:
                                            st.info("Brak spadków generycznych.")
                                            
                    st.divider()
                    st.subheader("Wpływ Zmian SERP: Zysk Kliknięć a Pozycja Google")
                    if not df_growth.empty:
                        df_growth['Pos_Round'] = df_growth['Pos_Prev'].round().astype(int)
                        
                        df_growth_brand = df_growth[df_growth['Type'] == 'Brand']
                        df_growth_gen = df_growth[df_growth['Type'] == 'Generic']
                        
                        serp_growth_brand = df_growth_brand[df_growth_brand['Pos_Round'] <= 25].groupby('Pos_Round')['Diff_Clicks'].sum().reset_index()
                        serp_growth_gen = df_growth_gen[df_growth_gen['Pos_Round'] <= 25].groupby('Pos_Round')['Diff_Clicks'].sum().reset_index()

                        c_serp3, c_serp4 = st.columns(2)
                        
                        with c_serp3:
                            if not serp_growth_brand.empty:
                                fig_serp_b_g = px.bar(
                                    serp_growth_brand, x='Pos_Round', y='Diff_Clicks', 
                                    title="Suma Zysków wg. Pozycji GSC - BRAND (Top 25)",
                                    labels={'Pos_Round': 'Była Pozycja w Wynikach GSC', 'Diff_Clicks': 'Zyskane Kliknięcia'},
                                    color_discrete_sequence=['#2ecc71']
                                )
                                fig_serp_b_g.update_xaxes(dtick=1)
                                c_serp3.plotly_chart(fig_serp_b_g, use_container_width=True)
                                
                        with c_serp4:
                            if not serp_growth_gen.empty:
                                fig_serp_g_g = px.bar(
                                    serp_growth_gen, x='Pos_Round', y='Diff_Clicks', 
                                    title="Suma Zysków wg. Pozycji GSC - GENERIC (Top 25)",
                                    labels={'Pos_Round': 'Była Pozycja w Wynikach GSC', 'Diff_Clicks': 'Zyskane Kliknięcia'},
                                    color_discrete_sequence=['#27ae60']
                                )
                                fig_serp_g_g.update_xaxes(dtick=1)
                                c_serp4.plotly_chart(fig_serp_g_g, use_container_width=True)

                        st.caption("Te wykresy pokazują na jakich pozycjach urosły Ci najbardziej kliknięcia, z racji na powroty SERP lub lepszy rynkowy CTR.")
                        
                        st.markdown("### Top 10 Największych Wzrostów wg. Pozycji GSC (Top 10 miejsc)")
                        st.markdown("Poniższe tabele ułatwią zrozumienie jakie konkretnie frazy (Brand vs Generic) zarobiły na poszczególnych kluczowych pozycjach.")
                        
                        for i in range(1, 11):
                            growth_pos_b = df_growth_brand[df_growth_brand['Pos_Round'] == i].sort_values('Diff_Clicks', ascending=False).head(10)
                            growth_pos_g = df_growth_gen[df_growth_gen['Pos_Round'] == i].sort_values('Diff_Clicks', ascending=False).head(10)
                            
                            total_growth = abs(int(df_growth[df_growth['Pos_Round'] == i]['Diff_Clicks'].sum()))
                            
                            if not growth_pos_b.empty or not growth_pos_g.empty:
                                with st.expander(f"📌 Raport Wzrostów dla zaokrąglonej pozycji {i} (+{total_growth} całkowitych klików zyskanych na tym slocie)"):
                                    c_t3, c_t4 = st.columns(2)
                                    with c_t3:
                                        st.markdown(f"**🟢 Wzrosty Brand (Top {len(growth_pos_b)})**")
                                        if not growth_pos_b.empty:
                                            st.dataframe(generate_ui_dataframe(growth_pos_b, "Query"), use_container_width=True)
                                        else:
                                            st.info("Brak wzrostów brandowych.")
                                    with c_t4:
                                        st.markdown(f"**🟢 Wzrosty Generic (Top {len(growth_pos_g)})**")
                                        if not growth_pos_g.empty:
                                            st.dataframe(generate_ui_dataframe(growth_pos_g, "Query"), use_container_width=True)
                                        else:
                                            st.info("Brak wzrostów generycznych.")
                with tab2:
                    st.header("Analiza Fraz (GSC Queries) - Z podziałem wielokrotnych diagnoz")
                    st.markdown("Widok szczegółowy z informacjami **Przed, Po i Różnicy**. Tabela posortowana jest od największych spadków do największych wzrostów.")
                    st.dataframe(ui_df_queries, use_container_width=True, height=600, column_config={
                        "Poprzedni URL (Ahrefs)": st.column_config.LinkColumn(),
                        "Aktualny URL (Ahrefs)": st.column_config.LinkColumn()
                    })

                with tab3:
                    st.header("Analiza Adresów URL (GSC Pages)")
                    if ui_df_pages is not None:
                        st.markdown("Widok szczegółowy dla adresów URL. Posortowana od największych spadków do największych wzrostów.")
                        st.dataframe(ui_df_pages, use_container_width=True, height=600, column_config={
                            "Adres URL (GSC)": st.column_config.LinkColumn()
                        })
                    else:
                        st.warning("Brak danych adresów URL. Wgraj plik z zakładką 'Strony' lub 'Pages'.")
                        
                with tab4:
                    st.header("Wyszukiwanie Głębokie i Filtrowanie Diagnoz")
                    st.markdown("Wybierz jedną z przyczyn by zobaczyć wszystkie obiekty, które zostały nią oflagowane (niektóre mogą być oflagowane kilkoma jednocześnie).")
                    
                    t4_1, t4_2 = st.tabs(["🔍 Frazy (Queries)", "📄 Adresy URL (Pages)"])
                    
                    with t4_1:
                        # Zbierz wszystkie unikalne tagi do filtra 
                        all_diags = set()
                        for d in df['Diagnosis'].dropna():
                            for k in d.split(', '):
                                all_diags.add(k)
                        
                        selected_diag = st.multiselect("Filtruj frazy zawierające diagnozę:", options=list(all_diags), default=[], key="diag_filter_q")
                        
                        df_filtered = df.copy()
                        if selected_diag:
                            mask = df_filtered['Diagnosis'].apply(lambda x: any(d in x for d in selected_diag))
                            df_filtered = df_filtered[mask]
                        
                        search_query = st.text_input("Szukaj frazy (zawiera):", key="phrase_filter_q")
                        if search_query:
                            df_filtered = df_filtered[df_filtered['Query'].str.contains(search_query, case=False, na=False)]
                            
                        st.dataframe(generate_ui_dataframe(df_filtered, "Query"), use_container_width=True, column_config={
                            "Poprzedni URL (Ahrefs)": st.column_config.LinkColumn(),
                            "Aktualny URL (Ahrefs)": st.column_config.LinkColumn()
                        })
                        
                    with t4_2:
                        if df_pages is not None:
                            all_diags_p = set()
                            for d in df_pages['Diagnosis'].dropna():
                                for k in d.split(', '):
                                    all_diags_p.add(k)
                                    
                            selected_diag_p = st.multiselect("Filtruj adresy URL zawierające diagnozę:", options=list(all_diags_p), default=[], key="diag_filter_p")
                            
                            df_filtered_p = df_pages.copy()
                            if selected_diag_p:
                                mask_p = df_filtered_p['Diagnosis'].apply(lambda x: any(d in x for d in selected_diag_p))
                                df_filtered_p = df_filtered_p[mask_p]
                            
                            search_url = st.text_input("Szukaj adresu URL (zawiera):", key="url_filter_p")
                            if search_url:
                                df_filtered_p = df_filtered_p[df_filtered_p['URL'].str.contains(search_url, case=False, na=False)]
                                
                            st.dataframe(generate_ui_dataframe(df_filtered_p, "Page"), use_container_width=True, column_config={
                                "Adres URL (GSC)": st.column_config.LinkColumn()
                            })
                        else:
                            st.info("Brak wgranego arkusza stron.")
                    
                with tab5:
                    st.header("🎯 Analiza Ahrefs: Ruch vs Pozycja")
                    st.markdown("Zderzenie estymacji ruchu ze zmianą pozycji i kliknieć na podsatwie danych Ahrefs.")
                    if df['Ah_Traff_Prev'].sum() > 0:
                        ah_corr = df[(df['Ah_Diff_Pos'] != 0) & (df['Diff_Clicks'] < 0)].copy()
                        if not ah_corr.empty:
                            fig_ah = px.scatter(ah_corr, x='Ah_Diff_Pos', y='Diff_Clicks', 
                                                color='Type', size='Ah_Traff_Prev', hover_data={'Query': True, 'Ah_Pos_Prev': ':.4f', 'Ah_Pos_Curr': ':.4f', 'Diagnosis': True, 'Ah_Diff_Pos': ':.4f', 'Diff_Clicks': True},
                                                title="Spadek kliknięć (GSC) vs Zmiana pozycji (Ahrefs)",
                                                labels={'Ah_Diff_Pos': 'Zmiana pozycji (Ahrefs)', 'Diff_Clicks': 'Utrata kliknięć (GSC)', 'Query': 'Fraza', 'Ah_Pos_Prev': 'Poprz. Poz (Ahrefs)', 'Ah_Pos_Curr': 'Akt. Poz (Ahrefs)'})
                            fig_ah.add_vline(x=0, line_dash="dash", line_color="red")
                            st.plotly_chart(fig_ah, use_container_width=True)
                        else:
                            st.info("Brak wystarczających różnic w pozycjach Ahrefs dla fraz ze spadkami.")
                    else:
                        st.warning("Ta analiza wymaga wgrania pliku Ahrefs!")

                with tab6:
                    st.header("📈 GKP Wolyumeny & GSC Zaawansowane")
                    
                    st.subheader("GKP: Wolumen Wyszukiwań (Zainteresowanie)")
                    st.markdown("Jak bardzo obiektywnie *popyt* użytkowników na dane słowo koreluje z naszymi spadkami.")
                    if df['GKP_Vol_Prev'].sum() > 0:
                        gkp_d = df_loss[(df_loss['GKP_Vol_Prev'] > 0) & (df_loss['Diff_Clicks'] < -5)].copy()
                        if not gkp_d.empty:
                            gkp_d['Vol_Diff'] = gkp_d['GKP_Vol_Curr'] - gkp_d['GKP_Vol_Prev']
                            fig3 = px.scatter(gkp_d, x='Vol_Diff', y='Diff_Clicks', color='Type', hover_data=['Query', 'Diagnosis', 'GKP_Vol_Prev', 'GKP_Vol_Curr'], title="GKP: Spadek Popytu Rynkowego vs Nasze Utracone Kliknięcia", labels={'Vol_Diff': 'Spadek Popytu (GKP)', 'Diff_Clicks': 'Utrata kliknięć (GSC)'})
                            st.plotly_chart(fig3, use_container_width=True)
                            
                            st.markdown("**(GKP) Zestawienie TOP fraz po utracie popytu użytkowników:**")
                            ui_gkp = gkp_d[['Query', 'Type', 'Diagnosis', 'GKP_Vol_Prev', 'GKP_Vol_Curr', 'Vol_Diff', 'GKP_Trend', 'Clicks_Prev', 'Clicks_Curr', 'Diff_Clicks']].copy()
                            ui_gkp['GKP_Trend'] = (ui_gkp['GKP_Trend'] * 100).map("{:.4f}%".format)
                            ui_gkp = ui_gkp.sort_values('Vol_Diff', ascending=True)
                            ui_gkp.columns = ['Fraza', 'Typ', 'Diagnoza', 'Popyt Poprz.', 'Popyt Akt.', 'Strata Popytu Num (GKP)', 'Strata Popytu % (GKP)', 'Kliki Poprz. (GSC)', 'Kliki Akt. (GSC)', 'Strata Kliknięć (GSC)']
                            st.dataframe(ui_gkp, use_container_width=True)
                        else:
                            st.info("Brak wystarczająco dużych spadków kwalifikujących się do korelacji GKP.")
                    else:
                        st.warning("Brak użytecznych danych wolumenowych z Keyword Plannera. Upewnij się, że wgrano plik i wpisano poprawne nazwy w kolumnach konfiguracji.")
                    
                    st.divider()
                    st.subheader("Krzywa CTR (CTR Curve Analysis)")
                    st.markdown("Porównanie średniego CTR dla poszczególnych pozycji (Przed vs Po). Często spadki kliknięć na stałych pozycjach wynikają wprost z pogorszonej *widoczności* na danej pozycji w wyniku zmian wyglądu SERPa.")
                    
                    ctr_df = df[(df['Pos_Prev'] > 0) & (df['Pos_Prev'] <= 20) & (df['Pos_Curr'] > 0) & (df['Pos_Curr'] <= 20)].copy()
                    if not ctr_df.empty:
                        ctr_df['Pos_Prev_Round'] = ctr_df['Pos_Prev'].round().astype(int)
                        ctr_df['Pos_Curr_Round'] = ctr_df['Pos_Curr'].round().astype(int)
                        
                        ctr_prev = ctr_df.groupby('Pos_Prev_Round')['CTR_Prev'].mean().reset_index()
                        ctr_prev.columns = ['Pozycja', 'CTR_Sredni']
                        ctr_prev['Okres'] = 'Poprzedni'
                        
                        ctr_curr = ctr_df.groupby('Pos_Curr_Round')['CTR_Curr'].mean().reset_index()
                        ctr_curr.columns = ['Pozycja', 'CTR_Sredni']
                        ctr_curr['Okres'] = 'Aktualny'
                        
                        ctr_curve = pd.concat([ctr_prev, ctr_curr])
                        
                        fig_ctr = px.line(ctr_curve, x='Pozycja', y='CTR_Sredni', color='Okres', 
                                          title="Średni CTR (GSC) dla pozycji 1-20", markers=True, labels={'Pozycja': 'Pozycja (GSC)', 'CTR_Sredni': 'Średni CTR (GSC)'}, hover_data={'CTR_Sredni': ':.4f'})
                        fig_ctr.update_layout(xaxis=dict(tickmode='linear', tick0=1, dtick=1))
                        fig_ctr.update_yaxes(tickformat=".2%")
                        st.plotly_chart(fig_ctr, use_container_width=True)
                        
                with tab7:
                    st.header("Pobierz Raporty")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            ui_df_queries.to_excel(writer, sheet_name='Frazy (Queries)', index=False)
                            if ui_df_pages is not None:
                                ui_df_pages.to_excel(writer, sheet_name='Adresy (Pages)', index=False)
                            if ui_gkp is not None and not ui_gkp.empty:
                                ui_gkp.to_excel(writer, sheet_name='GKP Utrata Popytu', index=False)
                            if fig_ctr is not None:
                                try:
                                    ctr_df_ex = ctr_curve.pivot(index='Pozycja', columns='Okres', values='CTR_Sredni').reset_index()
                                    ctr_df_ex.to_excel(writer, sheet_name='Krzywa CTR', index=False)
                                except Exception:
                                    pass

                        st.download_button("💾 Pobierz Raport .xlsx", data=output.getvalue(), file_name="Raport_SEO_Full.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary", use_container_width=True)

                    with col2:
                        html_report = generate_html_report(df, df_pages, df_loss, df_growth, fig1, fig2, fig_brand, fig_ah, fig3, ui_gkp, fig_ctr, ui_df_queries, ui_df_pages)
                        st.download_button("📄 Pobierz Pełen Raport Wizualny (HTML / PDF)", data=html_report, file_name="Raport_SEO_Wizualny.html", mime="text/html", type="primary", use_container_width=True)

        except Exception as e:
            st.error(f"❌ KRYTYCZNY BŁĄD PODCZAS PRZETWARZANIA: {e}")
            import traceback
            st.code(traceback.format_exc())