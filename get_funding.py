import requests
import json
import time
import os
import urllib3
import threading
import customtkinter as ctk
from datetime import datetime
import pyperclip  # Necesario para copiar al portapapeles: pip install pyperclip

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CryptoScannerApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # --- CONFIGURACIÓN DE VENTANA ---
        self.title("Binance Funding Radar Pro")
        self.geometry("1000x850")
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.running_loop = False
        
        # Fuentes
        self.font_button = ("Segoe UI", 18, "bold")
        self.font_status = ("Segoe UI", 16)
        self.font_json = ("Consolas", 20)

        # --- DISEÑO DE INTERFAZ ---
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        # Panel Superior
        self.top_frame = ctk.CTkFrame(self)
        self.top_frame.grid(row=0, column=0, padx=20, pady=20, sticky="ew")

        self.btn_run = ctk.CTkButton(self.top_frame, text="EJECUTAR ESCANEO", 
                                     font=self.font_button, command=self.run_once, 
                                     fg_color="#2ecc71", hover_color="#27ae60", height=50)
        self.btn_run.grid(row=0, column=0, padx=15, pady=15)

        self.btn_loop = ctk.CTkButton(self.top_frame, text="AUTO-ESCANEO: OFF", 
                                      font=self.font_button, command=self.toggle_loop, 
                                      fg_color="#e74c3c", height=50)
        self.btn_loop.grid(row=0, column=1, padx=15, pady=15)

        self.status_label = ctk.CTkLabel(self.top_frame, text="Estado: Esperando...", font=self.font_status)
        self.status_label.grid(row=0, column=2, padx=30)

        # Contenedor del JSON y Botón Copiar
        self.container = ctk.CTkFrame(self)
        self.container.grid(row=1, column=0, padx=20, pady=(0, 20), sticky="nsew")
        self.container.grid_columnconfigure(0, weight=1)
        self.container.grid_rowconfigure(0, weight=1)

        # Cuadro de Texto JSON
        self.txt_output = ctk.CTkTextbox(self.container, font=self.font_json, border_width=2)
        self.txt_output.grid(row=0, column=0, sticky="nsew")

        # Botón Copiar (Posicionado arriba a la derecha del cuadro)
        self.btn_copy = ctk.CTkButton(self.container, text="📋 Copiar JSON", 
                                      font=("Segoe UI", 12, "bold"), width=100, height=30,
                                      fg_color="#34495e", hover_color="#2c3e50",
                                      command=self.copy_to_clipboard)
        self.btn_copy.place(relx=0.98, rely=0.02, anchor="ne") # Esquina superior derecha

    def copy_to_clipboard(self):
        content = self.txt_output.get("1.0", ctk.END).strip()
        if content:
            pyperclip.copy(content)
            self.btn_copy.configure(text="✅ ¡Copiado!", fg_color="#27ae60")
            self.after(2000, lambda: self.btn_copy.configure(text="📋 Copiar JSON", fg_color="#34495e"))

    def toggle_loop(self):
        self.running_loop = not self.running_loop
        if self.running_loop:
            self.btn_loop.configure(text="AUTO-ESCANEO: ON", fg_color="#2ecc71")
            self.start_loop_thread()
        else:
            self.btn_loop.configure(text="AUTO-ESCANEO: OFF", fg_color="#e74c3c")

    def run_once(self):
        threading.Thread(target=self.check_all_market_logic, daemon=True).start()

    def start_loop_thread(self):
        def loop():
            while self.running_loop:
                self.check_all_market_logic()
                time.sleep(60) # Actualización cada 1 minuto
        threading.Thread(target=loop, daemon=True).start()

    def check_all_market_logic(self):
        self.status_label.configure(text=f"Estado: Escaneando ({datetime.now().strftime('%H:%M:%S')})")
        
        ENDPOINTS = ["https://fapi.binance.com/fapi/v1/premiumIndex", "https://www.binance.com/fapi/v1/premiumIndex"]
        TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        LIQ_URL = "https://fapi.binance.com/fapi/v1/allForceOrders"
        
        THRESHOLD = 0.005 # 0.5% (Tus ajustes)
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

        data_funding = None
        for url in ENDPOINTS:
            try:
                res = requests.get(url, headers=headers, timeout=15)
                if res.status_code == 200:
                    data_funding = res.json()
                    break
            except: continue

        if not data_funding:
            self.update_ui("ERROR: Conexión con Binance fallida.")
            return

        try:
            ticker_res = requests.get(TICKER_URL, headers=headers, timeout=15).json()
            ticker_map = {t['symbol']: t for t in ticker_res}
        except: ticker_map = {}

        try:
            liq_res = requests.get(LIQ_URL, headers=headers, timeout=10).json()
            liq_symbols = {l['symbol'] for l in liq_res}
        except: liq_symbols = set()

        history_file = "history_db.json"
        if os.path.exists(history_file):
            try:
                with open(history_file, "r") as f: history = json.load(f)
            except: history = {}
        else: history = {}

        now_ts = int(time.time())
        final_results = []
        session = requests.Session()

        for item in data_funding:
            symbol = item.get('symbol', '')
            if not symbol.endswith('USDT'): continue
            
            f_rate = float(item.get('lastFundingRate', 0))
            f_pct = round(f_rate * 100, 4)

            if abs(f_rate) >= THRESHOLD:
                price = float(item.get('markPrice', 0))
                t_info = ticker_map.get(symbol, {})
                
                try:
                    oi_data = session.get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}", timeout=5).json()
                    oi_usd = float(oi_data.get('openInterest', 0)) * price
                except: oi_usd = 0.0

                vol_24h = float(t_info.get('quoteVolume', 0)) if t_info else 0.0
                price_chg = float(t_info.get('priceChangePercent', 0)) if t_info else 0.0

                if symbol not in history: history[symbol] = []
                history[symbol].append({"ts": now_ts, "oi": oi_usd, "funding": f_pct})
                history[symbol] = [r for r in history[symbol] if now_ts - r['ts'] <= 86400]

                def get_change(records, seconds):
                    if len(records) < 2: return 0.0
                    target = now_ts - seconds
                    past = min(records, key=lambda x: abs(x['ts'] - target))
                    if now_ts - past['ts'] > (seconds * 2): return 0.0
                    return round(((oi_usd - past['oi']) / past['oi'] * 100), 2) if past['oi'] > 0 else 0.0

                oi_4h = get_change(history[symbol], 14400)
                oi_24h = get_change(history[symbol], 86400)

                trend = "STABLE"
                if len(history[symbol]) > 1:
                    trend = "INCREASING" if f_pct > history[symbol][-2]['funding'] else "DECREASING"

                candidate_squeeze = abs(f_pct) >= 1.8 and oi_4h >= -5

                final_results.append({
                    "symbol": symbol, "timestamp": now_ts, "funding_rate_pct": f_pct,
                    "funding_trend": trend, "price": price, "price_24h_change_pct": price_chg,
                    "volume_24h_usd": round(vol_24h, 2), "open_interest_usd": round(oi_usd, 2),
                    "oi_4h_change_pct": oi_4h, "oi_24h_change_pct": oi_24h,
                    "liquidation_cluster_detected": symbol in liq_symbols,
                    "candidate_squeeze": candidate_squeeze, "type": "POSITIVE" if f_rate > 0 else "NEGATIVE"
                })

        final_results = sorted(final_results, key=lambda x: abs(x['funding_rate_pct']), reverse=True)
        with open(history_file, "w") as f: json.dump(history, f)
        
        self.update_ui(json.dumps(final_results, indent=4))
        self.status_label.configure(text=f"Estado: OK ({datetime.now().strftime('%H:%M:%S')})")

    def update_ui(self, content):
        self.txt_output.delete("1.0", ctk.END)
        self.txt_output.insert("1.0", content)

if __name__ == "__main__":
    app = CryptoScannerApp()
    app.mainloop()
