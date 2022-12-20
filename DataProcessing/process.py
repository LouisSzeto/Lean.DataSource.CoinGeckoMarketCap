import getopt, sys
from CLRImports import *
from QuantConnect.Logging import *
from QuantConnect.Securities.CurrencyConversion import *

from datetime import datetime
from pathlib import Path
import requests
import time

VendorName = "coingecko"
VendorDataName = "marketcap"
datafolder = "C:/LeanCLI/data"
destination = '/temp-output-directory'

class CoinGeckoMarketCapDataDownloader:
    def __init__(self, destinationFolder, db_file):
        self.destinationFolder = Path(destinationFolder)
        self.universeFolder = self.destinationFolder / "universe"
        self.destinationFolder.mkdir(parents=True, exist_ok=True)
        self.universeFolder.mkdir(parents=True, exist_ok=True)

        self.symbol_id = self.GetAllSupportedSymbols(db_file)
        
    def Run(self, only_today=True):
        days = "1" if only_today else "max"
        
        for i, symbol in enumerate(self.symbol_id):
            coin_id = self.symbol_id[symbol]
            Log.Trace(f"CoinGeckoMarketCapDataDownloader:Run(): Process coin - {symbol}")
            trial_left = 5
            rate_gate = 10     # number of request in 1 second

            while trial_left > 0:
                try:
                    # Fetch data
                    req_start_time = time.time()
                    coin_history = self.HttpRequester(f"{coin_id}/market_chart?vs_currency=usd&days={days}&interval=daily")['market_caps']
                    req_end_time = time.time()
                    req_time = req_end_time - req_start_time
                    time.sleep(max(1 / rate_gate - req_time, 0))

                    if len(coin_history) <= 1:
                        raise Exception("No data fetched")
                    
                    # Get only data that already been consolidated
                    coin_history = coin_history[:-1]

                    # Format data
                    lines = []

                    for data_point in coin_history:
                        unix_timestamp, market_cap = data_point
                        date = datetime.fromtimestamp(unix_timestamp / 1000).strftime("%Y%m%d")

                        lines.append(f"{date},{market_cap}")

                        # Daily universe data writing
                        self.WriteToFile(date, f"{symbol},{market_cap}")
                        
                    # Per symbol data writing
                    self.WriteToFile(symbol, lines)
                    break

                except Exception as e:
                    print(f'CoinGeckoMarketCapDataDownloader:Run(): {e} - Failed to parse data for {symbol} - Retrying')
                    time.sleep(2)
                    trial_left -= 1
            
            if (i+1) % 100 == 0:
                print(f'CoinGeckoMarketCapDataDownloader:Run(): processed {i+1}/{len(self.symbol_id)} coins')

    def GetAllSupportedSymbols(self, db_file):
        start = time.time()

        # Get all possible crypto symbols QC supported
        Log.Trace("CoinGeckoMarketCapDataDownloader:GetAllSupportedSymbols(): Fetching all QC crypto symbols...")
        qc_crypto_symbols = set()

        with open(f"{db_file}", 'r') as f:
            lines = f.readlines()

        for line in lines:
            if line[0] == '#' or line == '\n':
                continue
            line  = line.strip()
            line = line.split(',')
            if(len(line) < 5):
                continue
            if line[2] == 'crypto':
                sid = SecurityIdentifier.GenerateCrypto(line[1], line[0]);
                symbol = Symbol(sid, line[1])
    
                success, baseCurrency, quoteCurrency = CurrencyPairUtil.TryDecomposeCurrencyPair(symbol, '', '')
                if success:
                    qc_crypto_symbols.add(baseCurrency.lower())
                    qc_crypto_symbols.add(quoteCurrency.lower())

        # Get symbol-id pair of coins supported by CoinGecko
        Log.Trace(f"CoinGeckoMarketCapDataDownloader:GetAllSupportedSymbols(): Get CoinGecko crypto symbols-id pairs for {len(qc_crypto_symbols)} coins")
        gecko_symbol_id = {}

        coins = self.HttpRequester("list")
        for coin in coins:
            if coin['symbol'] in qc_crypto_symbols:
                gecko_symbol_id[coin['symbol']] = coin['id']
        
        print(f"CoinGeckoMarketCapDataDownloader:GetAllSupportedSymbols(): Finished getting all QC supported CoinGecko crypto symbols-id pairs in {time.time() - start}")

        return gecko_symbol_id
    
    def WriteToFile(self, filename, content):
        target = self.universeFolder if filename.isnumeric() else self.destinationFolder
        target = target / f'{filename}.csv'
        
        lines = []
        if target.is_file():
            lines = open(target, 'r', encoding="utf-8").readlines()
        
        if isinstance(content, str):
            lines.append(content)
        else:
            lines.extend(content)
        lines = [x.replace('\n', '') for x in lines if x.replace('\n', '')]
        sorted_lines = sorted(set(lines), key=lambda x: x.split(',')[0])
        
        with open(target, 'w', encoding="utf-8") as file:
            file.write('\n'.join(sorted_lines))

    def HttpRequester(self, url):       
        base_url = 'https://api.coingecko.com/api/v3/coins'
        return requests.get(f'{base_url}/{url}').json()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("process.py only takes 1 argument.")
    argumentList = sys.argv[1]
    
    arguments, values = getopt.getopt(argumentList, "", ["process-only-today"])
    for currentArgument, currentValue in arguments:
        if currentArgument == "--process-only-today":
            today_only = bool(currentValue)
    
    start_time = time.time()
    
    db_file = Path(datafolder) / "symbol-properties/symbol-properties-database.csv"
    destinationDirectory = f"{destination}/alternative/{VendorName}/{VendorDataName}"
    Config.Set("data-folder", datafolder)
    
    instance = CoinGeckoMarketCapDataDownloader(destinationDirectory, db_file)
    instance.Run(today_only)
    
    time_taken = time.time() - start_time
    print("Total time taken to run in minutes : ", time_taken//60)