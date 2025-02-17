﻿# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

### <summary>
### Example algorithm using the custom data type as a source of alpha
### </summary>
class CoinGeckoMarketCapUniverseSelectionAlgorithm(QCAlgorithm): 
    def Initialize(self):
        ''' Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized. '''

        # Data ADDED via universe selection is added with Daily resolution.
        self.UniverseSettings.Resolution = Resolution.Daily

        self.SetStartDate(2022, 2, 14)
        self.SetEndDate(2022, 2, 18)
        self.SetCash(100000)

        # add a custom universe data source (defaults to usa-equity)
        self.AddUniverse(CoinGeckoMarketCapUniverse, "CoinGeckoMarketCapUniverse", Resolution.Daily, self.UniverseSelection)

    def UniverseSelection(self, data):
        ''' Selected the securities
        
        :param List of MyCustomUniverseType data: List of MyCustomUniverseType
        :return: List of Symbol objects '''

        for datum in data:
            self.Log(f"{datum.Symbol},{datum.Coin},{datum.MarketCap}")
        
        # define our selection criteria
        return [Symbol.Create(f"{d.Coin}USD", SecurityType.Crypto, Market.GDAX) for d in data if d.MarketCap > 200000]

    def OnSecuritiesChanged(self, changes):
        ''' Event fired each time that we add/remove securities from the data feed
		
        :param SecurityChanges changes: Security additions/removals for this time step
        '''
        self.Log(changes.ToString())
