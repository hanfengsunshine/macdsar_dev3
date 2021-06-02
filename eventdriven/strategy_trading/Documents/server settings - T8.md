## T8服务器，用于买方高频交易策略

## 交易服务器端口 ##
### 火币现货 ###
	6. 6104: liqam 子账户 三角
	7. 6105: liqam 主账户
	10. 6109: FHBRM
	11. 6110: liqamhfcta2
	12. 6111: perpcmm2
	13. 6409: FHBMOM, Shawn CTA
	14. 6410: cta2sub1, Shawn CTA
	15. 6120: cta2sub2, Shawn CTA
	16. 6121: buysidetest, Shawn CTA
	17. 6122: perpcmm0
	18. 6123: liqamfrarb1
	19. 6125: liqamarb1
	20. 6126: liqamarb2
	21. 6127: liqamfrarb2
	22. 6128: liqamfrarb3
	23. 6129: liqamfrarbeth1
	24. 6130：liqamarbeth1
	25. 6131：liqambcarb1
	26. 6132: liqamarbusdt1
	27. 6133: liqamarbusdt2
	28. 6134: liqamarbusdt3
	29. 6135: liqamarbusdt4
	30. 6136: liqamarbusdt5
	31. 6137: liqamarbusdt6
	
### 火币期货 ###
	2. 6300: BASCA
	3. 6301: liqam 主账户
	7. 6309: FHBMOM, Shawn CTA
	14. 6316: CARARB, 暂用于Richard CTA momentum
	15. 6317: FHBRM, 已移除
	18. 6320: liqamhfcta1
	19. 6308: cta2sub1, Shawn CTA
	20. 6310: liqamfrarb1
	21. 6311: cta2sub1
	22. 6312：liqambcarb1
	23. 6313: liqamfutpsbtc1
	24. 6314: perpcmm0
	25. 6315: cta2sub2
	26. 6316: liqamarbfutbtcusd1

### 火币永续 ###
	2. 6411: perpcmm2
	3. 6412: perpcmm0
	4. 6413: liqamfrarb1
	5. 6415: liqamarb1
	6. 6416: liqamfrarb2
	7. 6417: liqamfrarb3
	8. 6418: liqamusdtps
	9. 6419: cta2sub1
	10. 6420：liqamfrarbeth1
	11. 6421：liqamarbeth1
	12. 6422: liqamfutpsbtc1
	13. 6423: cta2sub2
	14. 6424：liqamfrarbeth2
	15. 6425：liqamfrarbeth3
	16. 6426：liqamfrarbeth4
	17. 6427: liqamarbusdt1
	18. 6428: liqamarbusdt2
	19. 6429: liqamarbusdt3
	20. 6430: liqamarbusdt4
	21. 6431: liqamarbusdt5
	22. 6432: liqamarbusdt6

### OKEX 现货 ###
    1. 6501: okexmm
	
### OKEX 永续 ###
    1. 6601: okexmm
    2. 6602: hbamokexarb1
    3. 6603: hbamokexarbusdt1
    4. 6604: hbamokexusdtps
    5. 6605: hbamokexarbeth1
    
### OKEX 期货 ###
    1. 6701：hbamokexarbfutbtcusd1

### BINANCE 现货 ###
    1. 6201：ambnarbusdt1
    2. 6202：ambnarbusdt2
    3. 6203：ambnarbusdt3
    4. 6204：ambnarbusdt4
    5. 6205：ambnarbusdt5
    6. 6206：ambnarbusdt6

### BINANCE USDT永续 ###
    1. 6511：hbambinancearb1
    2. 6512: hbambinanceusdtps
    3. 6513: hbambinancetest
    4. 6515: ambnarbusdt1
    5. 6516: ambnarbusdt2
    6. 6517: ambnarbusdt3
    7. 6518: ambnarbusdt4
    8. 6519: ambnarbusdt5
    9. 6520: ambnarbusdt6

### BINANCE期货 ###
    1. 6801: hbambinancearbfutbtcusd1
    2. 6802: ambnarbfutusd1 (交割)
    3. 6803: ambnarbusd1 (USD永续)

## 市场数据端口 ##
### 火币现货 ###
	1. 7000: 通用实盘 (主流市场, 有K线)
	2. 7101: 通用实盘 (主流市场， 无K线)
	3. 7500: 三角套利 (所有市场， 无K线)
	4. 7401: Shawn CTA test (无K线)

### 火币期货 ###
	1. 7200: 通用实盘 (有K线)
	2. 7300: 通用实盘 (无K线)
	3. 7301：通用实盘 liqamarbfutbtcusd1

### BITMEX ###
	1. 7400: Shawn CTA test (有K线)

### OKEX现货 ###
    1. 7500: depth and trade
    
### OKEX永续 ###
    1. 7600: depth and trade
    
### OKEX期货 ###
    1. 7700：depth and trade (hbamokexarbfutbtcusd1)

### BINANCE 现货 ###
    1. 7520: Binance 现货

### BINANCE永续 ###
    1. 7501: Binance 永续 long short
    
### BINANCE期货 ###
    1. 7801: depth and trade (hbambinancearbfutbtcusd1)
    