"""
Тестовый скрипт для проверки логики SELL сигналов
"""

def test_sell_logic_calculation():
    """Проверяем правильность расчётов при продаже"""
    
    # Сценарий 1: Прибыльная продажа
    print("=== Сценарий 1: Прибыльная продажа ===")
    avg_cost = 0.55
    quantity = 250
    sell_price = 0.64
    
    executed_size = quantity
    trade_value = sell_price * executed_size
    realized_pnl = (sell_price - avg_cost) * executed_size
    
    print(f"Средняя цена покупки: ${avg_cost}")
    print(f"Количество: {quantity}")
    print(f"Цена продажи: ${sell_price}")
    print(f"Выручка: ${trade_value:.2f}")
    print(f"Реализованная прибыль: ${realized_pnl:.2f}")
    print()
    
    # Сценарий 2: Убыточная продажа
    print("=== Сценарий 2: Убыточная продажа ===")
    avg_cost = 0.75
    quantity = 150
    sell_price = 0.45
    
    executed_size = quantity
    trade_value = sell_price * executed_size
    realized_pnl = (sell_price - avg_cost) * executed_size
    
    print(f"Средняя цена покупки: ${avg_cost}")
    print(f"Количество: {quantity}")
    print(f"Цена продажи: ${sell_price}")
    print(f"Выручка: ${trade_value:.2f}")
    print(f"Реализованный убыток: ${realized_pnl:.2f}")
    print()
    
    # Сценарий 3: Продажа в ноль
    print("=== Сценарий 3: Продажа точно по цене покупки ===")
    avg_cost = 0.60
    quantity = 200
    sell_price = 0.60
    
    executed_size = quantity
    trade_value = sell_price * executed_size
    realized_pnl = (sell_price - avg_cost) * executed_size
    
    print(f"Средняя цена покупки: ${avg_cost}")
    print(f"Количество: {quantity}")
    print(f"Цена продажи: ${sell_price}")
    print(f"Выручка: ${trade_value:.2f}")
    print(f"PnL: ${realized_pnl:.2f}")
    print()
    
    # Сценарий 4: Учёт slippage
    print("=== Сценарий 4: С учётом slippage ===")
    avg_cost = 0.55
    quantity = 250
    bid_price = 0.64
    slippage_bps = 50.0  # 0.5%
    slip = slippage_bps / 10_000.0
    sell_price = bid_price * (1.0 - slip)  # На sell - отнимаем slippage от bid
    
    executed_size = quantity
    trade_value = sell_price * executed_size
    realized_pnl = (sell_price - avg_cost) * executed_size
    
    print(f"Средняя цена покупки: ${avg_cost}")
    print(f"Количество: {quantity}")
    print(f"Bid цена: ${bid_price}")
    print(f"Slippage: {slippage_bps} bps ({slip*100:.2f}%)")
    print(f"Фактическая цена продажи: ${sell_price:.4f}")
    print(f"Выручка: ${trade_value:.2f}")
    print(f"Реализованная прибыль: ${realized_pnl:.2f}")
    print()

if __name__ == "__main__":
    test_sell_logic_calculation()
