import random
from datetime import datetime, timedelta
import pandas as pd


def generate_data(num_rows: int = 100) -> pd.DataFrame:
    """Generate a DataFrame with sample data.

    Args:
        num_rows (int): Number of rows to generate.

    Returns:
        pd.DataFrame: Generated DataFrame with sample data.
    """
    data = []
    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"]  # Sản phẩm example
    start_date = datetime(2023, 1, 1)  # Ngày bắt đầu
    for i in range(num_rows):
        date = start_date + timedelta(days=random.randint(0, 365))  # Ngày ngẫu nhiên
        product = random.choice(products)
        quantity = random.randint(1, 10)
        price = round(random.uniform(50, 1000), 2)
        data.append(
            {
                "id": i + 1,
                "sale_date": date.date(),
                "product": product,
                "quantity": quantity,
                "price": price,
            }
        )
    df = pd.DataFrame(data)
    return df


# if __name__ == "__main__":
#     df = generate_data(100)
#     print(df.head())
