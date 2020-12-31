import pandas as pd


def split_sales():
    sales = pd.read_csv("data/sales_train.csv")
    for i, x in sales.groupby(sales['date_block_num']):
        x.to_csv(f"data/sales/sales_{i}.csv", index=False)


def change_sep_in_items():
    items = pd.read_csv("data/items.csv")
    items.to_csv("data/items_p.csv", index=False, sep="|")
    items_cat = pd.read_csv("data/item_categories.csv")
    items_cat.to_csv("data/items_cat_p.csv", index=False, sep="|")


change_sep_in_items()
