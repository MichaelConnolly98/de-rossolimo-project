from src.transform.pandas_testing import dataframe_creator
import pandas as pd

def sales_facts():
    sales_order = dataframe_creator("sales_order")

    sales_order["created_date"] = pd.to_datetime(pd.to_datetime(sales_order["created_at"], format="mixed").dt.date)
    sales_order["created_time"] = pd.to_datetime(sales_order["created_at"], format="mixed").dt.time
    sales_order.drop(["created_at"], axis=1, inplace=True)

    sales_order["last_updated_date"] = pd.to_datetime(pd.to_datetime(sales_order["last_updated"], format="mixed").dt.date)
    sales_order["last_updated_time"] = pd.to_datetime(sales_order["last_updated"], format="mixed").dt.time
    sales_order.drop(["last_updated"], axis=1, inplace=True)

    sales_order["agreed_payment_date"] = pd.to_datetime(sales_order["agreed_payment_date"])
    sales_order["agreed_delivery_date"] = pd.to_datetime(sales_order["agreed_delivery_date"])

    sales_order['unit_price'] = pd.to_numeric(sales_order['unit_price'])

    sales_order["sales_order_id"] = sales_order.index
    sales_order.rename(columns={
        "staff_id" : "sales_staff_id"
    }, inplace=True)
    sales_order.index.name = "sales_record_id"

    desired_order = [
        "sales_order_id", 
        "created_date", 
        "created_time", 
        "last_updated_date", 
        "last_updated_time", 
        "sales_staff_id", 
        "counterparty_id", 
        "units_sold", 
        "unit_price", 
        "currency_id", 
        "design_id", 
        "agreed_payment_date", 
        "agreed_delivery_date", 
        "agreed_delivery_location_id"
    ]

    sales_order = sales_order[desired_order]
    sales_order.name = "fact_sales_order"

    return sales_order

def purchase_order_facts():
    purchase_order = dataframe_creator('purchase_order')

    purchase_order["created_date"] = pd.to_datetime(pd.to_datetime(purchase_order["created_at"], format="mixed").dt.date)
    purchase_order["created_time"] = pd.to_datetime(purchase_order["created_at"], format="mixed").dt.time
    purchase_order.drop(["created_at"], axis=1, inplace=True)

    purchase_order["last_updated_date"] = pd.to_datetime(pd.to_datetime(purchase_order["last_updated"], format="mixed").dt.date)
    purchase_order["last_updated_time"] = pd.to_datetime(purchase_order["last_updated"], format="mixed").dt.time
    purchase_order.drop(["last_updated"], axis=1, inplace=True)

    purchase_order["agreed_payment_date"] = pd.to_datetime(purchase_order["agreed_payment_date"])
    purchase_order["agreed_delivery_date"] = pd.to_datetime(purchase_order["agreed_delivery_date"])

    purchase_order['item_unit_price'] = pd.to_numeric(purchase_order['item_unit_price'])

    purchase_order["purchase_order_id"] = purchase_order.index
    purchase_order.index.name = "purchase_record_id"

    desired_order = [
        "purchase_order_id", 
        "created_date", 
        "created_time", 
        "last_updated_date", 
        "last_updated_time", 
        "staff_id", 
        "counterparty_id", 
        "item_code", 
        "item_quantity", 
        "item_unit_price", 
        "currency_id", 
        "agreed_delivery_date",
        "agreed_payment_date", 
        "agreed_delivery_location_id"
    ]

    purchase_order = purchase_order[desired_order]
    purchase_order.name = "fact_purchase_order"


    return purchase_order