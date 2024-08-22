from src.transform.most_recent_pandas import dataframe_creator_single
import pandas as pd

def sales_facts(file_dict=None):
    
    sales_order = dataframe_creator_single("sales_order", file_dict)
    if isinstance(sales_order, pd.DataFrame):
        sales_order["created_date"] = pd.to_datetime(pd.to_datetime(sales_order["created_at"], format="mixed").dt.date)
        sales_order["created_time"] = pd.to_datetime(sales_order["created_at"], format="mixed").dt.time
        sales_order.drop(["created_at"], axis=1, inplace=True)

        sales_order["last_updated_date"] = pd.to_datetime(pd.to_datetime(sales_order["last_updated"], format="mixed").dt.date)
        sales_order["last_updated_time"] = pd.to_datetime(sales_order["last_updated"], format="mixed").dt.time
        sales_order.drop(["last_updated"], axis=1, inplace=True)

        sales_order["agreed_payment_date"] = pd.to_datetime(sales_order["agreed_payment_date"])
        sales_order["agreed_delivery_date"] = pd.to_datetime(sales_order["agreed_delivery_date"])

        sales_order['unit_price'] = pd.to_numeric(sales_order['unit_price'])

        sales_order.rename(columns={
            "staff_id" : "sales_staff_id"
        }, inplace=True)

        desired_order = [ 
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
    else:
        return None

def purchase_order_facts(file_dict=None):
    
    purchase_order = dataframe_creator_single('purchase_order', file_dict)
    if isinstance(purchase_order, pd.DataFrame):
        purchase_order["created_date"] = pd.to_datetime(pd.to_datetime(purchase_order["created_at"], format="mixed").dt.date)
        purchase_order["created_time"] = pd.to_datetime(purchase_order["created_at"], format="mixed").dt.time
        purchase_order.drop(["created_at"], axis=1, inplace=True)

        purchase_order["last_updated_date"] = pd.to_datetime(pd.to_datetime(purchase_order["last_updated"], format="mixed").dt.date)
        purchase_order["last_updated_time"] = pd.to_datetime(purchase_order["last_updated"], format="mixed").dt.time
        purchase_order.drop(["last_updated"], axis=1, inplace=True)

        purchase_order["agreed_payment_date"] = pd.to_datetime(purchase_order["agreed_payment_date"])
        purchase_order["agreed_delivery_date"] = pd.to_datetime(purchase_order["agreed_delivery_date"])

        purchase_order['item_unit_price'] = pd.to_numeric(purchase_order['item_unit_price'])
        desired_order = [
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
    else:
        return None
def payment_facts(file_dict=None):
    payment = dataframe_creator_single('payment', file_dict)
    if isinstance(payment, pd.DataFrame):
        payment.drop(["company_ac_number", "counterparty_ac_number"], axis=1, inplace=True)

        payment["created_date"] = pd.to_datetime(pd.to_datetime(payment["created_at"], format="mixed").dt.date)
        payment["created_time"] = pd.to_datetime(payment["created_at"], format="mixed").dt.time
        payment.drop(["created_at"], axis=1, inplace=True)

        payment["last_updated_date"] = pd.to_datetime(pd.to_datetime(payment["last_updated"], format="mixed").dt.date)
        payment["last_updated_time"] = pd.to_datetime(payment["last_updated"], format="mixed").dt.time
        payment.drop(["last_updated"], axis=1, inplace=True)

        payment["payment_date"] = pd.to_datetime(payment["payment_date"])

        payment["payment"] = payment["payment_date"].astype(bool)

        payment['payment_amount'] = pd.to_numeric(payment['payment_amount'])


        desired_order = [
            "created_date", 
            "created_time", 
            "last_updated_date", 
            "last_updated_time", 
            "transaction_id", 
            "counterparty_id", 
            "payment_amount",
            "currency_id",
            "payment_type_id",
            "paid", 
            "payment_date"
        ]

        payment = payment[desired_order]
        payment.name = "fact_payment"


        return payment
    else:
        return None
