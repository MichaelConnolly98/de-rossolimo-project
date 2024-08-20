def payment_dim(currency_df):
    currency_df = currency_df.drop(['created_at', 'last_updated'], axis=1)
    return currency_df