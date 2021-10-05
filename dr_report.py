import tempfile
from sqlalchemy import create_engine
from pandas import Series, DataFrame, read_csv, merge
from pandas_gbq import to_gbq
from pydata_google_auth import get_user_credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional
from numpy import arange, where
from aenum import Enum, NoAlias

schema = 'bi'
user = 'your username' # pass to os variable
password = 'your password' # pass to os variable
host = 'bi.thd.cc'
port = 5433

# collection of error_messages
em_col = {
    'bluesnap_com': """
        case
                when
                error_message
                like
                '%FRAUD_DETECTED%'
                then
                'FRAUD_DETECTED'
                when
                error_message
                like
                '%FRAUD_ERROR%'
                then
                'FRAUD_ERROR'
                when
                error_message
                like
                '%SHOPPER_COUNTRY_OFAC_SANCTIONED%'
                then
                'SHOPPER_COUNTRY_OFAC_SANCTIONED'
                when
                error_message
                like
                '%INCORRECT_INFORMATION%'
                then
                'INCORRECT_INFORMATION'
                when
                error_message
                like
                '%SYSTEM_TECHNICAL_ERROR%'
                then
                'SYSTEM_TECHNICAL_ERROR'
                when
                error_message
                like
                '%CVV_ERROR%'
                then
                'CVV_ERROR'
                when
                error_message
                like
                '%PROCESSING_GENERAL_DECLINE%'
                then
                'PROCESSING_GENERAL_DECLINE'
                when
                error_message
                like
                '%GENERAL_PAYMENT_PROCESSING_ERROR%'
                then
                'GENERAL_PAYMENT_PROCESSING_ERROR'
                when
                error_message
                like
                '%RESTRICTED_CARD%'
                then
                'RESTRICTED_CARD'
                when
                error_message
                like
                '%INSUFFICIENT_FUNDS%'
                then
                'INSUFFICIENT_FUNDS'
                when
                error_message
                like
                '%PICKUP_CARD%'
                then
                'PICKUP_CARD'
                when
                error_message
                like
                '%STRONG_CUSTOMER_AUTHENTICATION_REQUIRED%'
                then
                'STRONG_CUSTOMER_AUTHENTICATION_REQUIRED'
                when
                error_message
                like
                '%EXPIRED_CARD%'
                then
                'EXPIRED_CARD'
                when
                error_message
                like
                '%HIGH_RISK_ERROR%'
                then
                'HIGH_RISK_ERROR'
                when
                error_message
                like
                '%INVALID_CARD_NUMBER%'
                then
                'INVALID_CARD_NUMBER'
                when
                error_message
                like
                '%LIMIT_EXCEEDED%'
                then
                'LIMIT_EXCEEDED'
                when
                error_message
                like
                '%INVALID_PIN_OR_PW_OR_ID_ERROR%'
                then
                'INVALID_PIN_OR_PW_OR_ID_ERROR'
                when
                error_message
                like
                '%THE_ISSUER_IS_UNAVAILABLE_OR_OFFLINE%'
                then
                'THE_ISSUER_IS_UNAVAILABLE_OR_OFFLINE'
                when
                error_message
                like
                '%PROCESSING_AMOUNT_ERROR%'
                then
                'PROCESSING_AMOUNT_ERROR'
                when
                error_message
                like
                '%NO_AVAILABLE_PROCESSORS%'
                then
                'NO_AVAILABLE_PROCESSORS'
                when
                error_message
                like
                '%INVALID_TRANSACTION_TYPE%'
                then
                'INVALID_TRANSACTION_TYPE'
                WHEN
                error_message
                like
                '% 000%'
                then
                'Failure while attempting to capture funds'
                WHEN
                error_message
                like
                '% 02%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 03%'
                then
                'null'
                WHEN
                error_message
                like
                '% 04%'
                then
                'PROCESSING_GENERAL_DECLINE'
                WHEN
                error_message
                like
                '% 05%'
                then
                'PROCESSING_GENERAL_DECLINE'
                WHEN
                error_message
                like
                '% 08%'
                then
                'null'
                WHEN
                error_message
                like
                '% 100%'
                then
                'Authorization has failed for this transaction. Please try again or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 100.100.500%'
                then
                'An error occurred. Please try again later.'
                WHEN
                error_message
                like
                '% 100.100.700%'
                then
                'An error occurred. Please try again later.'
                WHEN
                error_message
                like
                '% 100.150.203%'
                then
                'An error occurred. Please try again later.'
                WHEN
                error_message
                like
                '% 1000%'
                then
                'There are no E-Commerce merchant accounts configured to process Unknown transactions with currency'
                WHEN
                error_message
                like
                '% 101%'
                then
                'Credit card expiration date has passed.'
                WHEN
                error_message
                like
                '% 111%'
                then
                'Account does not exist. Please check the details and try again or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 12%'
                then
                'null'
                WHEN
                error_message
                like
                '% 122%'
                then
                'The security code entered is invalid. Please check number and enter again'
                WHEN
                error_message
                like
                '% 13%'
                then
                'This transaction has been declined. Please try a different card or contact the credit card provider for assistance'
                WHEN
                error_message
                like
                '% 14%'
                then
                'Invalid card number. Please check the number and try again, or use a different card'
                WHEN
                error_message
                like
                '% 19%'
                then
                'null'
                WHEN
                error_message
                like
                '% 20%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card'
                WHEN
                error_message
                like
                '% 21%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card'
                WHEN
                error_message
                like
                '% 22%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 23%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 24%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 30%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 3200%'
                then
                'There was an error processing the request with the acquiring bank.'
                WHEN
                error_message
                like
                '% 41%'
                then
                'This card has been reported lost or stolen. Please contact your bank for assistance (PV-41)'
                WHEN
                error_message
                like
                '% 42%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 43%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430285%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430306%'
                then
                'The expiration date entered is invalid. Enter valid expiration date or try another card'
                WHEN
                error_message
                like
                '% 430330%'
                then
                'Invalid card number. Please check the number and try again, or use a different card'
                WHEN
                error_message
                like
                '% 430348%'
                then
                'Not authorised'
                WHEN
                error_message
                like
                '% 430354%'
                then
                'null'
                WHEN
                error_message
                like
                '% 430357%'
                then
                'This card has been reported lost or stolen. Please contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430360%'
                then
                'Insufficient funds. Please use another card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430390%'
                then
                'Unable to authorise'
                WHEN
                error_message
                like
                '% 430396%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430403%'
                then
                'null'
                WHEN
                error_message
                like
                '% 430409%'
                then
                'online Referred(GC-430409)'
                WHEN
                error_message
                like
                '% 430412%'
                then
                'Not authorised'
                WHEN
                error_message
                like
                '% 430415%'
                then
                'Not authorised'
                WHEN
                error_message
                like
                '% 430418%'
                then
                'This card has been reported lost or stolen. Please contact your bank for assistance'
                WHEN
                error_message
                like
                '% 430421%'
                then
                'online Not authorised(GC-430421)'
                WHEN
                error_message
                like
                '% 430424%'
                then
                'Unable to authorise'
                WHEN
                error_message
                like
                '% 430490%'
                then
                'This transaction has failed. Please contact your bank for assistance'
                WHEN
                error_message
                like
                '% 44%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance (PV-44)'
                WHEN
                error_message
                like
                '% 51%'
                then
                'INSUFFICIENT_FUNDS'
                WHEN
                error_message
                like
                '% 54%'
                then
                'The expiration date entered is invalid. Enter valid expiration date or try another card (PV-54)'
                WHEN
                error_message
                like
                '% 55%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 57%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card'
                WHEN
                error_message
                like
                '% 58%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 600.200.500%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 6000%'
                then
                'A configured rule is preventing this transaction to be processed.'
                WHEN
                error_message
                like
                '% 61%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card'
                WHEN
                error_message
                like
                '% 62%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card'
                WHEN
                error_message
                like
                '% 65_SCA%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 75%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 76%'
                then
                'Invalid card number. Please check the number and try again, or use a different card'
                WHEN
                error_message
                like
                '% 800.100.152%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 800.100.153%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 800.100.156%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 800.100.157%'
                then
                'null'
                WHEN
                error_message
                like
                '% 800.100.162%'
                then
                'Authorization declined. Try a different card or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 800.100.171%'
                then
                'This card has been reported lost or stolen. Please contact your bank for assistance'
                WHEN
                error_message
                like
                '% 800.400.500%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card(POAS-800.400.500)'
                WHEN
                error_message
                like
                '% 87%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 89%'
                then
                'This transaction has failed. Please contact your credit card provider for assistance or try another card (PV-89)'
                WHEN
                error_message
                like
                '% 900.100.200%'
                then
                'An error occurred. Please try again later.'
                WHEN
                error_message
                like
                '% 91%'
                then
                'null'
                WHEN
                error_message
                like
                '% 92%'
                then
                'This transaction has been declined. Please check card details and try again, or contact your bank for assistance'
                WHEN
                error_message
                like
                '% 94%'
                then
                'null'
                WHEN
                error_message
                like
                '% 96%'
                then
                'null'
                WHEN
                error_message
                like
                '% 99%'
                then
                'null'
                WHEN
                error_message
                like
                '% General system failure%'
                then
                'null'
                WHEN
                error_message
                like
                '% N7%'
                then
                'The CVV security code entered is invalid. Please check the number and try again'
                WHEN
                error_message
                like
                '% R1%'
                then
                'null'
                when
                error_message
                like
                '%cURL%'
                then
                'cURL'
                when
                error_message
                like
                '%VALIDATION_GENERAL_FAILURE%'
                then
                'VALIDATION_GENERAL_FAILURE'
                when
                error_message
                like
                'Client error%'
                then
                'Client error'
                when
                error_message is null
                then
                'null'
                when
                error_message
                like
                '%EXPIRED_TOKEN%'
                then
                'Token is expired'
                when
                error_message
                like
                '%Insufficient funds%'
                then
                'INSUFFICIENT_FUNDS'
                when
                error_message
                like
                '%Transaction failed because there are no available processors.%'
                then
                'NO_AVAILABLE_PROCESSORS'
                when error_message like '%20031%'
                then 'MISSING_RELEVANT_METHOD_FOR_SHOPPER'
    """
    ,
    'cardpay': """

        case
                     when error_message is null or error_message = '' then 'blank'
                     when error_message like 'Current cur%' then 'Current currency not allowed'
                     when error_message like '%authorisation response%' then 'Invalid authorisation'
                     when error_message like 'cURL%' then 'cURL'
                     """
    ,
    'cashflows': """
    case
                     when error_message is null or error_message = '' then 'blank'
                     when error_message like 'Current cur%' then 'Current currency not allowed'
                     when error_message like '%authorisation response%' then 'Invalid authorisation'
                     when error_message like 'cURL%' then 'cURL'
    """
    ,
    'centrobill': """
    case when error_message is null then 'blank'
                     when error_message like 'Feed transaction%' then 'Feed transaction not found'
                     when error_message like '%risk%' then '0665 Transaction declined by risk management'
                     when error_message like '%bank decline%' then 'General bank decline'
                     when error_message like '%Insufficient funds%' then 'Insufficient funds or credit limit exceeded'
                     when error_message like 'Client error%' then 'Client error'
                     when error_message like 'Server error%' then 'Server error'
                     when error_message like '%not permitted%' then 'Transaction not permitted to cardholder'
                     when error_message like '%number is invalid%' then '0409 Invalid card number'
                     when error_message like '%"email"%' then 'Email is invalid'
                     when error_message like 'System er%' then '8000 System error'
                     when error_message like 'Restricted c%' then '0011 Restricted card'
                     when error_message like '%Security V%' then '9930 Security violation'
                     when error_message like '%Invalid card%' then '0408 Invalid card'
                     when error_message like '%stolen card%' then '0006 Pick up card (stolen card)'
                     when error_message like '%no fraud%' then '5004 Pick up card(no fraud)'
    """
    ,
    'checkout_com': """
    case when error_message is null then 'blank'
                         when error_message like 'cURL%' then 'cURL'
                         when error_message like '%Code: 422%' then 'The endpoint did not accept the request. (Code: 422)'
    """
    ,
    'dlocal': """
    case
               when error_message like 'REJECT%' then split_part(error_message, ' - ', 2)
               when error_message like 'Status:"R%' then
                   split_part(
                   'Status:"REJECTED", details:"Call bank for authorize."', '"', 4)
               when error_message is null then 'blank'
               when error_message like 'cURL%' then 'cURL'
               when error_message like 'Server err%' then 'Server error'
    """
    ,
    'hipay': """
    case when error_message is null then 'blank'
                     when error_message like 'cURL%' then 'cURL'
                     when error_message like '%exceeds the allowable maximum%' then 'Exceeds the allowable maximum'
                     when error_message like 'Current currency%' then 'Currency not allowed'
                     when error_message like '%timed out%' then 'Timeout'
    """
    ,
    'maxpay': """
    case when error_message is null then 'blank'
                     when error_message like 'cURL%' then 'cURL'
                     when error_message like 'stream_soc%' then 'stream_socket_error'
                     when error_message like 'Given alpha2C%' then 'Given alpha2Code is not present'
    """
    ,
    'safecharge': """
    case when error_message is null then 'blank'
                     when error_message like 'cURL%' then 'cURL'
                     when error_message like '%TRANSREASONAMOUNT=0%' then 'TRANSREASONAMOUNT0'
                     when error_message like '%TRANSREASONAMOUNT=1%' then 'TRANSREASONAMOUNT1'
                     when error_message like '%TRANSREASONAMOUNT=2%' then 'TRANSREASONAMOUNT2'
                     when error_message like '%TRANSREASONAMOUNT=3%' then 'TRANSREASONAMOUNT3'
                     when error_message like '<HTML%' then 'HTML'
                     when error_message like '%Timeout%'
                       or error_message like '%timed out%'
                       or error_message like '%timed-out%'
                       or error_message like '%Timed Out%'
                       then 'Timeout'
    """
}


class Provider(Enum):

    _settings_ = NoAlias

    BLUESNAP = 'bluesnap_com'
    CARDPAY = 'cardpay'
    CASHFLOWS = 'cashflows'
    CENTROBILL = 'centrobill'
    CHECKOUT = 'checkout_com'
    DLOCAL = 'dlocal'
    HIPAY = 'hipay'
    MAXPAY_KORTA = 'maxpay'
    MAXPAY_TRANSACT = 'maxpay'
    SAFECHARGE = 'safecharge'


class AbstractProvider:

    def __init__(self, provider, month_end):
        self.provider = provider
        self.month_end = month_end

    LATAM_COUNTRIES = ['BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
                       'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY',
                       'COSTA RICA', 'PARAGUAY', 'HONDURAS']

    def build_query(self):
        q = f'''
           with t as (
           select
           upper(country.name) country
           , count(1) n
           from bi_cloud_pay.payment_charge pc
           left join bi_cloud_clients.country_dic country
           on pc.country = country.code
           where provider like '%{self.provider}%'
           and created_at >= '2020-10-01'
           and created_at < '{self.month_end}'
           and target = 'checkout'
           group by 1
           order by 2 desc
           )
           select t.*, rank() over (order by n desc) rnk
           from t
           '''
        return q

    @staticmethod
    def get_source_f(query):
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
            engine = create_engine(
                'postgresql://' + user + ':' + password + '@' + host + ':' + str(port) + '/' + schema)
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = read_csv(tmpfile)
            cursor.close()
        return df

    @staticmethod
    def build_top3_list(df, min_latam):
        top3_list = []
        c = 0
        while len(top3_list) < 3:
            if df['is_latam'][c] == 0:
                top3_list.append(df['country'][c])
            elif df['is_latam'][c] != 0 and df['rnk'][c] == min_latam:
                top3_list.append(df['country'][c])
            else:
                pass
            c += 1
        return top3_list

    def build_query_2_1(self, top_list):
        q = f"""
        select
        date_trunc('month', created_at)::date ym
        , date_trunc('week', created_at)::date w
        , upper(country.name) as country
        , (case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
            'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 'LATAM'
        when upper(country.name) in ('{top_list[0]}', '{top_list[1]}') then upper(country.name) else 'OTHERS' end) as top_countries

        ,case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
           'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 2
        when upper(country.name) = '{top_list[0]}' then 1
        when upper(country.name) = '{top_list[1]}' then 3
        else 0
        end
        country_rank

         , '{self.provider}' as provider
         , pc.mid
         , pc.bin
         , cb.issuer bin_bank
         , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
         , case when pc.status = 'completed'
                then 'success'
                else
                {em_col[self.provider]}
                else
                error_message
                end
                end
                error_message
         , count(case when pc.status = 'completed' then id end) as is_success
         , count(case when pc.status != 'completed' then id end) as is_declined
         , count(id) as count_tries

        from bi_cloud_pay.payment_charge pc
            left join bi_cloud_clients.country_dic country
                on pc.country = country.code
            left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                on cast(cb.bin as text)=pc.bin
        where provider like '%{self.provider}%'
          and created_at >= '2020-10-01'
          and created_at < '{self.month_end}'
          and target = 'checkout'
        group by 1,2,3,4,5,6,7,8,9,10,11
        """
        return q

    def build_query_2_2(self, top_list):
        q = f"""
            select
            date_trunc('month', created_at)::date ym
             ,  date_trunc('week', created_at)::date w
                 , upper(country.name) as country
                 , case
                    when upper(country.name) in ('{top_list[0]}', '{top_list[1]}', '{top_list[2]}')
                    then upper(country.name) else 'OTHERS' end as top_countries

                , case
                   when upper(country.name) = '{top_list[0]}' then 1
                   when upper(country.name) = '{top_list[1]}' then 2
                   when upper(country.name) = '{top_list[2]}' then 3
                   else 0
                   end
                   country_rank

                 , '{self.provider}' as provider
                 , pc.mid
                 , pc.bin
                 , cb.issuer bin_bank
                 , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
             , case when pc.status = 'completed'
                    then 'success'
                    else
                    {em_col[self.provider]}
                    else
                    error_message
                    end
                    end
                    error_message
                 , count(case when pc.status = 'completed' then id end) as is_success
                 , count(case when pc.status != 'completed' then id end) as is_declined
                 , count(id) as count_tries

            from bi_cloud_pay.payment_charge pc
                left join bi_cloud_clients.country_dic country
                    on pc.country = country.code
                left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                    on cast(cb.bin as text)=pc.bin
            where provider like '%{self.provider}%'
              and created_at >= '2020-10-01'
              and created_at < '{self.month_end}'
              and target = 'checkout'
            group by 1,2,3,4,5,6,7,8,9,10,11
            """
        return q

    def bank_rank_by_dates(self, df, period_months: Optional[int] = None):

        def calc_start_month(date_obj):
            date_obj_ = datetime.strptime(date_obj, '%Y-%m-%d')
            date_obj_ = date_obj_ - relativedelta(months=period_months)
            return date_obj_.strftime('%Y-%m-%d')

        rank_map = {}
        if period_months is None:
            for _ in range(1, 4):  # 4 stands for TOP4 banks
                top = list(df[df['country_rank'] == _].groupby(['bin_bank'])
                           .sum()['count_tries'].sort_values(ascending=False)[:4].index)
                top_dict = {}
                for x in range(len(top)):
                    top_dict.update({tuple([_, top[x]]): x + 1})
                rank_map.update(top_dict)
        else:
            start_month = calc_start_month(self.month_end)
            for _ in range(1, 4):
                top = list(
                    df[
                        (df['country_rank'] == _) & (
                                df['ym'] >= start_month) & (
                                df['ym'] < self.month_end
                        )].groupby(['bin_bank'])
                          .sum()['count_tries'].sort_values(ascending=False)[:4].index)
                top_dict = {}
                for x in range(len(top)):
                    top_dict.update({tuple([_, top[x]]): x + 1})
                rank_map.update(top_dict)

        return rank_map

    @staticmethod
    def top_reasons(df, top_num):

        def unpivot_df(data):
            data_ = DataFrame(data).reset_index()
            return data_

        decl_map = unpivot_df(df['error_message'].value_counts(normalize=True))
        decl_map['rnk'] = arange(1, len(decl_map) + 1, 1)

        top_dr = DataFrame(decl_map[decl_map.rnk <= top_num]['index'])

        df_ = merge(df, top_dr, how='left', left_on='error_message', right_on='index')
        df_['index'] = df_['index'].fillna('Others')
        df_ = df_.rename(columns={'index': 'top_reasons'})

        return df_

    @staticmethod
    def write_to_gbq(df):

        scopes = [
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive',
        ]

        credentials = get_user_credentials(
            scopes,
            auth_local_webserver=True,
        )

        to_gbq(
            df,
            'reports_eu.DR_report_',
            project_id='cloudbi',
            if_exists='append',
            table_schema=[
                {'name': 'ym', 'type': 'DATE'},
                {'name': 'w', 'type': 'DATE'},
                {'name': 'country', 'type': 'STRING'},
                {'name': 'top_countries', 'type': 'STRING'},
                {'name': 'country_rank', 'type': 'INTEGER'},
                {'name': 'provider', 'type': 'STRING'},
                {'name': 'mid', 'type': 'STRING'},
                {'name': 'bin', 'type': 'INTEGER'},
                {'name': 'bin_bank', 'type': 'STRING'},
                {'name': 'status', 'type': 'STRING'},
                {'name': 'error_message', 'type': 'STRING'},
                {'name': 'is_success', 'type': 'INTEGER'},
                {'name': 'is_declined', 'type': 'INTEGER'},
                {'name': 'count_tries', 'type': 'INTEGER'},
                {'name': 'top_reasons', 'type': 'STRING'},
                {'name': 'bank_rank', 'type': 'INTEGER'},
                {'name': 'bank_rank_3M', 'type': 'INTEGER'}
            ]
        )


class MaxpayKortaProvider(AbstractProvider):
    def build_query(self):
        q = f'''
           with t as (
           select
           upper(country.name) country
           , count(1) n
           from bi_cloud_pay.payment_charge pc
           left join bi_cloud_clients.country_dic country
           on pc.country = country.code
           where provider like '%{self.provider}%'
           and mid not like '%transact%'
           and created_at >= '2020-10-01'
           and created_at < '{self.month_end}'
           and target = 'checkout'
           group by 1
           order by 2 desc
           )
           select t.*, rank() over (order by n desc) rnk
           from t
           '''
        return q

    def build_query_2_1(self, top_list):
        q = f"""
        select
        date_trunc('month', created_at)::date ym
        , date_trunc('week', created_at)::date w
        , upper(country.name) as country
        , (case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
            'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 'LATAM'
        when upper(country.name) in ('{top_list[0]}', '{top_list[1]}') then upper(country.name) else 'OTHERS' end) as top_countries

        ,case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
           'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 2
        when upper(country.name) = '{top_list[0]}' then 1
        when upper(country.name) = '{top_list[1]}' then 3
        else 0
        end
        country_rank

         , '{self.provider + '_korta'}' as provider
         , pc.mid
         , pc.bin
         , cb.issuer bin_bank
         , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
         , case when pc.status = 'completed'
                then 'success'
                else
                {em_col[self.provider]}
                else
                error_message
                end
                end
                error_message
         , count(case when pc.status = 'completed' then id end) as is_success
         , count(case when pc.status != 'completed' then id end) as is_declined
         , count(id) as count_tries

        from bi_cloud_pay.payment_charge pc
            left join bi_cloud_clients.country_dic country
                on pc.country = country.code
            left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                on cast(cb.bin as text)=pc.bin
        where provider like '%{self.provider}%'
          and mid not like '%transact%'
          and created_at >= '2020-10-01'
          and created_at < '{self.month_end}'
          and target = 'checkout'
        group by 1,2,3,4,5,6,7,8,9,10,11
        """
        return q

    def build_query_2_2(self, top_list):
        q = f"""
            select
            date_trunc('month', created_at)::date ym
             ,  date_trunc('week', created_at)::date w
                 , upper(country.name) as country
                 , case
                    when upper(country.name) in ('{top_list[0]}', '{top_list[1]}', '{top_list[2]}')
                    then upper(country.name) else 'OTHERS' end as top_countries

                , case
                   when upper(country.name) = '{top_list[0]}' then 1
                   when upper(country.name) = '{top_list[1]}' then 2
                   when upper(country.name) = '{top_list[2]}' then 3
                   else 0
                   end
                   country_rank

                 , '{self.provider + '_korta'}' as provider
                 , pc.mid
                 , pc.bin
                 , cb.issuer bin_bank
                 , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
             , case when pc.status = 'completed'
                    then 'success'
                    else
                    {em_col[self.provider]}
                    else
                    error_message
                    end
                    end
                    error_message
                 , count(case when pc.status = 'completed' then id end) as is_success
                 , count(case when pc.status != 'completed' then id end) as is_declined
                 , count(id) as count_tries

            from bi_cloud_pay.payment_charge pc
                left join bi_cloud_clients.country_dic country
                    on pc.country = country.code
                left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                    on cast(cb.bin as text)=pc.bin
            where provider like '%{self.provider}%'
              and mid not like '%transact%'
              and created_at >= '2020-10-01'
              and created_at < '{self.month_end}'
              and target = 'checkout'
            group by 1,2,3,4,5,6,7,8,9,10,11
            """
        return q


class MaxpayTransactProvider(AbstractProvider):
    def build_query(self):
        q = f'''
           with t as (
           select
           upper(country.name) country
           , count(1) n
           from bi_cloud_pay.payment_charge pc
           left join bi_cloud_clients.country_dic country
           on pc.country = country.code
           where provider like '%{self.provider}%'
           and mid like '%transact%'
           and created_at >= '2020-10-01'
           and created_at < '{self.month_end}'
           and target = 'checkout'
           group by 1
           order by 2 desc
           )
           select t.*, rank() over (order by n desc) rnk
           from t
           '''
        return q

    def build_query_2_1(self, top_list):
        q = f"""
        select
        date_trunc('month', created_at)::date ym
        , date_trunc('week', created_at)::date w
        , upper(country.name) as country
        , (case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
            'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 'LATAM'
        when upper(country.name) in ('{top_list[0]}', '{top_list[1]}') then upper(country.name) else 'OTHERS' end) as top_countries

        ,case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
           'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 2
        when upper(country.name) = '{top_list[0]}' then 1
        when upper(country.name) = '{top_list[1]}' then 3
        else 0
        end
        country_rank

         , '{self.provider + '_transact'}' as provider
         , pc.mid
         , pc.bin
         , cb.issuer bin_bank
         , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
         , case when pc.status = 'completed'
                then 'success'
                else
                {em_col[self.provider]}
                else
                error_message
                end
                end
                error_message
         , count(case when pc.status = 'completed' then id end) as is_success
         , count(case when pc.status != 'completed' then id end) as is_declined
         , count(id) as count_tries

        from bi_cloud_pay.payment_charge pc
            left join bi_cloud_clients.country_dic country
                on pc.country = country.code
            left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                on cast(cb.bin as text)=pc.bin
        where provider like '%{self.provider}%'
          and mid like '%transact%'
          and created_at >= '2020-10-01'
          and created_at < '{self.month_end}'
          and target = 'checkout'
        group by 1,2,3,4,5,6,7,8,9,10,11
        """
        return q

    def build_query_2_2(self, top_list):
        q = f"""
            select
            date_trunc('month', created_at)::date ym
             ,  date_trunc('week', created_at)::date w
                 , upper(country.name) as country
                 , case
                    when upper(country.name) in ('{top_list[0]}', '{top_list[1]}', '{top_list[2]}')
                    then upper(country.name) else 'OTHERS' end as top_countries

                , case
                   when upper(country.name) = '{top_list[0]}' then 1
                   when upper(country.name) = '{top_list[1]}' then 2
                   when upper(country.name) = '{top_list[2]}' then 3
                   else 0
                   end
                   country_rank

                 , '{self.provider + '_transact'}' as provider
                 , pc.mid
                 , pc.bin
                 , cb.issuer bin_bank
                 , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
             , case when pc.status = 'completed'
                    then 'success'
                    else
                    {em_col[self.provider]}
                    else
                    error_message
                    end
                    end
                    error_message
                 , count(case when pc.status = 'completed' then id end) as is_success
                 , count(case when pc.status != 'completed' then id end) as is_declined
                 , count(id) as count_tries

            from bi_cloud_pay.payment_charge pc
                left join bi_cloud_clients.country_dic country
                    on pc.country = country.code
                left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                    on cast(cb.bin as text)=pc.bin
            where provider like '%{self.provider}%'
              and mid like '%transact%'
              and created_at >= '2020-10-01'
              and created_at < '{self.month_end}'
              and target = 'checkout'
            group by 1,2,3,4,5,6,7,8,9,10,11
            """
        return q


class CentrobillTransactProvider(AbstractProvider):
    def build_query(self):
        q = f'''
           with t as (
           select
           upper(country.name) country
           , count(1) n
           from bi_cloud_pay.payment_charge pc
           left join bi_cloud_clients.country_dic country
           on pc.country = country.code
           where provider like '%{self.provider}%'
           and mid like '%decta%'
           and created_at >= '2020-10-01'
           and created_at < '{self.month_end}'
           and target = 'checkout'
           group by 1
           order by 2 desc
           )
           select t.*, rank() over (order by n desc) rnk
           from t
           '''
        return q

    def build_query_2_1(self, top_list):
        q = f"""
        select
        date_trunc('month', created_at)::date ym
        , date_trunc('week', created_at)::date w
        , upper(country.name) as country
        , (case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
            'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 'LATAM'
        when upper(country.name) in ('{top_list[0]}', '{top_list[1]}') then upper(country.name) else 'OTHERS' end) as top_countries

        ,case when upper(country.name) in ('BRAZIL', 'MEXICO', 'CHILE', 'ARGENTINA', 'PERU', 'COLOMBIA', 'PUERTO RICO',
           'ECUADOR', 'DOMINICAN REPUBLIC', 'GUATEMALA', 'PANAMA', 'URUGUAY', 'COSTA RICA', 'PARAGUAY', 'HONDURAS') then 2
        when upper(country.name) = '{top_list[0]}' then 1
        when upper(country.name) = '{top_list[1]}' then 3
        else 0
        end
        country_rank

         , '{self.provider}' as provider
         , pc.mid
         , pc.bin
         , cb.issuer bin_bank
         , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
         , case when pc.status = 'completed'
                then 'success'
                else
                {em_col[self.provider]}
                else
                error_message
                end
                end
                error_message
         , count(case when pc.status = 'completed' then id end) as is_success
         , count(case when pc.status != 'completed' then id end) as is_declined
         , count(id) as count_tries

        from bi_cloud_pay.payment_charge pc
            left join bi_cloud_clients.country_dic country
                on pc.country = country.code
            left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                on cast(cb.bin as text)=pc.bin
        where provider like '%{self.provider}%'
          and mid like '%decta%'
          and created_at >= '2020-10-01'
          and created_at < '{self.month_end}'
          and target = 'checkout'
        group by 1,2,3,4,5,6,7,8,9,10,11
        """
        return q

    def build_query_2_2(self, top_list):
        q = f"""
            select
            date_trunc('month', created_at)::date ym
             ,  date_trunc('week', created_at)::date w
                 , upper(country.name) as country
                 , case
                    when upper(country.name) in ('{top_list[0]}', '{top_list[1]}', '{top_list[2]}')
                    then upper(country.name) else 'OTHERS' end as top_countries

                , case
                   when upper(country.name) = '{top_list[0]}' then 1
                   when upper(country.name) = '{top_list[1]}' then 2
                   when upper(country.name) = '{top_list[2]}' then 3
                   else 0
                   end
                   country_rank

                 , '{self.provider}' as provider
                 , pc.mid
                 , pc.bin
                 , cb.issuer bin_bank
                 , (case when pc.status = 'completed' then 'completed' else 'declined' end) as status
             , case when pc.status = 'completed'
                    then 'success'
                    else
                    {em_col[self.provider]}
                    else
                    error_message
                    end
                    end
                    error_message
                 , count(case when pc.status = 'completed' then id end) as is_success
                 , count(case when pc.status != 'completed' then id end) as is_declined
                 , count(id) as count_tries

            from bi_cloud_pay.payment_charge pc
                left join bi_cloud_clients.country_dic country
                    on pc.country = country.code
                left join ( select bin, max(issuer) issuer from bi_cloud_clients.card_bin group by 1) cb
                    on cast(cb.bin as text)=pc.bin
            where provider like '%{self.provider}%'
              and mid like '%decta%'
              and created_at >= '2020-10-01'
              and created_at < '{self.month_end}'
              and target = 'checkout'
            group by 1,2,3,4,5,6,7,8,9,10,11
            """
        return q


def read_provider(provider, month_end:str):

    factories = {
        Provider.MAXPAY_KORTA: MaxpayKortaProvider(provider.value, month_end),
        Provider.MAXPAY_TRANSACT: MaxpayTransactProvider(provider.value, month_end),
        Provider.CENTROBILL: CentrobillTransactProvider(provider.value, month_end)
    }

    if provider in factories.keys():
        return factories[provider]
    else:
        return AbstractProvider(provider.value, month_end)


def main(provider, month_end:str) -> None:

    i = read_provider(provider, month_end)

    f1 = i.get_source_f(i.build_query())

    f1['is_latam'] = where(f1['country'].isin(i.LATAM_COUNTRIES), 1, 0)

    MIN_LATAM = f1[f1.is_latam == 1]['rnk'].min()

    top3_list = i.build_top3_list(f1, MIN_LATAM)

    TOP_LIST = [x for x in top3_list if x not in i.LATAM_COUNTRIES]

    if MIN_LATAM < 4:
        f2 = i.get_source_f(i.build_query_2_1(TOP_LIST))
    else:
        f2 = i.get_source_f(i.build_query_2_2(TOP_LIST))

    f2['error_message'] = f2['error_message'].fillna('blank')

    rm_all_time = i.bank_rank_by_dates(f2)
    rm_last_3M = i.bank_rank_by_dates(f2, 3)

    f2['bank_rank'] = Series(list(zip(f2['country_rank'], f2['bin_bank']))).map(rm_all_time)
    f2['bank_rank_last_3M'] = Series(list(zip(f2['country_rank'], f2['bin_bank']))).map(rm_last_3M)

    f2 = i.top_reasons(f2, 10)

    # tests
    print(f2['top_reasons'].value_counts(normalize=True).sum())  # success if sum = 1
    print(f2['provider'].unique())  # success if only one provider
    print('top_countries:', len(f2['top_countries'].unique()))  # success if len = 4
    print('top_reasons:', len(f2['top_reasons'].unique()))  # success if len = N+1 (10+1)
    print(f2['country_rank'].unique())  # success if unique values = 4
    print(f2['bank_rank'].unique())  # success if unique values = 5
    print(f2['bank_rank_last_3M'].unique())  # success if unique values = 5

    i.write_to_gbq(f2)

# main(Provider.MAXPAY_KORTA, '2021-09-01')

# for yy in Provider:
#     main(yy, '2021-09-01')
