from datetime import datetime, timedelta

from datetime import date
import json
import time
import sys
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from urllib.request import Request, urlopen
import numpy as np
import ssl
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.preprocessing import MinMaxScaler
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication


ssl._create_default_https_context = ssl._create_unverified_context
def craw_stock_price(**kwargs):

    to_date = kwargs["to_date"]
    from_date = "2022-01-01"

    stock_price_df = pd.DataFrame()
    stock_code = "DIG"

    ssl._create_default_https_context = ssl._create_unverified_context

    url = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:gte:{}~date:lte:{}&size=9990&page=1".format(stock_code, from_date, to_date)
    print(url)

    req = Request(url, headers={'User-Agent': 'Mozilla / 5.0 (Windows NT 6.1; WOW64; rv: 12.0) Gecko / 20100101 Firefox / 12.0'})
    x = urlopen(req, timeout=10).read()

    req.add_header("Authorization", "Basic %s" % "ABCZYXX")

    json_x = json.loads(x)['data']

    for stock in json_x:
        stock_price_df = pd.concat([stock_price_df, pd.DataFrame([stock])], ignore_index=True)

    stock_price_df.to_csv("/home/vinh/vinh_python/air_flow/datas/stock_price.csv", index=None)
    return True

class LSTMRegressor(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(LSTMRegressor, self).__init__()
        self.hidden_size = hidden_size
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers=4, dropout=0.2)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, input):
        output, _ = self.lstm(input)
        output = self.fc(output[:, -1, :])
        return output

def train_model_pytorch():
    dataset_train = pd.read_csv('/home/vinh/vinh_python/air_flow/datas/stock_price.csv')
    training_set = dataset_train.iloc[:, 5:6].values

    sc = MinMaxScaler(feature_range=(0, 1))
    training_set_scaled = sc.fit_transform(training_set)

    X_train = []
    y_train = []
    no_of_sample = len(training_set)

    for i in range(60, no_of_sample):
        X_train.append(training_set_scaled[i - 60:i, 0])
        y_train.append(training_set_scaled[i, 0])

    X_train, y_train = np.array(X_train), np.array(y_train)
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))

    input_size = 1
    hidden_size = 50
    output_size = 1
    model = LSTMRegressor(input_size, hidden_size, output_size)
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    X_train = torch.from_numpy(X_train).float()
    y_train = torch.from_numpy(y_train).float()

    num_epochs = 10
    batch_size = 32

    for epoch in range(num_epochs):
        for i in range(0, len(X_train), batch_size):
            inputs = X_train[i:i + batch_size]
            labels = y_train[i:i + batch_size]

            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

        print(f'Epoch {epoch + 1}/{num_epochs}, Loss: {loss.item()}')

    # save model
    torch.save(model.state_dict(), "/home/vinh/vinh_python/air_flow/model/stockmodel.pt")

    return True


def send_email():
    sender = "giaoviendinhcong4@gmail.com"
    recipient = "vinhduong227@gmail.com"
    attachment_file = "/home/vinh/vinh_python/air_flow/model/stockmodel.pt"

    ses = boto3.client('ses',
                       region_name='ap-southeast-1',
                       aws_access_key_id='AKIATMQYMOPS5334X2MH',
                       aws_secret_access_key='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')

    subject = "Update model"
    body = f"model done : {datetime.now()}"

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    msg.attach(MIMEText(body, 'plain'))

    with open(attachment_file, 'rb') as f:
        attachment_data = f.read()

    attachment = MIMEApplication(attachment_data)

    attachment.add_header('Content-Disposition', 'attachment', filename=attachment_file)

    msg.attach(attachment)

    response = ses.send_raw_email(
        Source=sender,
        Destinations=[recipient],
        RawMessage={'Data': msg.as_string()}
    )

    print("Email sent! Message ID:", response['MessageId'])
    return True

dag = DAG(
    'lstm_dags',
    default_args={
        'email': ['vinhduong227@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='training pipline DAG',
    schedule=timedelta(days=1),
    start_date= datetime.today() - timedelta(days=1),
    tags=['vinhdd'])


crawl_data = PythonOperator(
    task_id='crawl_data',
    python_callable=craw_stock_price,
    op_kwargs={"to_date": "{{ ds }}"},
    dag=dag
)

train_model = PythonOperator(
    task_id='train_model',
    python_callable=train_model_pytorch,
    dag=dag
)

email_operator = PythonOperator(
    task_id='email_operator',
    python_callable=send_email,
    dag=dag
)

crawl_data >> train_model >> email_operator